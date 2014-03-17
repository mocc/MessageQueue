/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.delivery;

import static org.apache.hedwig.util.VarArgs.va;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.filter.ServerMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.ProtocolVersion;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionType;
import org.apache.hedwig.protoextensions.PubSubResponseUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.UnexpectedError;
import org.apache.hedwig.server.delivery.ClusterDeliveryEndPoint.DeliveredMessage;
import org.apache.hedwig.server.handlers.SubscriptionChannelManager.SubChannelDisconnectedListener;
import org.apache.hedwig.server.netty.ServerStats;
import org.apache.hedwig.server.persistence.CancelScanRequest;
import org.apache.hedwig.server.persistence.Factory;
import org.apache.hedwig.server.persistence.MapMethods;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.ReadAheadCache;
import org.apache.hedwig.server.persistence.ScanCallback;
import org.apache.hedwig.server.persistence.ScanRequest;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.util.Callback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

public class FIFODeliveryManager implements DeliveryManager, SubChannelDisconnectedListener {

    protected static final Logger logger = LoggerFactory.getLogger(FIFODeliveryManager.class);

    private static Callback<Void> NOP_CALLBACK = new Callback<Void>() {
        @Override
        public void operationFinished(Object ctx, Void result) {
        }

        @Override
        public void operationFailed(Object ctx, PubSubException exception) {
        }
    };

    protected interface DeliveryManagerRequest {
        public void performRequest();
    }

    /**
     * Stores a mapping from topic to the delivery pointers on the topic. The
     * delivery pointers are stored in a sorted map from seq-id to the set of
     * subscribers at that seq-id
     */
    ConcurrentMap<ByteString, SortedMap<Long, Set<ActiveSubscriberState>>> perTopicDeliveryPtrs;

    /**
     * Mapping from delivery end point to the subscriber state that we are
     * serving at that end point. This prevents us e.g., from serving two
     * subscriptions to the same endpoint
     */
    ConcurrentMap<TopicSubscriber, ActiveSubscriberState> subscriberStates;

    private final ReadAheadCache cache;
    private final PersistenceManager persistenceMgr;
    private final TopicManager tm;
    private final ServerConfiguration cfg;
    // add for message queue sematics
    private  ZooKeeper zk;

    private final int numDeliveryWorkers;
    private final DeliveryWorker[] deliveryWorkers;
    private final ScheduledExecutorService clusterDeliveryScheduler = Executors.newSingleThreadScheduledExecutor();

    private class DeliveryWorker implements Runnable {

        BlockingQueue<DeliveryManagerRequest> requestQueue = new LinkedBlockingQueue<DeliveryManagerRequest>();

        /**
         * The queue of all subscriptions that are facing a transient error
         * either in scanning from the persistence manager, or in sending to the
         * consumer
         */
        Queue<ActiveSubscriberState> retryQueue = new PriorityBlockingQueue<ActiveSubscriberState>(32,
                new Comparator<ActiveSubscriberState>() {
                    @Override
                    public int compare(ActiveSubscriberState as1, ActiveSubscriberState as2) {
                        long s = as1.lastScanErrorTime - as2.lastScanErrorTime;
                        return s > 0 ? 1 : (s < 0 ? -1 : 0);
                    }
                });

        // Boolean indicating if this thread should continue running. This is
        // used
        // when we want to stop the thread during a PubSubServer shutdown.
        protected volatile boolean keepRunning = true;
        private final Thread workerThread;
        private final int idx;

        private final Object suspensionLock = new Object();
        private boolean suspended = false;

        DeliveryWorker(int index) {
            this.idx = index;
            workerThread = new Thread(this, "DeliveryManagerThread-" + index);
        }

        void start() {
            workerThread.start();
        }

        /**
         * Stop method which will enqueue a ShutdownDeliveryManagerRequest.
         */
        void stop() {
            enqueueWithoutFailure(new ShutdownDeliveryManagerRequest());
        }

        /**
         * Stop FIFO delivery worker from processing requests. (for testing)
         */
        void suspendProcessing() {
            synchronized (suspensionLock) {
                suspended = true;
            }
        }

        /**
         * Resume FIFO delivery worker. (for testing)
         */
        void resumeProcessing() {
            synchronized (suspensionLock) {
                suspended = false;
                suspensionLock.notify();
            }
        }

        @Override
        public void run() {
            while (keepRunning) {
                DeliveryManagerRequest request = null;

                try {
                    // We use a timeout of 1 second, so that we can wake up once
                    // in
                    // a while to check if there is something in the retry
                    // queue.
                    request = requestQueue.poll(1, TimeUnit.SECONDS);
                    synchronized (suspensionLock) {
                        while (suspended) {
                            suspensionLock.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                // First retry any subscriptions that had failed and need a
                // retry
                retryErroredSubscribers();

                if (request == null) {
                    continue;
                }

                request.performRequest();

            }
        }

        protected void enqueueWithoutFailure(DeliveryManagerRequest request) {
            if (!requestQueue.offer(request)) {
                throw new UnexpectedError("Could not enqueue object: " + request
                        + " to request queue for delivery worker ." + idx);
            }
        }

        public void retryErroredSubscriberAfterDelay(ActiveSubscriberState subscriber) {
            subscriber.setLastScanErrorTime(MathUtils.now());

            if (!retryQueue.offer(subscriber)) {
                throw new UnexpectedError("Could not enqueue to retry queue for delivery worker " + idx);
            }
        }

        public void clearRetryDelayForSubscriber(ActiveSubscriberState subscriber) {
            subscriber.clearLastScanErrorTime();
            if (!retryQueue.offer(subscriber)) {
                throw new UnexpectedError("Could not enqueue to delivery manager retry queue");
            }
            // no request in request queue now
            // issue a empty delivery request to not waiting for polling
            // requests queue
            if (requestQueue.isEmpty()) {
                enqueueWithoutFailure(new DeliveryManagerRequest() {
                    @Override
                    public void performRequest() {
                        // do nothing
                    }
                });
            }
        }

        protected void retryErroredSubscribers() {
            long lastInterestingFailureTime = MathUtils.now() - cfg.getScanBackoffPeriodMs();
            ActiveSubscriberState subscriber;

            while ((subscriber = retryQueue.peek()) != null) {
                if (subscriber.getLastScanErrorTime() > lastInterestingFailureTime) {
                    // Not enough time has elapsed yet, will retry later
                    // Since the queue is fifo, no need to check later items
                    return;
                }

                // retry now
                subscriber.deliverNextMessage();
                retryQueue.poll();
            }
        }

        protected class ShutdownDeliveryManagerRequest implements DeliveryManagerRequest {
            // This is a simple type of Request we will enqueue when the
            // PubSubServer is shut down and we want to stop the DeliveryManager
            // thread.
            @Override
            public void performRequest() {
                keepRunning = false;
            }
        }

    }

    public FIFODeliveryManager(TopicManager tm, PersistenceManager persistenceMgr, ServerConfiguration cfg) {
        this.tm = tm;
        this.persistenceMgr = persistenceMgr;
        if (persistenceMgr instanceof ReadAheadCache) {
            this.cache = (ReadAheadCache) persistenceMgr;
        } else {
            this.cache = null;
        }
        perTopicDeliveryPtrs = new ConcurrentHashMap<ByteString, SortedMap<Long, Set<ActiveSubscriberState>>>();
        subscriberStates = new ConcurrentHashMap<TopicSubscriber, ActiveSubscriberState>();
        this.cfg = cfg;
        // initialize the delivery workers
        this.numDeliveryWorkers = cfg.getNumDeliveryThreads();
        this.deliveryWorkers = new DeliveryWorker[numDeliveryWorkers];
        for (int i = 0; i < numDeliveryWorkers; i++) {
            deliveryWorkers[i] = new DeliveryWorker(i);
        }
    }

    // add for message queue semantics
    public FIFODeliveryManager(TopicManager tm, PersistenceManager persistenceMgr, ServerConfiguration cfg, ZooKeeper zk){
        this.tm = tm;
        this.persistenceMgr = persistenceMgr;
        if (persistenceMgr instanceof ReadAheadCache) {
            this.cache = (ReadAheadCache) persistenceMgr;
        } else {
            this.cache = null;
        }
        perTopicDeliveryPtrs = new ConcurrentHashMap<ByteString, SortedMap<Long, Set<ActiveSubscriberState>>>();
        subscriberStates = new ConcurrentHashMap<TopicSubscriber, ActiveSubscriberState>();
        this.cfg = cfg;
        this.zk = zk;
        // initialize the delivery workers
        this.numDeliveryWorkers = cfg.getNumDeliveryThreads();
        this.deliveryWorkers = new DeliveryWorker[numDeliveryWorkers];
        for (int i = 0; i < numDeliveryWorkers; i++) {
            deliveryWorkers[i] = new DeliveryWorker(i);
        }
    } 
    // *** add for message queue semantics **** 
    
    
    @Override
    public void start() {
        for (int i = 0; i < numDeliveryWorkers; i++) {
            deliveryWorkers[i].start();
        }
    }

    /**
     * Stop FIFO delivery manager from processing requests. (for testing)
     */
    @VisibleForTesting
    public void suspendProcessing() {
        for (int i = 0; i < numDeliveryWorkers; i++) {
            deliveryWorkers[i].suspendProcessing();
        }
    }

    /**
     * Resume FIFO delivery manager. (for testing)
     */
    @VisibleForTesting
    public void resumeProcessing() {
        for (int i = 0; i < numDeliveryWorkers; i++) {
            deliveryWorkers[i].resumeProcessing();
        }
    }

    /**
     * Stop the FIFO delivery manager.
     */
    @Override
    public void stop() {
        for (int i = 0; i < numDeliveryWorkers; i++) {
            deliveryWorkers[i].stop();
        }
    }

    private DeliveryWorker getDeliveryWorker(ByteString topic) {
        return deliveryWorkers[MathUtils.signSafeMod(topic.hashCode(), numDeliveryWorkers)];
    }

    /**
     * ===================================================================== Our
     * usual enqueue function, stop if error because of unbounded queue, should
     * never happen
     * 
     */
    protected void enqueueWithoutFailure(ByteString topic, DeliveryManagerRequest request) {
        getDeliveryWorker(topic).enqueueWithoutFailure(request);
    }

    /**
     * Tells the delivery manager to start sending out messages for a particular
     * subscription
     * 
     * @param topic
     * @param subscriberId
     * @param seqIdToStartFrom
     *            Message sequence-id from where delivery should be started
     * @param endPoint
     *            The delivery end point to which send messages to
     * @param filter
     *            Only messages passing this filter should be sent to this
     *            subscriber
     * @param callback
     *            Callback instance
     * @param ctx
     *            Callback context
     */
    @Override
    public void startServingSubscription(ByteString topic, ByteString subscriberId,
            SubscriptionPreferences preferences, MessageSeqId seqIdToStartFrom, DeliveryEndPoint endPoint,
            ServerMessageFilter filter, Callback<Void> callback, Object ctx) {
        int messageWindowSize;
        if (preferences.hasMessageWindowSize()) {
            messageWindowSize = preferences.getMessageWindowSize();
        } else {
            if (FIFODeliveryManager.this.cfg.getDefaultMessageWindowSize() > 0) {
                messageWindowSize = FIFODeliveryManager.this.cfg.getDefaultMessageWindowSize();
            } else {
                messageWindowSize = 0;//which means no window throttling policy
            }
        }
        if (preferences.hasSubscriptionType() && SubscriptionType.CLUSTER == preferences.getSubscriptionType()) {
            ClusterDeliveryEndPoint clusterEP = new ClusterDeliveryEndPoint("(topic: " + topic.toStringUtf8()
                    + ", subscriber: " + subscriberId.toStringUtf8() + ")", clusterDeliveryScheduler);
            clusterEP.addDeliveryEndPoint(endPoint, messageWindowSize);
            final ClusterSubscriber subscriber = new ClusterSubscriber(topic, subscriberId, preferences,
                    seqIdToStartFrom.getLocalComponent() - 1, clusterEP, filter, callback, ctx);
            StartServingClusterSubscriberRequest request = new StartServingClusterSubscriberRequest(subscriber,
                    endPoint, messageWindowSize);
            enqueueWithoutFailure(topic, request);
/*            clusterDeliveryScheduler.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    // run retry policy for cluster every 5 seconds
                    subscriber.checkExpiredMessages(System.currentTimeMillis());
                }

            }, 0, 5, TimeUnit.SECONDS);*/

        } else {
            SimpleSubscriber subscriber = new SimpleSubscriber(topic, subscriberId, preferences,
                    seqIdToStartFrom.getLocalComponent() - 1, endPoint, filter, messageWindowSize, callback, ctx);
            enqueueWithoutFailure(topic, subscriber);

        }
    }

    @Override
    public void stopServingSubscriber(ByteString topic, ByteString subscriberId, SubscriptionEvent event,
            DeliveryEndPoint endPoint, Callback<Void> cb, Object ctx) {
        enqueueWithoutFailure(topic, new StopServingSubscriber(topic, subscriberId, event, endPoint, cb, ctx));
    }

    /**
     * Instructs the delivery manager to backoff on the given subscriber and
     * retry sending after some time
     * 
     * @param subscriber
     */
    public void retryErroredSubscriberAfterDelay(ActiveSubscriberState subscriber) {
        getDeliveryWorker(subscriber.getTopic()).retryErroredSubscriberAfterDelay(subscriber);
    }

    public void clearRetryDelayForSubscriber(ActiveSubscriberState subscriber) {
        getDeliveryWorker(subscriber.getTopic()).clearRetryDelayForSubscriber(subscriber);
    }

    // TODO: for now, I don't move messageConsumed request to delivery manager
    // thread,
    // which is supposed to be fixed in {@link
    // https://issues.apache.org/jira/browse/BOOKKEEPER-503}
    @Override
    public void messageConsumed(ByteString topic, ByteString subscriberId, MessageSeqId consumedSeqId) {
        ActiveSubscriberState subState = subscriberStates.get(new TopicSubscriber(topic, subscriberId));
        if (null == subState) {
            return;
        }
        subState.messageConsumed(consumedSeqId.getLocalComponent());
    }

    /**
     * Instructs the delivery manager to move the delivery pointer for a given
     * subscriber
     * 
     * @param subscriber
     * @param prevSeqId
     * @param newSeqId
     */
    public void moveDeliveryPtrForward(ActiveSubscriberState subscriber, long prevSeqId, long newSeqId) {
        enqueueWithoutFailure(subscriber.getTopic(), new DeliveryPtrMove(subscriber, prevSeqId, newSeqId));
    }

    protected void removeDeliveryPtr(ActiveSubscriberState subscriber, Long seqId, boolean isAbsenceOk,
            boolean pruneTopic) {

        assert seqId != null;

        // remove this subscriber from the delivery pointers data structure
        ByteString topic = subscriber.getTopic();
        SortedMap<Long, Set<ActiveSubscriberState>> deliveryPtrs = perTopicDeliveryPtrs.get(topic);

        if (deliveryPtrs == null && !isAbsenceOk) {
            throw new UnexpectedError("No delivery pointers found while disconnecting " + "channel for topic:" + topic);
        }

        if (null == deliveryPtrs) {
            return;
        }

        if (!MapMethods.removeFromMultiMap(deliveryPtrs, seqId, subscriber) && !isAbsenceOk) {

            throw new UnexpectedError("Could not find subscriber:" + subscriber + " at the expected delivery pointer");
        }

        if (pruneTopic && deliveryPtrs.isEmpty()) {
            perTopicDeliveryPtrs.remove(topic);
        }

    }

    protected long getMinimumSeqId(ByteString topic) {
        SortedMap<Long, Set<ActiveSubscriberState>> deliveryPtrs = perTopicDeliveryPtrs.get(topic);

        if (deliveryPtrs == null || deliveryPtrs.isEmpty()) {
            return Long.MAX_VALUE - 1;
        }
        return deliveryPtrs.firstKey();
    }

    protected void addDeliveryPtr(ActiveSubscriberState subscriber, Long seqId) {

        // If this topic doesn't exist in the per-topic delivery pointers table,
        // create an entry for it
        SortedMap<Long, Set<ActiveSubscriberState>> deliveryPtrs = MapMethods.getAfterInsertingIfAbsent(
                perTopicDeliveryPtrs, subscriber.getTopic(), TreeMapLongToSetSubscriberFactory.instance);

        MapMethods.addToMultiMap(deliveryPtrs, seqId, subscriber, HashMapSubscriberFactory.instance);
    }

    class StartServingClusterSubscriberRequest implements DeliveryManagerRequest {

        private final ClusterSubscriber subscriber;
        private final DeliveryEndPoint endpoint;
        private final int messageWindowSize;

        /*
         * modified by hrq.
         */
        public StartServingClusterSubscriberRequest(ClusterSubscriber subscriber, DeliveryEndPoint endpoint,
                int messageWindowSize) {
            this.subscriber = subscriber;
            this.endpoint = endpoint;
            this.messageWindowSize = messageWindowSize;
        }

        @Override
        public void performRequest() {
            // Put this subscriber in the channel to subscriber mapping
            TopicSubscriber ts = new TopicSubscriber(subscriber.topic, subscriber.subscriberId);
            ActiveSubscriberState curSubscriber = subscriberStates.get(ts);
            if (null == curSubscriber) {
                subscriberStates.put(ts, subscriber);
            }

            // after put the active subscriber in subscriber states mapping
            // trigger the callback to tell it started to deliver the message
            // should let subscriber response go first before first delivered
            // message.
            subscriber.cb.operationFinished(subscriber.ctx, (Void) null);
            if (curSubscriber != null) {
                ((ClusterSubscriber) curSubscriber).clusterEP.addDeliveryEndPoint(endpoint, messageWindowSize);
            } else {
                synchronized (subscriber) {
                    subscriber.lastSeqIdCommunicatedExternally = subscriber.lastLocalSeqIdDelivered;

                    addDeliveryPtr(subscriber, subscriber.lastLocalSeqIdDelivered);

                }
                subscriber.deliverNextMessage();
            }
        }

    }

    class ClusterSubscriber extends ActiveSubscriberState {
        private final ClusterDeliveryEndPoint clusterEP;

        // time frequency for checking expired unconsumed messages in cluster
        final long timeOut = 5000;
        Long lastCheckTime = System.currentTimeMillis();

        // checking expired unconsumed messages in cluster
/*        private void checkExpiredMessages(long currentTime) {
            Set<DeliveredMessage> msgs = new HashSet<DeliveredMessage>();
            for (Long seq : clusterEP.pendings.keySet()) {
                DeliveredMessage msg = clusterEP.pendings.get(seq);
                if (currentTime - msg.lastDeliveredTime < timeOut) {
                    continue;
                }
                if (null != msg)
                    msgs.add(msg);
                logger.info("msgseq " + seq + " timeout...............");
            }
            clusterEP.closeAndTimeOutRedeliver(msgs);
        }*/

        public ClusterSubscriber(ByteString topic, ByteString subscriberId, SubscriptionPreferences preferences,
                long lastLocalSeqIdDelivered, ClusterDeliveryEndPoint deliveryEndPoint, ServerMessageFilter filter,
                Callback<Void> cb, Object ctx) {
            super(topic, subscriberId, preferences, lastLocalSeqIdDelivered, deliveryEndPoint, filter,
                    deliveryEndPoint, cb, ctx);
            clusterEP = deliveryEndPoint;

        }

        @Override
        void sendSubscriptionEventToDeliveryChannel(SubscriptionEvent event) {
            if (null != event
                    && (SubscriptionEvent.TOPIC_MOVED == event || SubscriptionEvent.SUBSCRIPTION_FORCED_CLOSED == event)) {
                // we should not close the channel now after enabling
                // multiplexing
                PubSubResponse response = PubSubResponseUtils.getResponseForSubscriptionEvent(topic, subscriberId,
                        event);
                clusterEP.sendSubscriptionEvent(response);
            }
        }

    }

    class SimpleSubscriber extends ActiveSubscriberState {

        public SimpleSubscriber(ByteString topic, ByteString subscriberId, SubscriptionPreferences preferences,
                long lastLocalSeqIdDelivered, DeliveryEndPoint deliveryEndPoint, ServerMessageFilter filter,
                int messageWindowSize, Callback<Void> cb, Object ctx) {
            super(topic, subscriberId, preferences, lastLocalSeqIdDelivered, deliveryEndPoint, filter,
                    new WindowThrottlingPolicy("(topic: " + topic.toStringUtf8() + ", subscriber: "
                            + subscriberId.toStringUtf8() + ")", lastLocalSeqIdDelivered, messageWindowSize), cb, ctx);
        }

        @Override
        void sendSubscriptionEventToDeliveryChannel(SubscriptionEvent event) {
            if (null != event
                    && (SubscriptionEvent.TOPIC_MOVED == event || SubscriptionEvent.SUBSCRIPTION_FORCED_CLOSED == event)) {
                // we should not close the channel now after enabling
                // multiplexing
                PubSubResponse response = PubSubResponseUtils.getResponseForSubscriptionEvent(topic, subscriberId,
                        event);
                deliveryEndPoint.send(response, new DeliveryCallback() {
                    @Override
                    public void sendingFinished() {
                        // do nothing now
                    }

                    @Override
                    public void transientErrorOnSend() {
                        // do nothing now
                    }

                    @Override
                    public void permanentErrorOnSend() {
                        // if channel is broken, close the channel
                        deliveryEndPoint.close();
                    }
                });
            }
        }

    }

    abstract class ActiveSubscriberState implements ScanCallback, DeliveryCallback, DeliveryManagerRequest,
            CancelScanRequest {

        ByteString topic;
        ByteString subscriberId;
        long lastLocalSeqIdDelivered;
        boolean connected = true;
        ReentrantReadWriteLock connectedLock = new ReentrantReadWriteLock();
        DeliveryEndPoint deliveryEndPoint;
        long lastScanErrorTime = -1;
        long localSeqIdDeliveringNow;
        long lastSeqIdCommunicatedExternally;
        ThrottlingPolicy throttlingPolicy;
        boolean isThrottled = false;
        ServerMessageFilter filter;
        Callback<Void> cb;
        Object ctx;

        // track the outstanding scan request
        // so we could cancel it
        ScanRequest outstandingScanRequest;

        final static int SEQ_ID_SLACK = 10;

        public ActiveSubscriberState(ByteString topic, ByteString subscriberId, SubscriptionPreferences preferences,
                long lastLocalSeqIdDelivered, DeliveryEndPoint deliveryEndPoint, ServerMessageFilter filter,
                ThrottlingPolicy throttlingPolicy, Callback<Void> cb, Object ctx) {
            this.topic = topic;
            this.subscriberId = subscriberId;
            this.lastLocalSeqIdDelivered = lastLocalSeqIdDelivered;
            this.deliveryEndPoint = deliveryEndPoint;
            this.filter = filter;
            this.throttlingPolicy = throttlingPolicy;
            this.cb = cb;
            this.ctx = ctx;
        }

        abstract void sendSubscriptionEventToDeliveryChannel(SubscriptionEvent event);

        public void setNotConnected(SubscriptionEvent event) {
            this.connectedLock.writeLock().lock();
            try {
                // have closed it.
                if (!connected) {
                    return;
                }
                this.connected = false;
                // put itself in ReadAhead queue to cancel outstanding scan
                // request
                // if outstanding scan request callback before cancel op
                // executed,
                // nothing it would cancel.
                if (null != cache && null != outstandingScanRequest) {
                    cache.cancelScanRequest(topic, this);
                }
            } finally {
                this.connectedLock.writeLock().unlock();
            }
            // send subscription events to delivery channel
            sendSubscriptionEventToDeliveryChannel(event);
            // uninitialize filter
            this.filter.uninitialize();
        }

        public ByteString getTopic() {
            return topic;
        }

        public synchronized long getLastScanErrorTime() {
            return lastScanErrorTime;
        }

        public synchronized void setLastScanErrorTime(long lastScanErrorTime) {
            this.lastScanErrorTime = lastScanErrorTime;
        }

        /**
         * Clear the last scan error time so it could be retry immediately.
         */
        protected synchronized void clearLastScanErrorTime() {
            this.lastScanErrorTime = -1;
        }

        protected boolean isConnected() {
            connectedLock.readLock().lock();
            try {
                return connected;
            } finally {
                connectedLock.readLock().unlock();
            }
        }

        protected synchronized void messageConsumed(long newSeqIdConsumed) {
            if (!throttlingPolicy.messageConsumed(newSeqIdConsumed)) {
                return;
            }
            // after updated seq id check whether it still exceed msg limitation
            if (shouldThrottle()) {
                return;
            }
            if (isThrottled) {
                isThrottled = false;
                logger.info("Try to wake up subscriber ({}) to deliver messages again : last delivered {}.", this,
                        lastLocalSeqIdDelivered);

                enqueueWithoutFailure(topic, new DeliveryManagerRequest() {
                    @Override
                    public void performRequest() {
                        // enqueue
                        clearRetryDelayForSubscriber(ActiveSubscriberState.this);
                    }
                });
            }
        }

        protected boolean shouldThrottle() {
            return throttlingPolicy.shouldThrottle(lastLocalSeqIdDelivered);
        }

        public void deliverNextMessage() {
            connectedLock.readLock().lock();
            try {
                doDeliverNextMessage();
            } finally {
                connectedLock.readLock().unlock();
            }
        }

        private void doDeliverNextMessage() {
            if (!connected) {
                return;
            }

            synchronized (this) {
                // check whether we have delivered enough messages without
                // receiving their consumes
                if (shouldThrottle()) {
                    logger.info("Subscriber ({}) is throttled : last delivered {}.", va(this, lastLocalSeqIdDelivered));
                    isThrottled = true;
                    // do nothing, since the delivery process would be
                    // throttled.
                    // After message consumed, it would be added back to retry
                    // queue.
                    return;
                }

                localSeqIdDeliveringNow = persistenceMgr.getSeqIdAfterSkipping(topic, lastLocalSeqIdDelivered, 1);

                outstandingScanRequest = new ScanRequest(topic, localSeqIdDeliveringNow,
                /* callback= */this, /* ctx= */null);
            }

            persistenceMgr.scanSingleMessage(outstandingScanRequest);
        }

        /**
         * ===============================================================
         * {@link CancelScanRequest} methods
         * 
         * This method runs ins same threads with ScanCallback. When it runs, it
         * checked whether it is outstanding scan request. if there is one,
         * cancel it.
         */
        @Override
        public ScanRequest getScanRequest() {
            // no race between cancel request and scan callback
            // the only race is between stopServing and deliverNextMessage
            // deliverNextMessage would be executed in netty callback which is
            // in netty thread
            // stopServing is run in delivery thread. if stopServing runs before
            // deliverNextMessage
            // deliverNextMessage would have chance to put a stub in
            // ReadAheadCache
            // then we don't have any chance to cancel it.
            // use connectedLock to avoid such race.
            return outstandingScanRequest;
        }

        private boolean checkConnected() {
            connectedLock.readLock().lock();
            try {
                // message scanned means the outstanding request is executed
                outstandingScanRequest = null;
                return connected;
            } finally {
                connectedLock.readLock().unlock();
            }
        }

        /**
         * ===============================================================
         * {@link ScanCallback} methods
         */

        @Override
        public void messageScanned(Object ctx, Message message) {
            if (!checkConnected()) {
                return;
            }

            // only increment topic access times when tried to deliver a message
            // for those subscribers just waiting for a published for a long
            // time
            // we don't increment topic access times, so the topic would be
            // evicted
            // in future.
            tm.incrementTopicAccessTimes(topic);

            if (!filter.testMessage(message)) {
                messageConsumed(message.getMsgId().getLocalComponent());
                sendingFinished();
                return;
            }

            /**
             * The method below will invoke our sendingFinished() method when
             * done
             */
            PubSubResponse response = PubSubResponse.newBuilder().setProtocolVersion(ProtocolVersion.VERSION_ONE)
                    .setStatusCode(StatusCode.SUCCESS).setTxnId(0).setMessage(message).setTopic(topic)
                    .setSubscriberId(subscriberId).build();

            deliveryEndPoint.send(response, //
                    // callback =
                    this);

        }

        @Override
        public void scanFailed(Object ctx, Exception exception) {
            if (!checkConnected()) {
                return;
            }

            // wait for some time and then retry
            retryErroredSubscriberAfterDelay(this);
        }

        @Override
        public void scanFinished(Object ctx, ReasonForFinish reason) {
            checkConnected();
        }

        /**
         * ===============================================================
         * {@link DeliveryCallback} methods
         */
        @Override
        public void sendingFinished() {
            if (!isConnected()) {
                return;
            }

            synchronized (this) {
                lastLocalSeqIdDelivered = localSeqIdDeliveringNow;

                if (lastLocalSeqIdDelivered > lastSeqIdCommunicatedExternally + SEQ_ID_SLACK) {
                    // Note: The order of the next 2 statements is important. We
                    // should
                    // submit a request to change our delivery pointer only
                    // *after* we
                    // have actually changed it. Otherwise, there is a race
                    // condition
                    // with removal of this channel, w.r.t, maintaining the
                    // deliveryPtrs
                    // tree map.
                    long prevId = lastSeqIdCommunicatedExternally;
                    lastSeqIdCommunicatedExternally = lastLocalSeqIdDelivered;
                    moveDeliveryPtrForward(this, prevId, lastLocalSeqIdDelivered);
                }
            }
            // increment delivered message
            ServerStats.getInstance().incrementMessagesDelivered();
            deliverNextMessage();
        }

        public synchronized long getLastSeqIdCommunicatedExternally() {
            return lastSeqIdCommunicatedExternally;
        }

        @Override
        public void permanentErrorOnSend() {
            // the underlying channel is broken, the channel will
            // be closed in UmbrellaHandler when exception happened.
            // so we don't need to close the channel again

            System.out.println("permanentErrorOnSend.............................");
            stopServingSubscriber(topic, subscriberId, null, deliveryEndPoint, NOP_CALLBACK, null);

        }

        @Override
        public void transientErrorOnSend() {
            retryErroredSubscriberAfterDelay(this);
        }

        /**
         * ===============================================================
         * {@link DeliveryManagerRequest} methods
         */
        @Override
        public void performRequest() {
            // Put this subscriber in the channel to subscriber mapping
            ActiveSubscriberState prevSubscriber = subscriberStates.put(new TopicSubscriber(topic, subscriberId), this);

            // after put the active subscriber in subscriber states mapping
            // trigger the callback to tell it started to deliver the message
            // should let subscriber response go first before first delivered
            // message.
            cb.operationFinished(ctx, (Void) null);

            if (prevSubscriber != null) {
                // we already in the delivery thread, we don't need to equeue a
                // stop request
                // just stop it now, since stop is not blocking operation.
                // and also it cleans the old state of the active subscriber
                // immediately.
                SubscriptionEvent se;
                if (deliveryEndPoint.equals(prevSubscriber.deliveryEndPoint)) {
                    logger.debug("Subscriber {} replaced a duplicated subscriber {} at same delivery point {}.",
                            va(this, prevSubscriber, deliveryEndPoint));
                    se = null;
                } else {
                    logger.debug("Subscriber {} from delivery point {} forcelly closed delivery point {}.",
                            va(this, deliveryEndPoint, prevSubscriber.deliveryEndPoint));
                    se = SubscriptionEvent.SUBSCRIPTION_FORCED_CLOSED;
                }
                doStopServingSubscriber(prevSubscriber, se);
            }

            synchronized (this) {
                lastSeqIdCommunicatedExternally = lastLocalSeqIdDelivered;
                addDeliveryPtr(this, lastLocalSeqIdDelivered);
            }

            deliverNextMessage();
        };

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Topic: ");
            sb.append(topic.toStringUtf8());
            sb.append("Subscriber: ");
            sb.append(subscriberId.toStringUtf8());
            sb.append(", DeliveryPtr: ");
            sb.append(lastLocalSeqIdDelivered);
            return sb.toString();

        }
    }

    protected class StopServingSubscriber implements DeliveryManagerRequest {
        final TopicSubscriber ts;
        final SubscriptionEvent event;
        // EndPoint could be null
        final DeliveryEndPoint endPoint;
        final Callback<Void> cb;
        final Object ctx;

        public StopServingSubscriber(ByteString topic, ByteString subscriberId, SubscriptionEvent event,
                DeliveryEndPoint endPoint, Callback<Void> callback, Object ctx) {
            this.ts = new TopicSubscriber(topic, subscriberId);
            this.event = event;
            this.endPoint = endPoint;
            this.cb = callback;
            this.ctx = ctx;
        }

        @Override
        public void performRequest() {
            ActiveSubscriberState subscriber = subscriberStates.get(ts);
            if (null != subscriber) {
                if (subscriber instanceof SimpleSubscriber) {
                    if (null == this.endPoint || subscriber.deliveryEndPoint.equals(this.endPoint)) {
                        // it is safe to remove the subscriber when the delivery
                        // end point is matched.
                        subscriberStates.remove(ts);
                        doStopServingSubscriber(subscriber, event);
                    }
                } else {
                    if (null == this.endPoint) {
                        subscriberStates.remove(ts);
                        doStopServingSubscriber(subscriber, event);
                    } else {
                        // stop specific delivery endpoint
                        ((ClusterDeliveryEndPoint) subscriber.deliveryEndPoint).removeDeliveryEndPoint(endPoint);
                        // we should not close the channel now after enabling
                        // multiplexing
                        PubSubResponse response = PubSubResponseUtils.getResponseForSubscriptionEvent(subscriber.topic,
                                subscriber.subscriberId, event);
                        endPoint.send(response, new DeliveryCallback() {
                            @Override
                            public void sendingFinished() {
                                // do nothing now
                            }

                            @Override
                            public void transientErrorOnSend() {
                                // do nothing now
                            }

                            @Override
                            public void permanentErrorOnSend() {
                                // if channel is broken, close the channel
                                endPoint.close();
                            }
                        });
                    }
                }
            }
            cb.operationFinished(ctx, null);
        }

    }

    /**
     * Stop serving a subscriber. This method should be called in a
     * {@link DeliveryManagerRequest}.
     * 
     * @param subscriber
     *            Active Subscriber to stop
     * @param event
     *            Subscription Event for the stop reason
     */
    private void doStopServingSubscriber(ActiveSubscriberState subscriber, SubscriptionEvent event) {
        // This will automatically stop delivery, and disconnect the channel
        subscriber.setNotConnected(event);

        // if the subscriber has moved on, a move request for its delivery
        // pointer must be pending in the request queue. Note that the
        // subscriber first changes its delivery pointer and then submits a
        // request to move so this works.
        removeDeliveryPtr(subscriber, subscriber.getLastSeqIdCommunicatedExternally(), //
                // isAbsenceOk=
                true,
                // pruneTopic=
                true);
    }

    protected class DeliveryPtrMove implements DeliveryManagerRequest {

        ActiveSubscriberState subscriber;
        Long oldSeqId;
        Long newSeqId;

        public DeliveryPtrMove(ActiveSubscriberState subscriber, Long oldSeqId, Long newSeqId) {
            this.subscriber = subscriber;
            this.oldSeqId = oldSeqId;
            this.newSeqId = newSeqId;
        }

        @Override
        public void performRequest() {
            ByteString topic = subscriber.getTopic();
            long prevMinSeqId = getMinimumSeqId(topic);

            if (subscriber.isConnected()) {
                removeDeliveryPtr(subscriber, oldSeqId, //
                        // isAbsenceOk=
                        false,
                        // pruneTopic=
                        false);

                addDeliveryPtr(subscriber, newSeqId);
            } else {
                removeDeliveryPtr(subscriber, oldSeqId, //
                        // isAbsenceOk=
                        true,
                        // pruneTopic=
                        true);
            }

            long nowMinSeqId = getMinimumSeqId(topic);

            if (nowMinSeqId > prevMinSeqId) {
                persistenceMgr.deliveredUntil(topic, nowMinSeqId);
            }
        }
    }

    /**
     * ====================================================================
     * 
     * Dumb factories for our map methods
     */
    protected static class TreeMapLongToSetSubscriberFactory implements
            Factory<SortedMap<Long, Set<ActiveSubscriberState>>> {
        static TreeMapLongToSetSubscriberFactory instance = new TreeMapLongToSetSubscriberFactory();

        @Override
        public SortedMap<Long, Set<ActiveSubscriberState>> newInstance() {
            return new TreeMap<Long, Set<ActiveSubscriberState>>();
        }
    }

    protected static class HashMapSubscriberFactory implements Factory<Set<ActiveSubscriberState>> {
        static HashMapSubscriberFactory instance = new HashMapSubscriberFactory();

        @Override
        public Set<ActiveSubscriberState> newInstance() {
            return new HashSet<ActiveSubscriberState>();
        }
    }

    @Override
    public void onSubChannelDisconnected(TopicSubscriber topicSubscriber, Channel channel) {

        stopServingSubscriber(topicSubscriber.getTopic(), topicSubscriber.getSubscriberId(), null, new ChannelEndPoint(
                channel), NOP_CALLBACK, null);

    }
    
    /**
     * ==========================================================================================
     * 
     * methods added for message queue semantics
     * 
     */
    @Override
    public boolean deleteQueuePersistenceInfo(ByteString topic){
    	
    	String topicPath = cfg.getZkTopicPath(new StringBuilder(), topic).toString();
    	return zkDeleteRecursive(topicPath);
    }

    public boolean zkDeleteRecursive(String path) {
        List<String> children = null;

        try {
            children = zk.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return true;
        } catch (KeeperException e) {
            return false;
        } catch (InterruptedException e) {
            return false;
        }

        for (String subPath : children) {
            if (!zkDeleteRecursive(path + "/" + subPath)) {
                return false;
            }
        }

        try {
            zk.delete(path, -1); // -1 matches any varsion
        } catch (Exception e) {
            return false;
        }
        return true;
    }
    //**** methods added for message queue semantics ***
}
