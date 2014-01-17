package org.apache.hedwig.server.delivery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;

public class ClusterDeliveryEndPoint implements DeliveryEndPoint, ThrottlingPolicy {
    /**
     * when closed = true, means the whole cluster is closed, default is false
     */
    volatile boolean closed = false;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

    /**
     * In cluster,every client has a corresponding DeliveryEndPoint and a
     * corresponding DeliveryState, which are mapped in endpoints.The
     * DeliveryState records all unconsumed messages of this client.
     */
    final ConcurrentHashMap<DeliveryEndPoint, DeliveryState> endpoints = new ConcurrentHashMap<DeliveryEndPoint, DeliveryState>();
    /**
     * the whole cluster's all unconsumed messages are mapped in pendings
     */
    final ConcurrentHashMap<Long, DeliveredMessage> pendings = new ConcurrentHashMap<Long, DeliveredMessage>();
    /**
     * all current unthrottled EPs are in deliverableEP queue
     */
    final LinkedBlockingQueue<DeliveryEndPoint> deliverableEP = new LinkedBlockingQueue<DeliveryEndPoint>();
    /**
     * all current throttled EPs are in throttledEP queue
     */
    final ConcurrentLinkedQueue<DeliveryEndPoint> throttledEP = new ConcurrentLinkedQueue<DeliveryEndPoint>();
    final String label;// the topic which the cluster subscribe to
    final ScheduledExecutorService scheduler;

    /***
     * every client in cluster holds a DeliveryState, which records both the
     * client's window size<messageWindowSize> and unconsumed messages<msgs>
     */
    static class DeliveryState {
        SortedSet<Long> msgs = new TreeSet<Long>();
        int messageWindowSize; // this is a message window size for every client

        public DeliveryState(int messageWindowSize) {
            this.messageWindowSize = messageWindowSize;
        }
    }

    /***
     * all messages sent to every client will be encapsulated in
     * DeliveredMessage, which also record the deliver time and responsible
     * deliverEP of this message
     */
    class DeliveredMessage {
        final PubSubResponse msg;
        volatile long lastDeliveredTime;
        volatile DeliveryEndPoint lastDeliveredEP = null;

        DeliveredMessage(PubSubResponse msg) {
            this.msg = msg;
            this.lastDeliveredTime = MathUtils.now();
        }

        /**
         * reset message's deliver time and deliverEP when redeliver this
         * message
         * 
         * @param ep
         *            the new deliverEP which this message will be sent
         */
        void resetDeliveredTime(DeliveryEndPoint ep) {
            DeliveryEndPoint oldEP = this.lastDeliveredEP;
            if (null != oldEP) {
                DeliveryState state = endpoints.get(oldEP);
                if (null != state) {
                    state.msgs.remove(msg.getMessage().getMsgId().getLocalComponent());
                }
            }
            this.lastDeliveredTime = MathUtils.now();
            this.lastDeliveredEP = ep;
        }
    }

    class ClusterDeliveryCallback implements DeliveryCallback {

        final DeliveryEndPoint ep;
        final DeliveryState state;
        final DeliveredMessage msg;
        final long deliveredTime;

        /**
         * this constructor is mainly for updating unconsumed messages for
         * client on sending
         * 
         * @param ep
         *            the sending deliveryEP
         * @param state
         *            the deliveryState of the sending deliveryEP
         * @param msg
         *            the sending message
         */
        ClusterDeliveryCallback(DeliveryEndPoint ep, DeliveryState state, DeliveredMessage msg) {
            this.ep = ep;
            this.state = state;
            this.msg = msg;
            this.deliveredTime = msg.lastDeliveredTime;

            // add this msgs to current delivery endpoint state
            this.state.msgs.add(msg.msg.getMessage().getMsgId().getLocalComponent());
        }

        public DeliveryState getState() {
            return state;
        }

        @Override
        public void sendingFinished() {
            // nop
        }

        @Override
        public void transientErrorOnSend() {
            closeAndRedeliver(ep, state);
        }

        @Override
        public void permanentErrorOnSend() {
            closeAndRedeliver(ep, state);
        }

    };

    /**
     * run a RedeliveryTask for every fault client who needs redelivery.
     */
    class RedeliveryTask implements Runnable {

        final DeliveryState state;

        RedeliveryTask(DeliveryState state) {
            this.state = state;
        }

        @Override
        public void run() {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    return;
                }
            } finally {
                closeLock.readLock().unlock();
            }
            Set<DeliveredMessage> msgs = new HashSet<DeliveredMessage>();

            for (long seqid : state.msgs) {
                DeliveredMessage msg = pendings.get(seqid);
                if (null != msg) {
                    msgs.add(msg);
                }
            }

            for (DeliveredMessage msg : msgs) {
                DeliveryEndPoint ep = send(msg);
                if (null == ep) {
                    // no delivery channel in endpoints, which means no client
                    // in this cluster
                    ClusterDeliveryEndPoint.this.close();
                    return;
                }
            }
        }

    }

    /**
     * run retry policy, redeliver all timeout unconsumed messages of the whole
     * cluster
     */
    class TimeOutRedeliveryTask implements Runnable {

        // final DeliveryState state;
        final Set<DeliveredMessage> msgs;

        TimeOutRedeliveryTask(Set<DeliveredMessage> msgs) {
            this.msgs = msgs;
        }

        @Override
        public void run() {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    return;
                }
            } finally {
                closeLock.readLock().unlock();
            }
            for (DeliveredMessage msg : msgs) {
                DeliveryEndPoint ep = send(msg);
                if (null == ep) {
                    // no delivery channel in endpoints, which means no client
                    // in this cluster
                    ClusterDeliveryEndPoint.this.close();
                    return;
                }
            }
        }

    }

    public ClusterDeliveryEndPoint(String label, ScheduledExecutorService scheduler) {

        this.label = label;
        this.scheduler = scheduler;
    }

    /**
     * add deliveryEP to cluster when a new client joins.
     * 
     * @param channelEP
     *            the deliveryEP of the new client
     * @param messageWindowSize
     *            the window size of this client
     */
    public void addDeliveryEndPoint(DeliveryEndPoint channelEP, int messageWindowSize) {
        addDeliveryEndPoint(channelEP, new DeliveryState(messageWindowSize));
    }

    private void addDeliveryEndPoint(DeliveryEndPoint endPoint, DeliveryState state) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            deliverableEP.add(endPoint);
            endpoints.put(endPoint, state);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * 
     * when one client's channel is something wrong, all unconsumed messages
     * sent to this client need to be redelivered to other available clients in
     * same cluster, then we run a RedeliveryTask for this fault client.
     * 
     * @param ep
     *            the fault client's deliveryEP
     * @param state
     *            the deliveryState which holds all unconsumed messages of this
     *            fault client
     */
    public void closeAndRedeliver(DeliveryEndPoint ep, DeliveryState state) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            if (null == state) {
                return;
            }
            if (state.msgs.isEmpty()) {
                return;
            }
            // redeliver the state
            scheduler.submit(new RedeliveryTask(state));
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * We set a timeout for every message which has been sent but unconsumed
     * yet. If this message hasn't been consumed by its responsible client
     * whithin timeout(maybe suffer some network problem), we will run retry
     * policy, redeliver all timeout unconsumed messages of the whole cluster
     */
    public void closeAndTimeOutRedeliver(Set<DeliveredMessage> msgs) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            // redeliver the state
            scheduler.submit(new TimeOutRedeliveryTask(msgs));
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * when a client drop out this cluster(like sending closeSubscription
     * request),we should remove its deliveryEP from cluster.
     * 
     * @param endPoint
     */
    public void removeDeliveryEndPoint(DeliveryEndPoint endPoint) {
        synchronized (deliverableEP) {
            if (deliverableEP.contains(endPoint)) {
                deliverableEP.remove(endPoint);
            } else
                throttledEP.remove(endPoint);
        }

        DeliveryState state = endpoints.remove(endPoint);
        if (endpoints.isEmpty()) {
            close();
        }
        if (null == state) {
            return;
        }
        if (state.msgs.isEmpty()) {
            return;
        }
        closeAndRedeliver(endPoint, state);
    }

    public boolean hasAvailableDeliveryEndPoints() {
        return !deliverableEP.isEmpty();
    }

    /**
     * consume specific message, update throttledEP and deliverableEP.
     */
    @Override
    public boolean messageConsumed(long newSeqIdConsumed) {
        DeliveredMessage msg;

        msg = pendings.remove(newSeqIdConsumed);
        DeliveryEndPoint lastDeliveredEP = msg.lastDeliveredEP;

        if (null != msg && null != lastDeliveredEP) {

            DeliveryState state = endpoints.get(lastDeliveredEP);
            if (state != null) {
                state.msgs.remove(newSeqIdConsumed);
            }
            synchronized (deliverableEP) {
                if (throttledEP.contains(lastDeliveredEP)) {
                    throttledEP.remove(lastDeliveredEP);
                    deliverableEP.offer(lastDeliveredEP);
                    return true;
                }
            }

        }
        return false;
    }

    /**
     * when all clients in cluster are throttled( unconsumed messages' num
     * exceed the client's window size), return true, else return false.
     */
    @Override
    public boolean shouldThrottle(long lastSeqIdDelivered) {
        return deliverableEP.isEmpty();
    }

    /**
     * send message to the cluster
     */
    @Override
    public void send(final PubSubResponse response, final DeliveryCallback callback) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                callback.permanentErrorOnSend();
                return;
            }

            DeliveryEndPoint ep = send(new DeliveredMessage(response));
            if (null == ep) {
                // no delivery channel in endpoints, which means no client
                // in this cluster
                callback.permanentErrorOnSend();
            } else {
                // callback after sending the message
                callback.sendingFinished();
            }

        } finally {
            closeLock.readLock().unlock();
        }
    }

    private DeliveryEndPoint send(final DeliveredMessage msg) {

        DeliveryCallback dcb;

        DeliveryEndPoint clusterEP = null;
        while (!endpoints.isEmpty()) {
            try {
                // blocking to wait for a deliverable EP until 1 second expired
                clusterEP = deliverableEP.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();

            }
            if (null == clusterEP) {
                continue;// if there is no delverable EP ,loop
            }
            DeliveryState state = endpoints.get(clusterEP);
            // update the message's deliver time and deliverEP
            msg.resetDeliveredTime(clusterEP);
            // add this msgs to current delivery endpoint state
            dcb = new ClusterDeliveryCallback(clusterEP, state, msg);

            long seqid = msg.msg.getMessage().getMsgId().getLocalComponent();
            // add this message into the cluster's unconsumed messages' map
            pendings.put(seqid, msg);

            // check whether this deliveryEndpoint should be throttled,
            if (state.msgs.size() < state.messageWindowSize) {
                deliverableEP.offer(clusterEP);
            } else {
                throttledEP.offer(clusterEP);
            }

            clusterEP.send(msg.msg, dcb);
            // if this operation fails, trigger redelivery of this message.
            return clusterEP;

        }
        return null;

    }

    /**
     * send SubscriptionEvent to every client in this cluster.
     * 
     * @param resp
     *            the SubscriptionEvent
     */

    public void sendSubscriptionEvent(PubSubResponse resp) {
        List<DeliveryEndPoint> eps = new ArrayList<DeliveryEndPoint>(deliverableEP);
        eps.addAll(throttledEP);

        for (final DeliveryEndPoint clusterEP : eps) {
            clusterEP.send(resp, new DeliveryCallback() {

                @Override
                public void sendingFinished() {
                    // do nothing
                }

                @Override
                public void transientErrorOnSend() {
                    closeAndRedeliver(clusterEP, endpoints.get(clusterEP));
                }

                @Override
                public void permanentErrorOnSend() {
                    closeAndRedeliver(clusterEP, endpoints.get(clusterEP));
                }

            });
        }
    }

    /**
     * close the cluster when no client in this cluster.
     */
    @Override
    public void close() {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }
    }

}
