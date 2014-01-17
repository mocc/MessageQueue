package org.apache.hedwig.client.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionType;
import org.apache.hedwig.server.PubSubServerStandAloneTestBase;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;

import com.google.protobuf.ByteString;

public class TestClusterClientWindowThrottling extends PubSubServerStandAloneTestBase {
    // Client side variables
    protected HedwigClient client1, client2, client3;
    protected Publisher publisher1, publisher2, publisher3;
    protected Subscriber subscriber1, subscriber2, subscriber3;
    protected boolean isAutoSendConsumeMessageEnabled;

    protected class ClusterClientConfiguration extends ClientConfiguration {
        @Override
        public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
            return getDefaultHedwigAddress();
        }

        @Override
        public boolean isAutoSendConsumeMessageEnabled() {
            return TestClusterClientWindowThrottling.this.isAutoSendConsumeMessageEnabled;

        }
    }

    public ClientConfiguration getClusterClientConfiguration() {
        return new ClusterClientConfiguration();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        client1.close();
        client2.close();
        client3.close();

        super.tearDown();
    }

    public void testClusterSubscriptionWithAutoConsume() throws Exception {
        TestClusterClientWindowThrottling.this.isAutoSendConsumeMessageEnabled = true;
        final int clientWindowSize1 = 20;
        final int clientWindowSize2 = 30;
        final int clientWindowSize3 = 20;
        ClientConfiguration conf = getClusterClientConfiguration();

        client1 = new HedwigClient(conf);
        client2 = new HedwigClient(conf);
        client3 = new HedwigClient(conf);

        publisher1 = client1.getPublisher();
        publisher2 = client2.getPublisher();
        publisher3 = client3.getPublisher();

        subscriber1 = client1.getSubscriber();
        subscriber2 = client2.getSubscriber();
        subscriber3 = client3.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("testClusterSubscriptionWithAutoConsume");
        ByteString subscriberId = ByteString.copyFromUtf8("mysubid");

        final String prefix = "message";
        final int numMessages = 100;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final ConcurrentHashMap<String, MessageSeqId> receivedMsgs = new ConcurrentHashMap<String, MessageSeqId>();

        SubscriptionOptions opts1 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize1).build();

        SubscriptionOptions opts2 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize2).build();

        SubscriptionOptions opts3 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize3).build();

        subscriber1.subscribe(topic, subscriberId, opts1);
        subscriber2.subscribe(topic, subscriberId, opts2);
        subscriber3.subscribe(topic, subscriberId, opts3);

        subscriber1.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        subscriber2.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        subscriber3.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);

            }
        });

        for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            publisher1.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);
        }

        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.", numMessages, numPublished.get());
        assertEquals("Should be expected " + numMessages + " publish responses.", numMessages, publishedMsgs.size());

        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(20, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " messages.", numMessages, numReceived.get());
        assertEquals("Should be expected " + numMessages + " messages in map.", numMessages, receivedMsgs.size());

        for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
            MessageSeqId pubId = publishedMsgs.get(str);
            MessageSeqId revId = receivedMsgs.get(str);
            assertTrue("Doesn't receive same message seq id for " + str, pubId.equals(revId));
        }

    }

    public void testClusterSubscriptionWithConsume() throws Exception {
        TestClusterClientWindowThrottling.this.isAutoSendConsumeMessageEnabled = false;
        final int clientWindowSize1 = 20;
        final int clientWindowSize2 = 30;
        final int clientWindowSize3 = 20;
        ClientConfiguration conf = getClusterClientConfiguration();

        client1 = new HedwigClient(conf);
        client2 = new HedwigClient(conf);
        client3 = new HedwigClient(conf);

        publisher1 = client1.getPublisher();
        publisher2 = client2.getPublisher();
        publisher3 = client3.getPublisher();

        subscriber1 = client1.getSubscriber();
        subscriber2 = client2.getSubscriber();
        subscriber3 = client3.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("testClusterSubscriptionWithConsume");
        ByteString subscriberId = ByteString.copyFromUtf8("mysubid");

        final String prefix = "message";
        final int numMessages = 100;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final ConcurrentHashMap<String, MessageSeqId> receivedMsgs = new ConcurrentHashMap<String, MessageSeqId>();

        SubscriptionOptions opts1 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize1).build();

        SubscriptionOptions opts2 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize2).build();

        SubscriptionOptions opts3 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize3).build();

        subscriber1.subscribe(topic, subscriberId, opts1);
        subscriber2.subscribe(topic, subscriberId, opts2);
        subscriber3.subscribe(topic, subscriberId, opts3);

        subscriber1.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                logger.debug(str + " received.");
                try {
                    subscriber1.consume(topic, subscriberId, msg.getMsgId());
                } catch (ClientNotSubscribedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        subscriber2.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                logger.debug(str + " received.");
                try {
                    subscriber2.consume(topic, subscriberId, msg.getMsgId());
                } catch (ClientNotSubscribedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        subscriber3.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                logger.debug(str + " received.");
                try {
                    subscriber3.consume(topic, subscriberId, msg.getMsgId());
                } catch (ClientNotSubscribedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);

            }
        });

        for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            publisher1.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);
        }

        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.", numMessages, numPublished.get());
        assertEquals("Should be expected " + numMessages + " publish responses.", numMessages, publishedMsgs.size());

        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(20, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " messages.", numMessages, numReceived.get());
        assertEquals("Should be expected " + numMessages + " messages in map.", numMessages, receivedMsgs.size());

        for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
            MessageSeqId pubId = publishedMsgs.get(str);
            MessageSeqId revId = receivedMsgs.get(str);
            assertTrue("Doesn't receive same message seq id for " + str, pubId.equals(revId));
        }

    }

    public void testClusterClientWindowWithoutConsume() throws Exception {
        TestClusterClientWindowThrottling.this.isAutoSendConsumeMessageEnabled = false;
        final int clientWindowSize1 = 20;
        final int clientWindowSize2 = 30;
        final int clientWindowSize3 = 20;
        final int clusterWindowSize = clientWindowSize1 + clientWindowSize2 + clientWindowSize3;
        ClientConfiguration conf = getClusterClientConfiguration();

        client1 = new HedwigClient(conf);
        client2 = new HedwigClient(conf);
        client3 = new HedwigClient(conf);

        publisher1 = client1.getPublisher();
        publisher2 = client2.getPublisher();
        publisher3 = client3.getPublisher();

        subscriber1 = client1.getSubscriber();
        subscriber2 = client2.getSubscriber();
        subscriber3 = client3.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("testClusterClientWindowThrottling");
        ByteString subscriberId = ByteString.copyFromUtf8("mysubid");

        final String prefix = "message";
        final int numMessages = 100;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        // final CountDownLatch receiveLatch = new CountDownLatch(1);

        final Map<String, MessageSeqId> receivedMsgs1 = new HashMap<String, MessageSeqId>();
        final Map<String, MessageSeqId> receivedMsgs2 = new HashMap<String, MessageSeqId>();
        final Map<String, MessageSeqId> receivedMsgs3 = new HashMap<String, MessageSeqId>();
        final ConcurrentHashMap<String, MessageSeqId> receivedMsgs = new ConcurrentHashMap<String, MessageSeqId>();

        SubscriptionOptions opts1 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize1).build();

        SubscriptionOptions opts2 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize2).build();

        SubscriptionOptions opts3 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize3).build();

        subscriber1.subscribe(topic, subscriberId, opts1);
        subscriber2.subscribe(topic, subscriberId, opts2);
        subscriber3.subscribe(topic, subscriberId, opts3);

        subscriber1.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs1.put(str, msg.getMsgId());
                logger.debug(str + " received.");
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                // if (clusterWindowSize == numReceived.incrementAndGet()) {
                // receiveLatch.countDown();
                // }
                callback.operationFinished(context, null);
            }
        });

        subscriber2.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs2.put(str, msg.getMsgId());
                logger.debug(str + " received.");
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                // if (clusterWindowSize == numReceived.incrementAndGet()) {
                // receiveLatch.countDown();
                // }
                callback.operationFinished(context, null);
            }
        });

        subscriber3.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs3.put(str, msg.getMsgId());
                logger.debug(str + " received.");
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                // if (clusterWindowSize == numReceived.incrementAndGet()) {
                // receiveLatch.countDown();
                // }
                callback.operationFinished(context, null);

            }
        });

        for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            publisher1.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);
        }

        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.", numMessages, numPublished.get());
        assertEquals("Should be expected " + numMessages + " publish responses.", numMessages, publishedMsgs.size());

        // assertTrue("Timed out waiting on callback for messages.",
        // receiveLatch.await(10, TimeUnit.SECONDS));
        Thread.sleep(10000);
        assertEquals("client1 Should be expected " + clientWindowSize1 + " messages received.", clientWindowSize1,
                receivedMsgs1.size());
        assertEquals("client2 Should be expected " + clientWindowSize2 + " messages received.", clientWindowSize2,
                receivedMsgs2.size());
        assertEquals("client3 Should be expected " + clientWindowSize3 + " messages received.", clientWindowSize3,
                receivedMsgs3.size());
        assertEquals("cluster Should be expected " + clusterWindowSize + " messages in map.", clusterWindowSize,
                receivedMsgs.size());

    }

    public void testClusterClientWindowWithRedelivery() throws Exception {
        TestClusterClientWindowThrottling.this.isAutoSendConsumeMessageEnabled = false;
        final int clientWindowSize1 = 30;
        final int clientWindowSize2 = 25;
        final int clientWindowSize3 = 20;
        // final int clusterWindowSize = clientWindowSize1 + clientWindowSize2 +
        // clientWindowSize3;
        ClientConfiguration conf = getClusterClientConfiguration();

        client1 = new HedwigClient(conf);
        client2 = new HedwigClient(conf);
        client3 = new HedwigClient(conf);

        publisher1 = client1.getPublisher();
        publisher2 = client2.getPublisher();
        publisher3 = client3.getPublisher();

        subscriber1 = client1.getSubscriber();
        subscriber2 = client2.getSubscriber();
        subscriber3 = client3.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("testRedelivery");
        ByteString subscriberId = ByteString.copyFromUtf8("mysubid");

        final String prefix = "message";
        final int numMessages = 100;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);

        final Map<String, MessageSeqId> receivedMsgs1 = new HashMap<String, MessageSeqId>();
        final Map<String, MessageSeqId> receivedMsgs2 = new HashMap<String, MessageSeqId>();
        final Map<String, MessageSeqId> receivedMsgs3 = new HashMap<String, MessageSeqId>();
        final ConcurrentHashMap<String, MessageSeqId> receivedMsgs = new ConcurrentHashMap<String, MessageSeqId>();

        SubscriptionOptions opts1 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize1).build();

        SubscriptionOptions opts2 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize2).build();

        SubscriptionOptions opts3 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(clientWindowSize3).build();

        subscriber1.subscribe(topic, subscriberId, opts1);
        subscriber2.subscribe(topic, subscriberId, opts2);
        subscriber3.subscribe(topic, subscriberId, opts3);

        subscriber1.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {

                String str = msg.getBody().toStringUtf8();
                receivedMsgs1.put(str, msg.getMsgId());
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                try {
                    subscriber1.consume(topic, subscriberId, msg.getMsgId());
                } catch (ClientNotSubscribedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);

            }
        });

        subscriber2.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs2.put(str, msg.getMsgId());
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                try {
                    subscriber2.consume(topic, subscriberId, msg.getMsgId());
                } catch (ClientNotSubscribedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        subscriber3.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs3.put(str, msg.getMsgId());
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                try {
                    subscriber3.consume(topic, subscriberId, msg.getMsgId());
                } catch (ClientNotSubscribedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);

            }
        });

        for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            publisher1.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);
        }

        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.", numMessages, numPublished.get());
        assertEquals("Should be expected " + numMessages + " publish responses.", numMessages, publishedMsgs.size());
        // subscriber1.closeSubscription(topic, subscriberId);
        client1.close();
        Thread.sleep(10000);
        // subscriber2.stopDelivery(topic, subscriberId);

        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(50, TimeUnit.SECONDS));
        assertEquals("should be expected " + numMessages + "messages received.", numMessages, receivedMsgs.size());
        logger.debug("client1 received " + receivedMsgs1.size() + "messages.");
        logger.debug("client2 received " + receivedMsgs2.size() + "messages.");
        logger.debug("client3 received " + receivedMsgs3.size() + "messages.");

    }
}
