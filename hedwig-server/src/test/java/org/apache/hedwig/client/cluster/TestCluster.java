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
package org.apache.hedwig.client.cluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;

import org.apache.hedwig.admin.HedwigAdmin;
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
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionType;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;

@RunWith(Parameterized.class)
public class TestCluster extends HedwigHubTestBase {

    private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 10;
    static final Logger LOG = LoggerFactory.getLogger(HedwigAdmin.class);

    protected class myServerConfiguration extends HubServerConfiguration {

        myServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }

        @Override
        public int getDefaultMessageWindowSize() {
            return TestCluster.this.DEFAULT_MESSAGE_WINDOW_SIZE;
        }
    }

    protected class myClientConfiguration extends HubClientConfiguration {

        int messageWindowSize;

        myClientConfiguration() {
            this(TestCluster.this.DEFAULT_MESSAGE_WINDOW_SIZE);
        }

        myClientConfiguration(int messageWindowSize) {
            this.messageWindowSize = messageWindowSize;
        }

        @Override
        public int getMaximumOutstandingMessages() {
            return messageWindowSize;
        }

        void setMessageWindowSize(int messageWindowSize) {
            this.messageWindowSize = messageWindowSize;
        }

        @Override
        public boolean isAutoSendConsumeMessageEnabled() {
            return false;
        }

        @Override
        public boolean isSubscriptionChannelSharingEnabled() {
            return isSubscriptionChannelSharingEnabled;
        }
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false } });
    }

    protected boolean isSubscriptionChannelSharingEnabled;

    public TestCluster(boolean isSubscriptionChannelSharingEnabled) {
        super(1);
        this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
        System.out.println("enter construct");
    }

    @Override
    @Before
    public void setUp() throws Exception {
    	System.out.println("enter setup");  //first construct,then setup
    	System.setProperty("build.test.dir", "F:\\logDir");
        super.setUp();
    }
    @Override
    @After
    public void tearDown() throws Exception {
    	System.out.println("enter tearDown");
    	//client.close();
        super.tearDown();
        System.out.println("after tearDown");
    }
    @Override
    protected ServerConfiguration getServerConfiguration(int port, int sslPort) {
        return new myServerConfiguration(port, sslPort);
    }

    @Test(timeout=60000)
    public void testMutiClients() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("testAsyncPublishWithResponse");
        ByteString subid = ByteString.copyFromUtf8("mysubid");

        final String prefix = "AsyncMessage-";
        final int numMessages = 30;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
       
        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        
        HedwigClient client1 = new HedwigClient(new myClientConfiguration());
        Publisher publisher1 = client1.getPublisher();
        final Subscriber subscriber1 = client1.getSubscriber();
        
        HedwigClient client2 = new HedwigClient(new myClientConfiguration());
        Publisher publisher2 = client2.getPublisher();
        final Subscriber subscriber2 = client2.getSubscriber();
        
        SubscriptionOptions opts = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(100)
            .setSubscriptionType(SubscriptionType.CLUSTER)
            .build();
        
        subscriber1.subscribe(topic, subid, opts);
        subscriber2.subscribe(topic, subid, opts);
        
        for (int i=0; i<numMessages; i++) {
            final String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            publisher1.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
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
        
        subscriber1.startDelivery(topic, subid, new MessageHandler() {
            synchronized public void deliver(ByteString topic, ByteString subscriberId,
                                             Message msg, Callback<Void> callback,
                                             Object context) {
                String str = msg.getBody().toStringUtf8();
                //System.out.println(msg.getMsgId().getLocalComponent());  start from 1
                LOG.info("local fetch message:"+str);
                if (numMessages == numReceived.incrementAndGet()) {
                	try {
						subscriber1.stopDelivery(topic, subscriberId);
						subscriber2.stopDelivery(topic, subscriberId);
					} catch (ClientNotSubscribedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    receiveLatch.countDown();
                }
                
                callback.operationFinished(context, null);
            }
        });
        subscriber2.startDelivery(topic, subid, new MessageHandler() {
            synchronized public void deliver(ByteString topic, ByteString subscriberId,
                                             Message msg, Callback<Void> callback,
                                             Object context) {
                String str = msg.getBody().toStringUtf8();
                //System.out.println(msg.getMsgId().getLocalComponent());  start from 1
                LOG.info("local fetch message:"+str);
                if (numMessages == numReceived.incrementAndGet()) {
                	try {
						subscriber1.stopDelivery(topic, subscriberId);
						subscriber2.stopDelivery(topic, subscriberId);
					} catch (ClientNotSubscribedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    receiveLatch.countDown();
                }
                
                callback.operationFinished(context, null);
            }
        });
        
        assertTrue("Timed out waiting on callback for publish requests.",
                   publishLatch.await(1000, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.",
                     numMessages, numPublished.get());       
        assertTrue("Timed out waiting on callback for messages.",
                   receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " messages.",
                     numMessages, numReceived.get());
        
        
        subscriber1.closeSubscription(topic, subid);
        client1.close();
        subscriber2.closeSubscription(topic, subid);
        client2.close();

    }

}
