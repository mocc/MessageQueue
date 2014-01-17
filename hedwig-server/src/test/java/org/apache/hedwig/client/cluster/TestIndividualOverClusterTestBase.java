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
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;

@RunWith(Parameterized.class)
public class TestIndividualOverClusterTestBase extends HedwigHubTestBase {

    private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 10;
    static final Logger LOG = LoggerFactory.getLogger(HedwigAdmin.class);
    // Client side variables
    protected HedwigClient client;
    protected Publisher publisher;
    protected Subscriber subscriber;

    protected class myServerConfiguration extends HubServerConfiguration {

        myServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }

        @Override
        public int getDefaultMessageWindowSize() {
            return TestIndividualOverClusterTestBase.this.DEFAULT_MESSAGE_WINDOW_SIZE;
        }
    }

    protected class myClientConfiguration extends HubClientConfiguration {

        int messageWindowSize;

        myClientConfiguration() {
            this(TestIndividualOverClusterTestBase.this.DEFAULT_MESSAGE_WINDOW_SIZE);
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

    public TestIndividualOverClusterTestBase(boolean isSubscriptionChannelSharingEnabled) {
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
        client = new HedwigClient(new myClientConfiguration());
        publisher = client.getPublisher();
        subscriber = client.getSubscriber();
    }
    @Override
    @After
    public void tearDown() throws Exception {
    	System.out.println("enter tearDown");
    	client.close();
        super.tearDown();
        System.out.println("after tearDown");
    }
    @Override
    protected ServerConfiguration getServerConfiguration(int port, int sslPort) {
        return new myServerConfiguration(port, sslPort);
    }

    @Test(timeout=60000)
    public void testAsyncPublishWithResponse() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("testAsyncPublishWithResponse");
        ByteString subid = ByteString.copyFromUtf8("mysubid");

        final String prefix = "AsyncMessage-";
        final int numMessages = 30;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
       
        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);

        SubscriptionOptions opts = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(100).build();
        
        subscriber.subscribe(topic, subid, opts);
        
        for (int i=0; i<numMessages; i++) {
            final String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            publisher.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
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
        
        subscriber.startDelivery(topic, subid, new MessageHandler() {
            synchronized public void deliver(ByteString topic, ByteString subscriberId,
                                             Message msg, Callback<Void> callback,
                                             Object context) {
                String str = msg.getBody().toStringUtf8();
                //System.out.println(msg.getMsgId().getLocalComponent());  start from 1
                LOG.info("local fetch message:"+str);
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                
                callback.operationFinished(context, null);
            }
        });
        
        assertTrue("Timed out waiting on callback for publish requests.",
                   publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.",
                     numMessages, numPublished.get());
       
        assertTrue("Timed out waiting on callback for messages.",
                   receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " messages.",
                     numMessages, numReceived.get());

    }

}
