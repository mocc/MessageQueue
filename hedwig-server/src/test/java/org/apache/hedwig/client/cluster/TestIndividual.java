/**
 * test by ly.
 */
package org.apache.hedwig.client.cluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
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

import com.google.protobuf.ByteString;

import org.apache.commons.logging.LogFactory;
import org.apache.hedwig.admin.HedwigAdmin;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.PubSubServerStandAloneTestBase;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.SubscriptionListener;
import org.apache.hedwig.util.HedwigSocketAddress;

@RunWith(Parameterized.class)
public class TestIndividual extends PubSubServerStandAloneTestBase {

    private static final int RETENTION_SECS_VALUE = 10;
    static final Logger LOG = LoggerFactory.getLogger(HedwigAdmin.class);
    // Client side variables
    protected HedwigClient client;
    protected Publisher publisher;
    protected Subscriber subscriber;
    
    protected class RetentionServerConfiguration extends StandAloneServerConfiguration {
        @Override
        public boolean isStandalone() {
            return true;
        }

        @Override
        public int getRetentionSecs() {
            return RETENTION_SECS_VALUE;
        }
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }/*, { false }*/ });
    }

    protected boolean isSubscriptionChannelSharingEnabled;

    public TestIndividual(boolean isSubscriptionChannelSharingEnabled) {
        this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = new HedwigClient(new ClientConfiguration() {
            @Override
            public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
                return getDefaultHedwigAddress();
            }

            @Override
            public boolean isSubscriptionChannelSharingEnabled() {
                return TestIndividual.this.isSubscriptionChannelSharingEnabled;
            }

			@Override
			public boolean isAutoSendConsumeMessageEnabled() {
				// TODO Auto-generated method stub
				return false;
			}
            
        });
        publisher = client.getPublisher();
        subscriber = client.getSubscriber();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        client.close();
        super.tearDown();
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
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(15).build();
        
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
       
    	for (int i=1; i<=15; i++) {
    		subscriber.consume(topic, subid,
				            MessageSeqId.newBuilder().setLocalComponent(i).build());
			
    	}
        
        assertTrue("Timed out waiting on callback for messages.",
                   receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " messages.",
                     numMessages, numReceived.get());

    }

}
