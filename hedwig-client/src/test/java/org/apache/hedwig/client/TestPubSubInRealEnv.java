package org.apache.hedwig.client;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionType;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;

import com.google.protobuf.ByteString;

public class TestPubSubInRealEnv {

    ClientConfiguration cfg;
    HedwigClient client;
    Publisher pub;
    Subscriber sub;

    public TestPubSubInRealEnv(String hubServer) {
        // TODO Auto-generated constructor stub
        this.cfg = new FinalClientConfiguration(hubServer);
        this.client = new HedwigClient(cfg);
        this.pub = client.getPublisher();
        this.sub = client.getSubscriber();
    }

    protected class FinalClientConfiguration extends ClientConfiguration {

        String hubServer;

        private FinalClientConfiguration(String hubServer) {
            this.hubServer = hubServer;
        }

        @Override
        protected HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
            if (myDefaultServerAddress == null)
                myDefaultServerAddress = new HedwigSocketAddress(hubServer);
            return myDefaultServerAddress;
        }
    }

    public void publish(Publisher pub, String t) throws CouldNotConnectException, ServiceDownException {
        ByteString topic = ByteString.copyFromUtf8(t);
        String prefix = "message";
        int numMsg = 100;
        for (int i = 0; i < numMsg; i++) {
            String str = prefix + i;
            ByteString message = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(message).build();
            pub.publish(topic, msg);
        }
    }

    public void subscribe(Subscriber sub, String t, String subId) throws CouldNotConnectException,
            ClientAlreadySubscribedException, ServiceDownException, InvalidSubscriberIdException {
        ByteString topic = ByteString.copyFromUtf8(t);
        ByteString subscriberId = ByteString.copyFromUtf8(subId);
        SubscriptionOptions subOpt = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.INDIVIDUAL)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).build();
        sub.subscribe(topic, subscriberId, subOpt);
    }

    public void startDelivery(Subscriber sub, String t, String subId) throws ClientNotSubscribedException,
            AlreadyStartDeliveryException {
        ByteString topic = ByteString.copyFromUtf8(t);
        ByteString subscriberId = ByteString.copyFromUtf8(subId);
        sub.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                String message = msg.getBody().toStringUtf8();
                System.out.println(message);
                callback.operationFinished(context, null);

            }
        });

    }

    public static void main(String[] args) throws CouldNotConnectException, ClientAlreadySubscribedException,
            ServiceDownException, InvalidSubscriberIdException, ClientNotSubscribedException,
            AlreadyStartDeliveryException {

        TestPubSubInRealEnv hubSession1 = new TestPubSubInRealEnv("211.68.70.42:4080:9876");
        TestPubSubInRealEnv hubSession2 = new TestPubSubInRealEnv("210.32.133.7:4080:9876");
        TestPubSubInRealEnv hubSession3 = new TestPubSubInRealEnv("210.32.181.184:4080:9876");
        // hubSession1.sub.unsubscribe(ByteString.copyFromUtf8("r2t1"),
        // ByteString.copyFromUtf8("sub1"));
        // hubSession2.subscribe(hubSession2.sub, "test2", "sub3");
        // hubSession2.startDelivery(hubSession2.sub, "test2", "sub3");
        // hubSession1.subscribe(hubSession1.sub, "r2t1", "sub2");
        hubSession3.publish(hubSession3.pub, "topic1");
        // hubSession1.startDelivery(hubSession1.sub, "r2t1", "sub2");

    }
}
