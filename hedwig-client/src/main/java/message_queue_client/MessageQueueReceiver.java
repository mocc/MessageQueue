package message_queue_client;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;

import com.google.protobuf.ByteString;

public class MessageQueueReceiver {
    public static final String QUEUE_PREFIX = "QUEUE_";
    public static final ByteString subscriberId = ByteString.copyFromUtf8("QueueReceiver");

    private final Subscriber subscriber;

    public MessageQueueReceiver(Subscriber subscriber) {
        this.subscriber = subscriber;
    }

    public void receiveMessage(final String queue, final SubscriptionOptions options,
            final MessageHandler messageHandler) throws CouldNotConnectException, ClientAlreadySubscribedException,
            ServiceDownException, InvalidSubscriberIdException, ClientNotSubscribedException,
            AlreadyStartDeliveryException {

        ByteString topic = ByteString.copyFromUtf8(QUEUE_PREFIX + queue);
        subscriber.subscribe(topic, subscriberId, options);
        subscriber.startDelivery(topic, subscriberId, messageHandler);
    }

    public void receiveMessageWithFilter(final String queue, final SubscriptionOptions options,
            final ClientMessageFilter messageFilter, MessageHandler messageHandler) throws CouldNotConnectException,
            ClientAlreadySubscribedException, ServiceDownException, InvalidSubscriberIdException,
            ClientNotSubscribedException, AlreadyStartDeliveryException {

        ByteString topic = ByteString.copyFromUtf8(QUEUE_PREFIX + queue);
        subscriber.subscribe(topic, subscriberId, options);
        subscriber.startDeliveryWithFilter(topic, subscriberId, messageHandler, messageFilter);
    }

    public void commitMessage(final String queue, MessageSeqId messageSeqId) throws ClientNotSubscribedException {
        subscriber.consume(ByteString.copyFromUtf8(QUEUE_PREFIX + queue), subscriberId, messageSeqId);
    }

    public void stopReceiving(final String queue) throws CouldNotConnectException, ClientNotSubscribedException,
            ServiceDownException, InvalidSubscriberIdException {

        subscriber.closeSubscription(ByteString.copyFromUtf8(QUEUE_PREFIX + queue), subscriberId);
    }
}
