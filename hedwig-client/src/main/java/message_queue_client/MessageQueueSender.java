package message_queue_client;

import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.util.Callback;

import com.google.protobuf.ByteString;

public class MessageQueueSender {

    public static final String QUEUE_PREFIX = "QUEUE_";
    private final Publisher publisher;

    public MessageQueueSender(Publisher publisher) {
        this.publisher = publisher;
    }

    public void createQueue(final String queue) throws CouldNotConnectException, ServiceDownException {
        publisher.publish(ByteString.copyFromUtf8(QUEUE_PREFIX + queue), null);
    }

    public void sendMessage(final String queue, Message msg) throws CouldNotConnectException, ServiceDownException {
        publisher.publish(ByteString.copyFromUtf8(QUEUE_PREFIX + queue), msg);
    }

    public void asyncSendMessage(final String queue, Message msg, Callback<Void> callback, Object context) {
        publisher.asyncPublish(ByteString.copyFromUtf8(QUEUE_PREFIX + queue), msg, callback, context);
    }

    public void asyncSendMessageWithResponse(final String queue, Message msg, Callback<PublishResponse> callback,
            Object context) {
        publisher.asyncPublishWithResponse(ByteString.copyFromUtf8(QUEUE_PREFIX + queue), msg, callback, context);
    }
}
