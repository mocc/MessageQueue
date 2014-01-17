package message_queue_client;

import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.jboss.netty.channel.ChannelFactory;

public class MessageQueueClient {
    private final HedwigClient client;

    public MessageQueueClient(ClientConfiguration cfg) {
        client = new HedwigClient(cfg);
    }

    public MessageQueueClient(ClientConfiguration cfg, ChannelFactory socketFactory) {
        client = new HedwigClient(cfg, socketFactory);
    }

    public MessageQueueSender getSender() {
        return new MessageQueueSender(client.getPublisher());
    }

    public MessageQueueReceiver getReceiver() {
        return new MessageQueueReceiver(client.getSubscriber());
    }

    public void close() {
        client.close();
    }
}
