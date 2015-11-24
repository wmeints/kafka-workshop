package nl.infosupport.kafkaworkshop.fanout;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessageListener {

    private final ConsumerConnector connector;
    private final Logger log = LoggerFactory.getLogger(MessageListener.class);
    private ExecutorService threadPool;

    public MessageListener(ConsumerConnector connector) {
        this.connector = connector;
    }

    /**
     * Starts listening to a topic reading the specified number of partitions
     *
     * @param topic         Topic to read from
     * @param partitions    Number of partitions to read from
     * @param messageAction Action to execute for each message received
     */
    public void start(String topic, int partitions, MessageAction messageAction) {
        if(threadPool != null) {
            throw new RuntimeException("The listener is already running!");
        }

        Map<String, Integer> topicCount = new HashMap<>();
        topicCount.put(topic, partitions);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                connector.createMessageStreams(topicCount);

        threadPool = Executors.newFixedThreadPool(partitions);

        for (KafkaStream<byte[], byte[]> stream : consumerStreams.get("events")) {
            threadPool.submit(new MessageHandler(stream, messageAction));
            log.debug("Started listener for topic");
        }
    }

    public void stop() {
        threadPool.shutdown();
    }

    private static class MessageHandler implements Runnable {
        private final KafkaStream<byte[], byte[]> stream;
        private MessageAction action;
        private final Logger log = LoggerFactory.getLogger(MessageHandler.class);

        public MessageHandler(KafkaStream<byte[], byte[]> stream, MessageAction action) {
            this.stream = stream;
            this.action = action;
        }

        public void run() {
            // The iterator used in this method never ends unless you stop the threadpool
            // This means that you don't have to worry about exit conditions here ;-)
            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

            while (iterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> messageData = iterator.next();

                log.debug("Received message from topic");

                action.execute(new String(messageData.message()));
            }
        }
    }
}
