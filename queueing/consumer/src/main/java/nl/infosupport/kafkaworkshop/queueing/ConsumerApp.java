package nl.infosupport.kafkaworkshop.queueing;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.*;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class ConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);

    public static void main(String[] args) throws Exception {

        // Required properties:
        //  - zookeeper host used to discover brokers in your cluster from which messages need to be fetched
        //  - group ID to identify the application that is fetching (you can have multiple instances of a consumer)

        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", "localhost:2181");
        consumerProperties.put("group.id", "consumerapp");

        ConsumerConnector consumerConnector =
                Consumer.createJavaConsumerConnector(
                        new ConsumerConfig(consumerProperties));

        consumerConnector.commitOffsets();

        // Kafka is capable of using multiple threads to process incoming messages.
        // However, for a queueing scenario you have to leave this to just one, otherwise this turns into a troublesome
        // scenario pretty quickly!
        Map<String, Integer> topicCount = new HashMap<>();
        topicCount.put("events", 1);

        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector.createMessageStreams(topicCount);

        for (KafkaStream<byte[], byte[]> stream : consumerStreams.get("events")) {
            log.info("Starting message handler for topic events");
            threadPool.submit(new MessageHandler(consumerConnector, stream));
        }

        // The rest of this method is a nasty trick to prevent Java from shutting down on us.
        // Please ignore this and don't use if you have better ways to control the runtime.
        Semaphore signal = new Semaphore(0, false);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                threadPool.shutdown();
                signal.release();
            }
        });

        signal.acquire();
    }

    private static class MessageHandler implements Runnable {
        private final ConsumerConnector consumer;
        private final KafkaStream<byte[], byte[]> stream;
        private final Logger log = LoggerFactory.getLogger(MessageHandler.class);

        public MessageHandler(ConsumerConnector consumer, KafkaStream<byte[], byte[]> stream) {
            this.consumer = consumer;
            this.stream = stream;
        }

        public void run() {
            // The iterator used in this method never ends unless you stop the threadpool
            // This means that you don't have to worry about exit conditions here ;-)
            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

            while (iterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> messageData = iterator.next();

                // WARNING: If your producer does not provide a key, this method will never complete.
                // This is due to the fact that Kafka blocks the call until it finds the data for the key, which
                // is of course not available if you don't provide a key.
                log.info("Received message {}", new String(new String(messageData.message())));

                // When using Kafka in a queueing scenario you have to disable to autocommit setting
                // and start committing offsets yourself. Committing is an expensive operation so this slows
                // the whole application down. If you can, save up a few commits and send one request.
                consumer.commitOffsets();
            }
        }
    }
}
