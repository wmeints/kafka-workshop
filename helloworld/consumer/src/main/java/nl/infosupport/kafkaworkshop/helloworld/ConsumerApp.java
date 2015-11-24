package nl.infosupport.kafkaworkshop.helloworld;

import kafka.consumer.*;
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

        // Use autocommit settings so that you don't need to commit the messages you received.
        // This is typically done in scenarios where consistency isn't an issue.
        consumerProperties.put("auto.commit.interval.ms", "1000");

        kafka.javaapi.consumer.ConsumerConnector consumerConnector =
                Consumer.createJavaConsumerConnector(
                        new ConsumerConfig(consumerProperties));

        // Kafka is capable of using multiple threads to process incoming messages.
        // Please keep in mind that using more threads doesn't mean that your application will be faster.
        // This setting requires tuning, most likely you're going to use ma. 2 * CPU_COUNT as the setting for this.
        // However, if you require more power in the rest of the app you are best off testing things out using a
        // performance tool.
        int threadCount = 2;

        Map<String,Integer> topicCount = new HashMap<>();
        topicCount.put("events",threadCount);

        Map<String,List<KafkaStream<byte[],byte[]>>> consumerStreams =
                consumerConnector.createMessageStreams(topicCount);

        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);

        for(KafkaStream<byte[],byte[]> stream: consumerStreams.get("events")) {
            log.info("Starting message handler for topic events");
            threadPool.submit(new MessageHandler(stream));
        }

        // The rest of this method is a nasty trick to prevent Java from shutting down on us.
        // Please ignore this and don't use if you have better ways to control the runtime.
        Semaphore signal = new Semaphore(0,false);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                threadPool.shutdown();
                signal.release();
            }
        });

        signal.acquire();
    }

    private static class MessageHandler implements Runnable {
        private final KafkaStream<byte[],byte[]> stream;
        private final Logger log = LoggerFactory.getLogger(MessageHandler.class);

        public MessageHandler(KafkaStream<byte[], byte[]> stream) {
            this.stream = stream;
        }

        public void run() {
            // The iterator used in this method never ends unless you stop the threadpool
            // This means that you don't have to worry about exit conditions here ;-)
            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

            while(iterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> messageData = iterator.next();

                // WARNING: If your producer does not provide a key, this method will never complete.
                // This is due to the fact that Kafka blocks the call until it finds the data for the key, which
                // is of course not available if you don't provide a key.
                log.info("Received message [{}] {}", messageData.key(),messageData.message());
            }
        }
    }
}
