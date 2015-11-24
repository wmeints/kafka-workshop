package nl.infosupport.kafkaworkshop.helloworld;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);

    public static void main(String[] args) throws Exception {

        // Required properties:
        //  - zookeeper host used to discover brokers in your cluster from which messages need to be fetched
        //  - group ID to identify the application that is fetching (you can have multiple instances of a consumer)

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", "localhost:9092");
        consumerProperties.put("group.id", "consumerapp");

        // Set some default deserializers for the consumed messages.
        // This is required because otherwise Kafka doesn't know what to do with the data.
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Use autocommit settings so that you don't need to commit the messages you received.
        // This is typically done in scenarios where consistency isn't an issue.
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.commit.interval.ms", "1000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
        consumer.subscribe(Arrays.asList("events"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(msg -> log.info("Received message: {}", msg.value()));
        }
    }
}
