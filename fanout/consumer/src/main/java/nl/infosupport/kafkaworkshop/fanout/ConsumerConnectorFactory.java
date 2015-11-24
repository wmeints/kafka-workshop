package nl.infosupport.kafkaworkshop.fanout;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.Properties;

public class ConsumerConnectorFactory {
    public static ConsumerConnector createConsumer(String hostName, String groupId) {
        // Required properties:
        //  - zookeeper host used to discover brokers in your cluster from which messages need to be fetched
        //  - group ID to identify the application that is fetching (you can have multiple instances of a consumer)

        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", hostName);
        consumerProperties.put("group.id", groupId);

        // Use autocommit settings so that you don't need to commit the messages you received.
        // This is typically done in scenarios where consistency isn't an issue.
        consumerProperties.put("auto.commit.interval.ms", "1000");

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
    }
}
