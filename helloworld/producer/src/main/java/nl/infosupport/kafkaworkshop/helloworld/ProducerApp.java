package nl.infosupport.kafkaworkshop.helloworld;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerApp {
    public static void main(String[] args) {
        Properties producerProperties = new Properties();

        // Required properties:
        //  - A list of brokers separated by a semicolon.
        //  - A serializer so that messages can be converted to Kafka specific bits
        // - The number of acknowledgements you want back before the message is considered to be sent succesfully

        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("serializer.class", "kafka.serializer.StringEncoder");
        producerProperties.put("request.required.acks", "1");

        // Set some default deserializers for the consumed messages.
        // This is required because otherwise Kafka doesn't know what to do with the data.
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

        // Send a single keyed message to the server.
        // Each message in Kafka has a key which you can set to anything you like.
        // It also has a payload, which you can fill with any data you like as long as it can be
        // serialized by the encoder. Normally you'd use something like BSON or JSON here.
        ProducerRecord<String, String> msg = new ProducerRecord<String, String>("events", "Hello world!");

        producer.send(msg);
    }
}
