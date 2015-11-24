package nl.infosupport.kafkaworkshop.queueing;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class ProducerApp {
    public static void main(String[] args) {
        Properties producerProperties = new Properties();

        // Required properties:
        //  - A list of brokers separated by a semicolon.
        //  - A serializer so that messages can be converted to Kafka specific bits
        // - The number of acknowledgements you want back before the message is considered to be sent succesfully

        producerProperties.put("metadata.broker.list","localhost:9092");
        producerProperties.put("serializer.class","kafka.serializer.StringEncoder");
        producerProperties.put("request.required.acks","1");

        ProducerConfig producerConfig = new ProducerConfig(producerProperties);
        Producer<String,String> producer = new Producer<String,String>(producerConfig);

        // Send a single keyed message to the server.
        // Each message in Kafka has a key which you can set to anything you like.
        // It also has a payload, which you can fill with any data you like as long as it can be
        // serialized by the encoder. Normally you'd use something like BSON or JSON here.
        KeyedMessage<String,String> msg = new KeyedMessage<String, String>(
                "events","HelloWorldEvent","Hello world!");

        producer.send(msg);
    }
}
