package nl.infosupport.kafkaworkshop.helloworld;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class ProducerApp {
    public static void main(String[] args) {
        Properties producerProperties = new Properties();

        producerProperties.put("metadata.broker.list","localhost:9092");
        producerProperties.put("serializer.class","kafka.serializer.StringEncoder");
        producerProperties.put("request.required.acks","1");

        ProducerConfig producerConfig = new ProducerConfig(producerProperties);
        Producer<String,String> producer = new Producer<String,String>(producerConfig);

        KeyedMessage<String,String> msg = new KeyedMessage<String, String>(
                "events","HelloWorldEvent","Hello world!");

        producer.send(msg);
    }
}
