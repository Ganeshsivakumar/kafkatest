package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            try {
                System.out.println("started to publish data");
                producer.send(new ProducerRecord<>("testtopic", "key-" + i, "message-dummymessage" + i));
                System.out.println("done");
            } catch (Exception e) {
                System.out.println("error occured" + e);
            }
        }
        producer.close();
    }
}
