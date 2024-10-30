package com.example;

import java.util.Collections;
import java.util.Properties;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

public class TestConsumer extends PTransform<PBegin, PCollection<String>> {

    @Override
    public PCollection<String> expand(PBegin input) {
        return input.apply(Create.of((Void) null))
                .apply("read datat", ParDo.of(new ReadFromKafkaDoFn()));
    }

    static class ReadFromKafkaDoFn extends DoFn<Void, String> {

        private KafkaConsumer<String, String> consumer;

        @Setup
        public void setup() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "test-group4");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.offset.reset", "earliest");
            props.put("max.poll.records", "500"); // Limit records per poll
            props.put("log_level", "DEBUG");

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList("testtopic"));

        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            // Poll Kafka and output each record
            System.out.println("started to process");
            try {
                System.out.println("started to read data");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("data is " + record);
                    context.output(record.value());
                }
            } catch (Exception e) {
                System.out.println("error happened: " + e);
            }

            /*
             * try {
             * for (int i = 0; i < 10; i++) { // Example loop to extend polling time
             * ConsumerRecords<String, String> records =
             * consumer.poll(Duration.ofMillis(500));
             * for (ConsumerRecord<String, String> record : records) {
             * System.out.println("data is " + record);
             * System.out.println("vaue is " + record.value());
             * context.output(record.value());
             * }
             * Thread.sleep(500); // To simulate continuous reading
             * }
             * } catch (Exception e) {
             * System.out.println("error happened: " + e);
             * }
             */

        }

        @Teardown
        public void teardown() {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

}
