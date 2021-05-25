package com.cutajarjames.kafkatraining.solutions.exercise2;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class SimpleConsumer {
    private final static String TOPIC = "kafkaTrainingSequence";
    private final static String SERVERS = "ie1-kdp001-qa.qa.betfair:9092,ie1-kdp002-qa.qa.betfair:9092,ie1-kdp003-qa.qa.betfair:9092";
    //private final static String SERVERS = "localhost:9092,localhost:9092,localhost:9092";

    private Consumer<String, Long> createConsumer() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        var consumer = new KafkaConsumer<String, Long>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    public void startConsumer() {
        var consumer = createConsumer();
        while (true) {
            System.out.println("Polling for 10 seconds");
            var records = consumer.poll(Duration.ofSeconds(10));

            records.forEach(record -> {
                System.out.printf("Key: %s, Value: %s, Partition: %d, Offset: %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
    }

    public static void main(String[] args) {
        var simpleConsumer = new SimpleConsumer();
        simpleConsumer.startConsumer();
    }
}
