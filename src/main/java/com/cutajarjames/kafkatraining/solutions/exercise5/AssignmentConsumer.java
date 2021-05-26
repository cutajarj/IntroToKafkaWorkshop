package com.cutajarjames.kafkatraining.solutions.exercise5;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AssignmentConsumer {
    private final static String TOPIC = "kTEx5";

    //1. Change the servers here
    private final static String SERVERS = "localhost:9092,localhost:9092,localhost:9092";

    //2. Implement create consumer
    private Consumer<String, Long> createConsumer() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "COW");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        var consumer = new KafkaConsumer<String, Long>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    //4. Implement start consumer
    public void startConsumer() throws InterruptedException {
        var consumer = createConsumer();
        while (true) {
            consumer.poll(Duration.ofMillis(1000));
            System.out.println(consumer.assignment());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        var consumer = new AssignmentConsumer();
        consumer.startConsumer();
    }
}
