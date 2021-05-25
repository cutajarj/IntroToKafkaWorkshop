package com.cutajarjames.kafkatraining.exercise2;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Write a simple kafka consumer that consumes to the TOPIC below from beginning and prints out the records
 * Can you guess the sequences on each key?
 */
public class SimpleConsumer {

    private final static String TOPIC = "kafkaTrainingSequence";

    //1. Change the servers here
    private final static String SERVERS = "localhost:9092,localhost:9092,localhost:9092";

    //2. Implement create consumer
    private Consumer<String, Long> createConsumer() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        return null;
    }

    //3. Implement start consumer
    public void startConsumer() {
        var consumer = createConsumer();
        while (true) {
            //4. Poll
            //5. Print records
            //6. Commit
        }
    }

    public static void main(String[] args) {

    }
}
