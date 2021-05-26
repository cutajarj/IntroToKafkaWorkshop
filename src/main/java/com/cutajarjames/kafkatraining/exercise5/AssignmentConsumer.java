package com.cutajarjames.kafkatraining.exercise5;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class AssignmentConsumer {
    private final static String TOPIC = "kafkaTrainingSequence";

    //1. Change the servers here
    private final static String SERVERS = "localhost:9092,localhost:9092,localhost:9092";

    //2. Implement create consumer
    private Consumer<String, Long> createConsumer() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        //3. Put more properties such as deserializers, group id, and a way to consume from the end

        return null;
    }

    //4. Implement start consumer
    public void startConsumer() {
        var consumer = createConsumer();
        while (true) {
            //5. Poll
            //6. Print out the partitions assignments from the consumer
        }
    }


    //7. Run this class and after a few seconds, start a second instance of it
    public static void main(String[] args) throws InterruptedException {
        var consumer = new com.cutajarjames.kafkatraining.solutions.exercise5.AssignmentConsumer();
        consumer.startConsumer();
    }
}
