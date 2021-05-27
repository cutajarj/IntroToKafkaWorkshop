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
        //3. Put more properties such as deserializers, group id, and a way to consume from the beginning
        //The key on the record is a STRING, and the value is a LONG
        return null;
    }

    //4. Implement start consumer
    public void startConsumer() {
        var consumer = createConsumer();
        while (true) {
            //5. Poll
            //6. Print records, with their partition key and offset
            //7. Commit offsets
        }
    }

    public static void main(String[] args) {
        var simpleConsumer = new SimpleConsumer();
        simpleConsumer.startConsumer();
    }
}
