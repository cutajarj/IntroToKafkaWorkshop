package com.cutajarjames.kafkatraining.exercise4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    private final static String[] INCIDENTS_TO_USE = {"HOME_POINT", "AWAY_POINT", "HOME_FOUL", "AWAY_FOUL", "NEW_PHASE", "SUBSTITUTION"};

    private final static String TOPIC = "kafkaTrainingBasketball";

    //1. Change the servers here
    private final static String SERVERS = "localhost:9092,localhost:9092,localhost:9092";


    //2. Implement create producer
    private Producer<String, String> createProducer() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        //3. Send the correct serializers and return
        return null;
    }

    public void startProducer() throws InterruptedException {
        var producer = createProducer();
        for (int i = 0; i < 1000; i++) {
            //4. Choose a random incident from INCIDENTS_TO_USE

            //5. Send the random incident, using a "<insertTeamA> vs <insertTeamB>" as key (choose whatever teams you want)

            Thread.sleep(3000);
        }
        //6. Close producer
    }

    public static void main(String[] args) throws InterruptedException {
        var simpleProducer = new SimpleProducer();
        simpleProducer.startProducer();
    }
}
