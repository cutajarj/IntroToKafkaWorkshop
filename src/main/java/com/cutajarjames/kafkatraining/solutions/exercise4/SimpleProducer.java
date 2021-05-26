package com.cutajarjames.kafkatraining.solutions.exercise4;

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
        //3. Send the correct serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);
    }

    public void startProducer() throws InterruptedException {
        var producer = createProducer();
        for (int i = 0; i < 1000; i++) {
            //4. Choose a random incident
            var r = (int)(Math.random() * INCIDENTS_TO_USE.length);
            var incident = INCIDENTS_TO_USE[r];

            //5. Send the random incident, using a "<insertTeamA> vs <insertTeamB>" as key (choose whatever teams you want)
            var record = new ProducerRecord<String, String>(TOPIC, "Malta Vs Porto", incident);
            producer.send(record);
            Thread.sleep(3000);
        }
        //6. Close producer
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        var simpleProducer = new SimpleProducer();
        simpleProducer.startProducer();
    }
}
