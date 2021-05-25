package com.cutajarjames.kafkatraining.solutions.exercise3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class SequenceProducer {
    private final static String TOPIC = "kafkaTrainingSequence";
    private final static String SERVERS = "ie1-kdp001-qa.qa.betfair:9092,ie1-kdp002-qa.qa.betfair:9092,ie1-kdp001-qa.qa.betfair:9092";

    public static void main(String[] args) {
        var props = new Properties();
        props.put("bootstrap.servers", SERVERS);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        var producer = new KafkaProducer<String, Long>(props);
        var seq1 = seq1(500);
        System.out.println(seq1);
        send(seq1, producer, "sequence1");
        var seq2 = seq2(90);
        System.out.println(seq2);
        send(seq2, producer, "sequence2");
        var seq3 = seq3(100);
        System.out.println(seq3);
        send(seq3, producer, "sequence3");
        System.out.println("Done");
        producer.close();
    }

    private static void send(List<Long> seq, KafkaProducer<String, Long> producer, String key) {
        for (Long n : seq) {
            var record = new ProducerRecord<String, Long>(TOPIC, key, n);
            producer.send(record);
        }
    }

    private static List<Long> seq3(int n) {
        var numbers = new ArrayList<Long>(n);
        for (long i = 1; i <= n; i++) {
            numbers.add((i * (i + 1)) / 2);
        }
        return numbers;
    }

    private static List<Long> seq2(int n) {
        var numbers = new ArrayList<>(List.of(1L, 1L));
        for (long i = 2; i < n; i++) {
            numbers.add(numbers.get(numbers.size() - 1) + numbers.get(numbers.size() - 2));
        }
        return numbers;
    }

    private static List<Long> seq1(int n) {
        boolean pTable[] = new boolean[n + 1];
        Arrays.fill(pTable, true);
        for (int p = 2; p * p <= n; p++) {
            if (pTable[p]) {
                for (int i = p * 2; i <= n; i += p) {
                    pTable[i] = false;
                }
            }
        }
        List<Long> result = new LinkedList<>();
        for (int i = 2; i <= n; i++) {
            if (pTable[i]) {
                result.add((long) i);
            }
        }
        return result;
    }
}
