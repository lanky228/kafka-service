package com.liuzhengri;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleKafkaProducer {
    private static KafkaProducer<String, String> producer;
    private final static String TOPIC = "adienTest2";

    private SimpleKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //设置分区类,根据key进行数据分区
        producer = new KafkaProducer<>(props);
    }

    private void produce() {
        for (int i = 30; i < 40; i++) {
            String key = String.valueOf(i);
            String data = "hello kafka message：" + key;
            producer.send(new ProducerRecord<>(TOPIC, key, data));
            System.out.println(data);
        }
        producer.close();
    }

    public static void main(String[] args) {
        new SimpleKafkaProducer().produce();
    }
}