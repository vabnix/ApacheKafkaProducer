package com.vaibhav.learning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";

        //Creating Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating Producer
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>("first_spring_topic", "Hello Vaibhav");
        //Sending data
        kafkaProducer.send(record);

        //flush data
        kafkaProducer.flush();
        kafkaProducer.close();

        SpringApplication.run(KafkaApplication.class, args);
    }

}
