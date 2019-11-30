package com.vaibhav.learning.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(KafkaApplication.class);
        String bootstrapServer = "127.0.0.1:9092";

        //Creating Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        CallProducer(properties);
        CallProducerWithKey(properties);



        SpringApplication.run(KafkaApplication.class, args);
    }

    private static void CallProducer(Properties properties) {
        Logger logger = LoggerFactory.getLogger(KafkaApplication.class);
        //Creating Producer
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        for (int i = 0; i < 10; i++){
            ProducerRecord<String, String> record = new ProducerRecord<>("first_spring_topic", "Hello Vaibhav " + i);
            //Sending data
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        logger.info(metadata.timestamp() + " Received new Metadata " +
                                " Topic: " + metadata.topic()  +
                                " Partition " + metadata.partition() +
                                " Has Offset " + metadata.hasOffset() +
                                " Offset " + metadata.offset());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            });
        }
        //flush data
        kafkaProducer.flush();
        kafkaProducer.close();
    }


    private static void CallProducerWithKey(Properties properties) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(KafkaApplication.class);
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        for (int i = 0; i < 10; i++){
            String topic = "first_spring_topic";
            String value = "Hello Vaibhav " + i;
            String key = "id_"+ i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key, value);
            //Sending data
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        logger.info(metadata.timestamp() + " Received new Metadata " +
                                " Topic: " + metadata.topic()  +
                                " Partition " + metadata.partition() +
                                " Has Offset " + metadata.hasOffset() +
                                " Offset " + metadata.offset());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get();
        }
        //flush data
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
