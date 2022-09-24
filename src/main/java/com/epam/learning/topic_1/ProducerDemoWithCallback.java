package com.epam.learning.topic_1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws InterruptedException {

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {

            String message = "hello from java";
            String topic = "first_topic";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    // producer successfully sent the message
                    logger.info("Received new metadata.\n Topic : {}\n Offset : {}\n Partition : {}\n Timestamp : {}\n SerializedKeySize : {}\n SerializedValueSize : {}",
                            metadata.topic(), metadata.offset(), metadata.partition(), metadata.timestamp(), metadata.serializedKeySize(), metadata.serializedValueSize());
                } else {
                    // exception occurred while sending message
                    logger.error("Error while sending message", exception);
                }
            });
        }
    }
}