package com.epam.learning.topic_1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    public static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {

            int partitionReadFrom = 5;
            int offsetReadFrom = 1;
            String topicName = "first_topic";
            TopicPartition topicPartition = new TopicPartition(topicName, partitionReadFrom);
            consumer.assign(Collections.singletonList(topicPartition));
            consumer.seek(topicPartition, offsetReadFrom);

            int numberOfRecordsToRead = 4;
            int currentNumberOfRecords = 0;
            boolean keepOnReading = true;

            while (keepOnReading) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    currentNumberOfRecords++;

                    for (Header header : record.headers()) {
                        logger.info("header {} - {}", header.key(), header.value());
                    }
                    logger.info("key - {}", record.key());
                    logger.info("offset - {}", record.offset());
                    logger.info("partition - {}", record.partition());
                    logger.info("topic - {}", record.topic());
                    logger.info("value - {}\n", record.value());
                    if (currentNumberOfRecords >= numberOfRecordsToRead) {
                        keepOnReading = false;
                        break;
                    }
                }
            }
        }
    }
}
