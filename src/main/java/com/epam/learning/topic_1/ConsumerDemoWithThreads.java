package com.epam.learning.topic_1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    public static void main(String[] args) throws InterruptedException {
        new ConsumerDemoWithThreads().run();
    }

    public void run() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(latch);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumerRunnable.shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info("Application exited!!!");

        }));
    }

    class ConsumerRunnable implements Runnable {

        private static final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
        private final KafkaConsumer<String, String> consumer;
        private final CountDownLatch latch;

        public ConsumerRunnable(CountDownLatch latch) {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_group_3");
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<>(consumerProperties);

            consumer.subscribe(Collections.singletonList("first_topic"));

            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                    for (ConsumerRecord<String, String> record : records) {
                        for (Header header : record.headers()) {
                            logger.info("header {} - {}", header.key(), header.value());
                        }
                        logger.info("key - {}", record.key());
                        logger.info("offset - {}", record.offset());
                        logger.info("partition - {}", record.partition());
                        logger.info("topic - {}", record.topic());
                        logger.info("value - {}\n", record.value());
                    }
                }
            } catch (WakeupException ex) {
                logger.info("Consumer is interrupted from polling");
            } finally {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown() {
            consumer.wakeup();
        }
    }
}
