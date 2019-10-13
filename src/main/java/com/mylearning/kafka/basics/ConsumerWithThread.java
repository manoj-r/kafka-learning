package com.mylearning.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {

    public static void main(String[] args) {
        new ConsumerWithThread().startConsumer();
    }

    // public ConsumerWithThread() {}

    private void startConsumer() {
        Logger LOGGER = LoggerFactory.getLogger(ConsumerWithThread.class);
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-second-application";
        CountDownLatch latch = new CountDownLatch(1);
        String topic = "first_topic";
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers,groupId, topic, latch);

        Thread consumerThread = new Thread(myConsumerRunnable);
        consumerThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            LOGGER.info("In shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("Application has ended");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.info("Program interrupted " , e);
        } finally {
            LOGGER.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private Logger LOGGER = LoggerFactory.getLogger(ConsumerRunnable.class);
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(properties);

            //subscribe to topic
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    //poll data from topic
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("Key {}, value {}", record.key(), record.value());
                        LOGGER.info("Partition {}, offset {}", record.partition(), record.offset());
                    }
                }
            } catch (WakeupException e) {
                LOGGER.info("Received shutdown signal");
            } finally {
                consumer.close();
                // inform the main code that program has exited
                latch.countDown();
            }
        }

        public void shutdown() {
            //wakeup will interrupt consumer.poll() by throwing wakeup exception
            consumer.wakeup();
        }
    }
}

