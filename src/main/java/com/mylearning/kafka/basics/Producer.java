package com.mylearning.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        Logger LOGGER = LoggerFactory.getLogger(Producer.class);

        String bootstrapServers = "127.0.0.1:9092";

        //create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        String topic = "first_topic";

        //send data
        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            String value = "value_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                    if (null == exception) {
                        LOGGER.info("Metadata \n topic: {} \n Partition: {} \n offset: {} \n Timestamp: {}",
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    } else {
                        LOGGER.error("Error while producing");
                    }
                }
            );
        }
        //close producer
        //producer.flush();
        producer.close();
    }
}
