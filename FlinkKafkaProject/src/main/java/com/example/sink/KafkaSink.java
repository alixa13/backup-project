package com.example.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.example.APIConfig;

import java.util.Properties;
import java.util.logging.Logger;

/**
 * Kafka producer utility for sending messages to Kafka topics.
 */
public class KafkaSink {
    private static final Logger LOGGER = Logger.getLogger(KafkaSink.class.getName());
    private static KafkaProducer<String, String> producer;

    static {
        initializeProducer();
    }

    /**
     * Initialize the Kafka producer with configuration from APIConfig.
     */
    private static void initializeProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, APIConfig.getKafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        producer = new KafkaProducer<>(props);
        LOGGER.info("Kafka producer initialized with bootstrap servers: " + APIConfig.getKafkaBootstrapServers());
    }

    /**
     * Write a message to a Kafka topic.
     * 
     * @param topic The Kafka topic to write to
     * @param message The message to send
     */
    public static void writeToKafka(String topic, String message) {
        if (producer == null) {
            LOGGER.severe("Kafka producer is not initialized");
            return;
        }

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.severe("Error sending message to Kafka: " + exception.getMessage());
                } else {
                    LOGGER.info("Message sent to Kafka topic " + metadata.topic() + 
                              " partition " + metadata.partition() + 
                              " offset " + metadata.offset());
                }
            });
        } catch (Exception e) {
            LOGGER.severe("Failed to write to Kafka: " + e.getMessage());
        }
    }

    /**
     * Close the Kafka producer and flush any pending messages.
     */
    public static void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            LOGGER.info("Kafka producer closed");
        }
    }
} 