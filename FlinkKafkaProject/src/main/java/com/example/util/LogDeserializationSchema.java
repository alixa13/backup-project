package com.example.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserialization schema for JSON log messages from Kafka.
 * Converts byte arrays to JsonNode objects for processing.
 */
public class LogDeserializationSchema implements DeserializationSchema<JsonNode> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LogDeserializationSchema.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JsonNode deserialize(byte[] message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            LOG.debug("Successfully deserialized message: {}", jsonNode.toString());
            return jsonNode;
        } catch (IOException e) {
            LOG.error("Failed to deserialize message: {}", new String(message), e);
            return null; // Return null to drop the message
        }
    }

    @Override
    public boolean isEndOfStream(JsonNode nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return TypeInformation.of(JsonNode.class);
    }
} 