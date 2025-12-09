package com.example.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Custom deserializer for Zeek logs that handles unknown fields and provides better error handling.
 */
public class ZeekLogDeserializer implements DeserializationSchema<JsonNode> {
    private static final Logger LOGGER = Logger.getLogger(ZeekLogDeserializer.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        // Configure ObjectMapper to ignore unknown fields
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.configure(DeserializationFeature.ACCEPT_FLOAT_AS_INT, true);
        MAPPER.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        
        // Add any custom modules if needed
        SimpleModule module = new SimpleModule();
        MAPPER.registerModule(module);
    }

    @Override
    public JsonNode deserialize(byte[] message) throws IOException {
        try {
            return MAPPER.readTree(message);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to deserialize message: " + new String(message), e);
            return null;
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