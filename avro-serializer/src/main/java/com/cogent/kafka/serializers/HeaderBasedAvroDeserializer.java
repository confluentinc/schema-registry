package com.cogent.kafka.serializers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Avro deserializer that uses message headers to determine subject names
 * for dynamic schema routing in Cogent's data pipeline.
 */
public class HeaderBasedAvroDeserializer extends KafkaAvroDeserializer {

    private static final String SUBJECT_HEADER_NAME = "cogent_extraction_avro_subject_name";
    private static final String DLQ_SUBJECT_NAME = "__cogent_extraction_avro_subject_name_unavailable__";
    
    // Thread-local storage for headers, similar to how the base class handles keys
    private static final ThreadLocal<Headers> currentHeaders = new ThreadLocal<>();

    public HeaderBasedAvroDeserializer() {
        super();
    }

    public HeaderBasedAvroDeserializer(SchemaRegistryClient client) {
        super.schemaRegistry = client;
        super.ticker = ticker(client);
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        super.configure(props, isKey);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        // Store headers in thread-local for access in getSubjectName
        currentHeaders.set(headers);
        try {
            return super.deserialize(topic, headers, data);
        } finally {
            // Clean up thread-local
            currentHeaders.remove();
        }
    }

    @Override
    protected String getSubjectName(String topic, boolean isKey, Object value, ParsedSchema schema) {
        // Only check headers for value deserialization (not keys)
        if (!isKey) {
            Headers headers = currentHeaders.get();
            if (headers != null) {
                Header subjectHeader = headers.lastHeader(SUBJECT_HEADER_NAME);
                if (subjectHeader != null) {
                    String customSubject = new String(subjectHeader.value(), StandardCharsets.UTF_8).trim();
                    if (!customSubject.isEmpty()) {
                        return customSubject;
                    }
                }
            }
            
            // If header is not present or empty, return DLQ subject name to cause DLQ
            return DLQ_SUBJECT_NAME;
        }

        // For keys, use the default subject name strategy
        return super.getSubjectName(topic, isKey, value, schema);
    }
}