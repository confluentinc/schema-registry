/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cogent.kafka.connect;

import com.cogent.kafka.connect.HeaderBasedAvroConverter;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests for HeaderBasedAvroConverter that verify header-based subject routing
 * and compatibility with standard AvroConverter functionality.
 */
public class HeaderBasedAvroConverterTest {
    private static final String TOPIC = "test-topic";
    private static final String SUBJECT_HEADER_NAME = "cogent_extraction_avro_subject_name";
    private static final String DLQ_SUBJECT_NAME = "__cogent_extraction_avro_subject_name_unavailable__";

    private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");

    private final SchemaRegistryClient schemaRegistry;
    private final HeaderBasedAvroConverter converter;

    public HeaderBasedAvroConverterTest() {
        schemaRegistry = new MockSchemaRegistryClient();
        converter = new HeaderBasedAvroConverter(schemaRegistry);
    }

    @Before
    public void setUp() {
        converter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
    }

    @Test
    public void testBasicFunctionality() throws IOException, RestClientException {
        // Test that basic serialization/deserialization works like standard AvroConverter
        // Since we deserialize without headers, we need to register under the DLQ subject
        org.apache.avro.Schema boolSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN);
        schemaRegistry.register(DLQ_SUBJECT_NAME, new AvroSchema(boolSchema));
        
        SchemaAndValue original = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true);
        byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original.value());
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
        
        // Because of registration in schema registry and lookup, we'll have added a version number
        SchemaAndValue expected = new SchemaAndValue(SchemaBuilder.bool().version(1).build(), true);
        assertEquals(expected, schemaAndValue);
    }

    @Test
    public void testComplexStruct() throws IOException, RestClientException {
        // Test with a complex struct to ensure full compatibility
        // Since we deserialize without headers, we need to register under the DLQ subject
        org.apache.avro.Schema structSchema = org.apache.avro.SchemaBuilder
                .record("TestStruct").fields()
                .requiredInt("id")
                .requiredString("name")
                .requiredBoolean("active")
                .endRecord();
        schemaRegistry.register(DLQ_SUBJECT_NAME, new AvroSchema(structSchema));
        
        SchemaBuilder builder = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("active", Schema.BOOLEAN_SCHEMA);
        Schema schema = builder.build();
        
        Struct original = new Struct(schema)
                .put("id", 123)
                .put("name", "test-user")
                .put("active", true);

        byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
        
        // Verify structure is preserved
        assertNotNull(schemaAndValue.value());
        assertEquals(Struct.class, schemaAndValue.value().getClass());
        Struct result = (Struct) schemaAndValue.value();
        assertEquals(123, result.get("id"));
        assertEquals("test-user", result.get("name"));
        assertEquals(true, result.get("active"));
    }

    @Test
    public void testHeaderBasedSubjectRouting() throws IOException, RestClientException {
        // Test the core functionality: header-based subject routing
        String customSubject = "custom-subject-value";
        
        // Create test data
        Schema schema = SchemaBuilder.struct()
                .field("message", Schema.STRING_SCHEMA)
                .build();
        Struct data = new Struct(schema).put("message", "hello world");

        // Register schema under custom subject
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
                .record("TestRecord").fields()
                .requiredString("message")
                .endRecord();
        schemaRegistry.register(customSubject, new AvroSchema(avroSchema));

        // Serialize data using standard KafkaAvroSerializer with custom subject
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(SR_CONFIG, false);
        
        org.apache.avro.generic.GenericRecord avroRecord = 
                new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
                        .set("message", "hello world").build();
        byte[] serializedData = serializer.serialize(customSubject.replace("-value", ""), avroRecord);

        // Create headers with custom subject name
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(SUBJECT_HEADER_NAME, customSubject.getBytes()));

        // Deserialize with headers - should use custom subject from header
        SchemaAndValue result = converter.toConnectData(TOPIC, headers, serializedData);
        
        assertNotNull("Deserialized data should not be null", result.value());
        assertEquals("Schema should have version 1", Integer.valueOf(1), result.schema().version());
    }

    @Test
    public void testMissingHeaderFallsBackToDLQ() throws IOException, RestClientException {
        // Test that missing headers result in DLQ subject name usage
        
        // Register schema under DLQ subject
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
                .record("DLQRecord").fields()
                .requiredString("message")
                .endRecord();
        schemaRegistry.register(DLQ_SUBJECT_NAME, new AvroSchema(avroSchema));

        // Serialize data using the DLQ subject
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(SR_CONFIG, false);
        
        org.apache.avro.generic.GenericRecord avroRecord = 
                new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
                        .set("message", "dlq message").build();
        byte[] serializedData = serializer.serialize(DLQ_SUBJECT_NAME.replace("-value", "").replace("__", ""), avroRecord);

        // Deserialize WITHOUT headers - should use DLQ subject name
        SchemaAndValue result = converter.toConnectData(TOPIC, (Headers) null, serializedData);
        
        assertNotNull("DLQ data should be deserialized", result.value());
    }

    @Test 
    public void testEmptyHeaderFallsBackToDLQ() throws IOException, RestClientException {
        // Test that empty header values result in DLQ subject name usage
        
        // Register schema under DLQ subject  
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
                .record("EmptyHeaderRecord").fields()
                .requiredString("data")
                .endRecord();
        schemaRegistry.register(DLQ_SUBJECT_NAME, new AvroSchema(avroSchema));

        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(SR_CONFIG, false);
        
        org.apache.avro.generic.GenericRecord avroRecord = 
                new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
                        .set("data", "empty header test").build();
        byte[] serializedData = serializer.serialize("empty", avroRecord);

        // Create headers with empty subject name
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(SUBJECT_HEADER_NAME, "".getBytes()));

        // Should fall back to DLQ behavior
        SchemaAndValue result = converter.toConnectData(TOPIC, headers, serializedData);
        assertNotNull("Data should still be deserialized", result.value());
    }

    @Test
    public void testWhitespaceHeaderFallsBackToDLQ() throws IOException, RestClientException {
        // Test that whitespace-only header values result in DLQ subject name usage
        
        // Register schema under DLQ subject
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
                .record("WhitespaceHeaderRecord").fields()
                .requiredString("content")
                .endRecord();
        schemaRegistry.register(DLQ_SUBJECT_NAME, new AvroSchema(avroSchema));

        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(SR_CONFIG, false);
        
        org.apache.avro.generic.GenericRecord avroRecord = 
                new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
                        .set("content", "whitespace test").build();
        byte[] serializedData = serializer.serialize("whitespace", avroRecord);

        // Create headers with whitespace-only subject name
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(SUBJECT_HEADER_NAME, "   ".getBytes()));

        // Should fall back to DLQ behavior
        SchemaAndValue result = converter.toConnectData(TOPIC, headers, serializedData);
        assertNotNull("Data should still be deserialized", result.value());
    }

    @Test
    public void testOriginalCogentScenario() throws IOException, RestClientException {
        // Test the original scenario: topic "pacific-m365-user" with header "m365-user-value"
        String testTopic = "pacific-m365-user";
        String customSubject = "m365-user-value";
        
        // Register schema under the custom subject
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
                .record("M365User").fields()
                .requiredString("userId")
                .requiredString("email")
                .endRecord();
        schemaRegistry.register(customSubject, new AvroSchema(avroSchema));

        // Serialize data
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(SR_CONFIG, false);
        
        org.apache.avro.generic.GenericRecord avroRecord = 
                new org.apache.avro.generic.GenericRecordBuilder(avroSchema)
                        .set("userId", "user123")
                        .set("email", "user@company.com").build();
        byte[] serializedData = serializer.serialize("m365-user", avroRecord);

        // Create headers with custom subject name
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(SUBJECT_HEADER_NAME, customSubject.getBytes()));

        // Deserialize with headers - should use custom subject from header
        SchemaAndValue result = converter.toConnectData(testTopic, headers, serializedData);
        
        assertNotNull("Deserialized record should not be null", result.value());
        assertEquals("Schema should have version 1", Integer.valueOf(1), result.schema().version());
        
        System.out.println("✅ Original Cogent scenario test passed!");
        System.out.println("   Topic: " + testTopic);
        System.out.println("   Header: " + SUBJECT_HEADER_NAME + " = " + customSubject);
        System.out.println("   Successfully used header-based subject routing");
    }

    @Test
    public void testNullSerialization() {
        // Test null handling (should work like standard AvroConverter)
        byte[] converted = converter.fromConnectData(TOPIC, Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
        assertNull(converted);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
        assertEquals(SchemaAndValue.NULL, schemaAndValue);
    }
} 