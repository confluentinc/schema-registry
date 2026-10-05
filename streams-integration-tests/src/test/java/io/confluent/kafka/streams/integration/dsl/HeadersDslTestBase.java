/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.streams.integration.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

abstract class HeadersDslTestBase extends ClusterTestHarness {

    protected HeadersDslTestBase() {
        super(1, true);
    }

    protected void createTopics(String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.stream(topicNames)
                .map(name -> new NewTopic(name, 1, (short) 1))
                .collect(Collectors.toList());
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        }
    }

    protected Properties createProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
            HeaderSchemaIdSerializer.class.getName());
        return props;
    }

    protected void assertSchemaIdHeaders(Headers headers, String topic, String context) {
        Header keyHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keyHeader, context + ": should have __key_schema_id header");
        assertHeaderGuidMatchesSubject(keyHeader.value(), topic + "-key", context + " key");

        Header valueHeader = headers.lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
        assertNotNull(valueHeader, context + ": should have __value_schema_id header");
        assertHeaderGuidMatchesSubject(valueHeader.value(), topic + "-value", context + " value");
    }

    protected void assertKeySchemaIdHeader(Headers headers, String topic, String context) {
        Header keyHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keyHeader, context + ": should have __key_schema_id header");
        assertHeaderGuidMatchesSubject(keyHeader.value(), topic + "-key", context + " key");
    }

    // Cross-checks the schema-id header bytes against Schema Registry: decodes the GUID from
    // the 17-byte V1 header and asserts it matches the latest registered GUID for the subject.
    protected void assertHeaderGuidMatchesSubject(byte[] headerBytes, String subject, String context) {
        assertEquals(17, headerBytes.length, context + ": GUID header should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, headerBytes[0],
            context + ": header should have V1 magic byte");

        ByteBuffer bb = ByteBuffer.wrap(headerBytes, 1, 16);
        UUID headerGuid = new UUID(bb.getLong(), bb.getLong());

        try {
            io.confluent.kafka.schemaregistry.client.rest.entities.Schema registered =
                restApp.restClient.getLatestVersion(subject);
            assertEquals(registered.getGuid(), headerGuid.toString(),
                context + ": header GUID does not match latest registered GUID for subject " + subject);
        } catch (Exception e) {
            fail(context + ": failed to look up subject " + subject + " in Schema Registry: "
                + e.getMessage());
        }
    }
}
