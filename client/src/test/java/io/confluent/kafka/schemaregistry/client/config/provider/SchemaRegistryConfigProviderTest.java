/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.config.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SchemaRegistryConfigProviderTest {

    private SchemaRegistryConfigProvider provider;

    @Before
    public void setup() throws Exception {
        provider = new SchemaRegistryConfigProvider();
        Map<String, String> configs = new HashMap<>();
        configs.put("schema.registry.url", "mock://config");
        provider.configure(configs);

        SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("config");
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");
        properties.put("key3", "value3");
        Metadata m = new Metadata(Collections.emptyMap(), properties, null);
        AvroSchema schema = avroSchema(0);
        schema = schema.copy(m, null);
        schemaRegistryClient.register("config", schema);

        properties = new HashMap<>();
        properties.put("contextKey1", "contextValue1");
        properties.put("contextKey2", "contextValue2");
        properties.put("contextKey3", "contextValue3");
        m = new Metadata(Collections.emptyMap(), properties, null);
        schema = avroSchema(1);
        schema = schema.copy(m, null);
        schemaRegistryClient.register(":.mycontext:config2", schema);
    }

    @After
    public void close() throws IOException {
        provider.close();
    }

    @Test
    public void testGetAllKeysAtPath() {
        ConfigData configData = provider.get("subjects/config/versions/1");
        Set<String> keys = new HashSet<>();
        keys.add("key1");
        keys.add("key2");
        keys.add("key3");
        assertEquals(keys, configData.data().keySet());
        assertEquals("value1", configData.data().get("key1"));
        assertEquals("value2", configData.data().get("key2"));
        assertEquals("value3", configData.data().get("key3"));
    }

    @Test
    public void testGetSetOfKeysAtPath() {
        Set<String> keys = new HashSet<>();
        keys.add("key1");
        ConfigData configData = provider.get("subjects/config/versions/1", keys);
        assertEquals(keys, configData.data().keySet());
        assertEquals("value1", configData.data().get("key1"));
    }

    @Test
    public void testGetAllKeysAtLatest() {
        ConfigData configData = provider.get("subjects/config/versions/latest");
        Set<String> keys = new HashSet<>();
        keys.add("key1");
        keys.add("key2");
        keys.add("key3");
        assertEquals(keys, configData.data().keySet());
        assertEquals("value1", configData.data().get("key1"));
        assertEquals("value2", configData.data().get("key2"));
        assertEquals("value3", configData.data().get("key3"));
    }
    @Test
    public void testGetAllKeysAtContextPath() {
        ConfigData configData = provider.get("contexts/.mycontext/subjects/config2/versions/1");
        Set<String> keys = new HashSet<>();
        keys.add("contextKey1");
        keys.add("contextKey2");
        keys.add("contextKey3");
        assertEquals(keys, configData.data().keySet());
        assertEquals("contextValue1", configData.data().get("contextKey1"));
        assertEquals("contextValue2", configData.data().get("contextKey2"));
        assertEquals("contextValue3", configData.data().get("contextKey3"));
    }

    @Test
    public void testGetSetOfKeysAtContextPath() {
        Set<String> keys = new HashSet<>();
        keys.add("contextKey1");
        ConfigData configData = provider.get("contexts/.mycontext/subjects/config2/versions/1", keys);
        assertEquals(keys, configData.data().keySet());
        assertEquals("contextValue1", configData.data().get("contextKey1"));
    }

    @Test
    public void testGetAllKeysAtContextLatest() {
        ConfigData configData = provider.get("contexts/.mycontext/subjects/config2/versions/latest");
        Set<String> keys = new HashSet<>();
        keys.add("contextKey1");
        keys.add("contextKey2");
        keys.add("contextKey3");
        assertEquals(keys, configData.data().keySet());
        assertEquals("contextValue1", configData.data().get("contextKey1"));
        assertEquals("contextValue2", configData.data().get("contextKey2"));
        assertEquals("contextValue3", configData.data().get("contextKey3"));
    }

    @Test
    public void testEmptyPath() {
        ConfigData configData = provider.get("");
        assertTrue(configData.data().isEmpty());
    }

    @Test
    public void testEmptyPathWithKey() {
        ConfigData configData = provider.get("", Collections.singleton("key1"));
        assertTrue(configData.data().isEmpty());
    }

    @Test
    public void testNullPath() {
        ConfigData configData = provider.get(null);
        assertTrue(configData.data().isEmpty());
    }

    @Test
    public void testNullPathWithKey() {
        ConfigData configData = provider.get(null, Collections.singleton("key1"));
        assertTrue(configData.data().isEmpty());
    }

    @Test
    public void testServiceLoaderDiscovery() {
        ServiceLoader<ConfigProvider> serviceLoader = ServiceLoader.load(ConfigProvider.class);
        assertTrue(StreamSupport.stream(serviceLoader.spliterator(), false).anyMatch(
            configProvider -> configProvider instanceof SchemaRegistryConfigProvider));
    }

    private static AvroSchema avroSchema(final int i) {
        return new AvroSchema(avroSchemaString(i));
    }

    private static String avroSchemaString(final int i) {
        return "{\"type\": \"record\", \"name\": \"Blah" + i + "\", "
            + "\"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
    }
}

