/*
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import org.junit.Test;

import java.util.Iterator;

import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaKeySerializer;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SchemaRegistryKeysTest {

  @Test
  public void testSchemaKeySerde() {
    String subject = "foo";
    int version = 1;
    SchemaKey key = new SchemaKey(subject, version);
    SchemaKeySerializer keySerializer = new SchemaKeySerializer();
    byte[] serializedKey = null;
    try {
      serializedKey = keySerializer.toBytes(key);
    } catch (SerializationException e) {
      fail();
    }
    assertNotNull(serializedKey);
    try {
      SchemaRegistryKey deserializedKey = keySerializer.fromBytes(serializedKey);
      assertEquals("Deserialized key should be equal to original key", key, deserializedKey);
    } catch (SerializationException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSchemaKeyComparator() {
    String subject = "foo";
    SchemaRegistryKey key1 = new SchemaKey(subject, KafkaSchemaRegistry.MIN_VERSION);
    SchemaRegistryKey key2 = new SchemaKey(subject, KafkaSchemaRegistry.MAX_VERSION);
    assertTrue("key 1 should be less than key2", key1.compareTo(key2) < 0);
    SchemaRegistryKey key1Dup = new SchemaKey(subject, KafkaSchemaRegistry.MIN_VERSION);
    assertEquals("key 1 should be equal to key1Dup", key1, key1Dup);
    String subject4 = "bar";
    SchemaRegistryKey key4 = new SchemaKey(subject4, KafkaSchemaRegistry.MIN_VERSION);
    assertTrue("key1 should be greater than key4", key1.compareTo(key4) > 0);
    String subject5 = "fo";
    SchemaRegistryKey key5 = new SchemaKey(subject5, KafkaSchemaRegistry.MIN_VERSION);
    // compare key1 and key5
    assertTrue("key5 should be less than key1", key1.compareTo(key5) > 0);
    SchemaRegistryKey[] expectedOrder = {key4, key5, key1, key2};
    testStoreKeyOrder(expectedOrder);
  }

  @Test
  public void testConfigKeySerde() {
    String subject = "foo";
    ConfigKey key1 = new ConfigKey(null);
    ConfigKey key2 = new ConfigKey(subject);
    SchemaKeySerializer keySerializer = new SchemaKeySerializer();
    byte[] serializedKey1 = null;
    byte[] serializedKey2 = null;
    try {
      serializedKey1 = keySerializer.toBytes(key1);
      serializedKey2 = keySerializer.toBytes(key2);
    } catch (SerializationException e) {
      fail();
    }
    try {
      SchemaRegistryKey deserializedKey1 = keySerializer.fromBytes(serializedKey1);
      SchemaRegistryKey deserializedKey2 = keySerializer.fromBytes(serializedKey2);
      assertEquals("Deserialized key should be equal to original key", key1, deserializedKey1);
      assertEquals("Deserialized key should be equal to original key", key2, deserializedKey2);
    } catch (SerializationException e) {
      fail();
    }
  }

  @Test
  public void testConfigKeyComparator() {
    ConfigKey key1 = new ConfigKey(null);
    ConfigKey key2 = new ConfigKey(null);
    assertEquals("Top level config keys should be equal", key1, key2);
    String subject = "foo";
    ConfigKey key3 = new ConfigKey(subject);
    assertTrue("Top level config should be less than subject level config",
               key1.compareTo(key3) < 0);
    String subject4 = "bar";
    ConfigKey key4 = new ConfigKey(subject4);
    assertTrue("key3 should be greater than key4", key3.compareTo(key4) > 0);
    SchemaRegistryKey[] expectedOrder = {key1, key4, key3};
    testStoreKeyOrder(expectedOrder);
  }

  @Test
  public void testKeyComparator() {
    String subject = "foo";
    ConfigKey topLevelConfigKey = new ConfigKey(null);
    ConfigKey subjectLevelConfigKey = new ConfigKey(subject);
    SchemaKey schemaKey = new SchemaKey(subject, 1);
    SchemaKey schemaKeyWithHigherVersion = new SchemaKey(subject, 2);
    SchemaRegistryKey[]
        expectedOrder =
        {topLevelConfigKey, subjectLevelConfigKey, schemaKey, schemaKeyWithHigherVersion};
    testStoreKeyOrder(expectedOrder);
  }

  private void testStoreKeyOrder(SchemaRegistryKey[] orderedKeys) {
    int numKeys = orderedKeys.length;
    InMemoryStore<SchemaRegistryKey, String> store = new InMemoryStore<SchemaRegistryKey, String>();
    while (--numKeys >= 0) {
      try {
        store.put(orderedKeys[numKeys], orderedKeys[numKeys].toString());
      } catch (StoreException e) {
        fail("Error writing key " + orderedKeys[numKeys].toString() + " to the in memory store");
      }
    }
    // test key order
    try {
      Iterator<SchemaRegistryKey> keys = store.getAllKeys();
      SchemaRegistryKey[] retrievedKeyOrder = new SchemaRegistryKey[orderedKeys.length];
      int keyIndex = 0;
      while (keys.hasNext()) {
        retrievedKeyOrder[keyIndex++] = keys.next();
      }
      assertArrayEquals(orderedKeys, retrievedKeyOrder);
    } catch (StoreException e) {
      fail();
    }
  }
}
