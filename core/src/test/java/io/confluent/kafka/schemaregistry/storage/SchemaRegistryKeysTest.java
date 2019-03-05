/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import org.junit.Test;

import java.util.Iterator;

import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchemaRegistryKeysTest {

  @Test
  public void testSchemaKeySerdeForMagicByte0() {
    String subject = "foo";
    int version = 1;
    SchemaKey key = new SchemaKey(subject, version);
    key.setMagicByte(0);
    Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();
    byte[] serializedKey = null;
    try {
      serializedKey = serializer.serializeKey(key);
    } catch (SerializationException e) {
      fail();
    }
    assertNotNull(serializedKey);
    try {
      SchemaRegistryKey deserializedKey = serializer.deserializeKey(serializedKey);
      assertEquals("Deserialized key should be equal to original key", key, deserializedKey);
    } catch (SerializationException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSchemaKeySerdeForMagicByte1() {
    String subject = "foo";
    int version = 1;
    SchemaKey key = new SchemaKey(subject, version);
    Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();
    byte[] serializedKey = null;
    try {
      serializedKey = serializer.serializeKey(key);
    } catch (SerializationException e) {
      fail();
    }
    assertNotNull(serializedKey);
    try {
      SchemaRegistryKey deserializedKey = serializer.deserializeKey(serializedKey);
      assertEquals("Deserialized key should be equal to original key", key, deserializedKey);
    } catch (SerializationException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSchemaKeySerdeForUnSupportedMagicByte() {
    String subject = "foo";
    int version = 1;
    SchemaKey key = new SchemaKey(subject, version);
    key.setMagicByte(2);
    Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();
    byte[] serializedKey = null;
    try {
      serializedKey = serializer.serializeKey(key);
    } catch (SerializationException e) {
      fail();
    }
    assertNotNull(serializedKey);
    try {
      serializer.deserializeKey(serializedKey);
      fail("Deserialization shouldn't be supported");
    } catch (SerializationException e) {
      assertEquals("Can't deserialize schema for the magic byte 2", e.getMessage());
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
    Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();
    byte[] serializedKey1 = null;
    byte[] serializedKey2 = null;
    try {
      serializedKey1 = serializer.serializeKey(key1);
      serializedKey2 = serializer.serializeKey(key2);
    } catch (SerializationException e) {
      fail();
    }
    try {
      SchemaRegistryKey deserializedKey1 = serializer.deserializeKey(serializedKey1);
      SchemaRegistryKey deserializedKey2 = serializer.deserializeKey(serializedKey2);
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
    InMemoryCache<SchemaRegistryKey, String> store = new InMemoryCache<SchemaRegistryKey, String>();
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
