/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.schema.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class BackupSchemaFetcherTest {

  private static final String ADDRESS_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Address\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"street\",\"type\":\"string\"},"
      + "{\"name\":\"city\",\"type\":\"string\"}"
      + "]}";

  private static final String USER_SCHEMA =
      "{\"type\":\"record\",\"name\":\"User\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"name\",\"type\":\"string\"},"
      + "{\"name\":\"age\",\"type\":\"int\"}"
      + "]}";

  private static final String COUNTRY_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Country\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"code\",\"type\":\"string\"},"
      + "{\"name\":\"name\",\"type\":\"string\"}"
      + "]}";

  private SchemaRegistryClient schemaRegistry;
  private BackupSchemaFetcher fetcher;

  @Before
  public void setUp() {
    schemaRegistry = new MockSchemaRegistryClient();
    fetcher = new BackupSchemaFetcher(schemaRegistry);
  }

  @Test
  public void fetchSchemaInfo_returnsRawSchemaAndVersion()
      throws Exception {
    AvroSchema avro = new AvroSchema(USER_SCHEMA);
    int schemaId = schemaRegistry.register("orders-value", avro);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(schemaId);
    assertNotNull(info);
    assertNotNull(info.getRawSchema());
    assertEquals(Integer.valueOf(1),
        info.getVersionForSubject("orders-value"));
  }

  @Test
  public void fetchSchemaInfo_multipleSubjects_correctVersionPerSubject()
      throws Exception {
    AvroSchema avro = new AvroSchema(USER_SCHEMA);
    int id1 = schemaRegistry.register("topic-a-value", avro);
    schemaRegistry.register("topic-b-value", avro);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(id1);
    assertEquals(Integer.valueOf(1),
        info.getVersionForSubject("topic-a-value"));
    assertEquals(Integer.valueOf(1),
        info.getVersionForSubject("topic-b-value"));
    assertNull(info.getVersionForSubject("nonexistent-subject"));
  }

  @Test
  public void fetchSchemaInfo_multipleVersionsUnderSubject()
      throws Exception {
    schemaRegistry.register("users-value", new AvroSchema(USER_SCHEMA));
    int id2 = schemaRegistry.register("users-value",
        new AvroSchema(ADDRESS_SCHEMA));
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(id2);
    assertEquals(Integer.valueOf(2),
        info.getVersionForSubject("users-value"));
  }

  @Test
  public void fetchSchemaInfo_withDirectReference_buildsReferenceTree()
      throws Exception {
    AvroSchema addressSchema = new AvroSchema(ADDRESS_SCHEMA);
    int addressId = schemaRegistry.register(
        "address-value", addressSchema);
    SchemaReference addressRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    String userWithRefSchema =
        "{\"type\":\"record\",\"name\":\"User\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"address\",\"type\":\"com.example.Address\"}"
        + "]}";
    AvroSchema mainSchema = new AvroSchema(userWithRefSchema,
        Collections.singletonList(addressRef),
        Collections.singletonMap(
            "com.example.Address", ADDRESS_SCHEMA), null);
    int mainId = schemaRegistry.register("user-value", mainSchema);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(mainId);
    assertNotNull(info.getReferenceTreeJson());
    assertNotNull(info.getDirectRefsJson());
    assertEquals(1, info.getDirectReferences().size());
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree =
        info.getReferenceTree();
    assertTrue(tree.containsKey("com.example.Address"));
    BackupSchemaFetcher.RefTreeEntry entry =
        tree.get("com.example.Address");
    assertEquals("address-value", entry.getSubject());
    assertEquals(1, entry.getVersion());
    assertEquals(addressId, entry.getGlobalId());
    assertNotNull(entry.getSchema());
  }

  @Test
  public void fetchSchemaInfo_withNestedReferences_buildsFullTree()
      throws Exception {
    AvroSchema countrySchema = new AvroSchema(COUNTRY_SCHEMA);
    int countryId = schemaRegistry.register(
        "country-value", countrySchema);
    SchemaReference countryRef = new SchemaReference(
        "com.example.Country", "country-value", 1);
    String addressWithCountry =
        "{\"type\":\"record\",\"name\":\"Address\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"street\",\"type\":\"string\"},"
        + "{\"name\":\"country\",\"type\":\"com.example.Country\"}"
        + "]}";
    AvroSchema addressSchema = new AvroSchema(addressWithCountry,
        Collections.singletonList(countryRef),
        Collections.singletonMap(
            "com.example.Country", COUNTRY_SCHEMA), null);
    int addressId = schemaRegistry.register(
        "address-value", addressSchema);
    SchemaReference addressRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    String userSchema =
        "{\"type\":\"record\",\"name\":\"User\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"address\",\"type\":\"com.example.Address\"}"
        + "]}";
    Map<String, String> userResolved = new HashMap<>();
    userResolved.put("com.example.Address", addressWithCountry);
    userResolved.put("com.example.Country", COUNTRY_SCHEMA);
    AvroSchema mainSchema = new AvroSchema(userSchema,
        Collections.singletonList(addressRef),
        userResolved, null);
    int mainId = schemaRegistry.register("user-value", mainSchema);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(mainId);
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree =
        info.getReferenceTree();
    assertEquals(2, tree.size());
    assertTrue(tree.containsKey("com.example.Address"));
    assertTrue(tree.containsKey("com.example.Country"));
    assertEquals("address-value",
        tree.get("com.example.Address").getSubject());
    assertEquals(addressId,
        tree.get("com.example.Address").getGlobalId());
    assertEquals("country-value",
        tree.get("com.example.Country").getSubject());
    assertEquals(countryId,
        tree.get("com.example.Country").getGlobalId());
  }

  @Test
  public void fetchSchemaInfo_cacheReturnsSameInstance()
      throws Exception {
    AvroSchema avro = new AvroSchema(USER_SCHEMA);
    int schemaId = schemaRegistry.register("test-value", avro);
    BackupSchemaFetcher.BackupSchemaInfo first =
        fetcher.fetchSchemaInfo(schemaId);
    BackupSchemaFetcher.BackupSchemaInfo second =
        fetcher.fetchSchemaInfo(schemaId);
    assertSame(first, second);
  }

  @Test
  public void fetchSchemaInfo_noReferences_emptyTreeAndRefs()
      throws Exception {
    AvroSchema avro = new AvroSchema(USER_SCHEMA);
    int schemaId = schemaRegistry.register("test-value", avro);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(schemaId);
    assertTrue(info.getReferenceTree().isEmpty());
    assertTrue(info.getDirectReferences().isEmpty());
    assertNull(info.getReferenceTreeJson());
    assertNull(info.getDirectRefsJson());
  }

  @Test
  public void fetchSchemaInfo_eachIdMapsToCorrectVersion()
      throws Exception {
    AvroSchema schemaV1 = new AvroSchema(USER_SCHEMA);
    AvroSchema schemaV2 = new AvroSchema(ADDRESS_SCHEMA);
    AvroSchema schemaV3 = new AvroSchema(COUNTRY_SCHEMA);
    String subject = "evolving-value";
    int id1 = schemaRegistry.register(subject, schemaV1);
    int id2 = schemaRegistry.register(subject, schemaV2);
    int id3 = schemaRegistry.register(subject, schemaV3);
    assertTrue("IDs must be unique", id1 != id2 && id2 != id3);
    BackupSchemaFetcher.BackupSchemaInfo info1 =
        fetcher.fetchSchemaInfo(id1);
    BackupSchemaFetcher.BackupSchemaInfo info2 =
        fetcher.fetchSchemaInfo(id2);
    BackupSchemaFetcher.BackupSchemaInfo info3 =
        fetcher.fetchSchemaInfo(id3);
    assertEquals("ID " + id1 + " must be version 1",
        Integer.valueOf(1), info1.getVersionForSubject(subject));
    assertEquals("ID " + id2 + " must be version 2",
        Integer.valueOf(2), info2.getVersionForSubject(subject));
    assertEquals("ID " + id3 + " must be version 3",
        Integer.valueOf(3), info3.getVersionForSubject(subject));
    assertEquals(schemaV1.canonicalString(), info1.getRawSchema());
    assertEquals(schemaV2.canonicalString(), info2.getRawSchema());
    assertEquals(schemaV3.canonicalString(), info3.getRawSchema());
  }
}
