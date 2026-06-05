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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

public class BackupReferenceResolverTest {

  private static final String ADDRESS_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Address\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"street\",\"type\":\"string\"},"
      + "{\"name\":\"city\",\"type\":\"string\"}"
      + "]}";

  private static final String COUNTRY_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Country\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"code\",\"type\":\"string\"},"
      + "{\"name\":\"name\",\"type\":\"string\"}"
      + "]}";

  private SchemaRegistryClient targetRegistry;
  private BackupReferenceResolver resolver;

  private final BackupReferenceResolver.ParsedSchemaFactory avroFactory =
      (raw, refs, resolved) -> !refs.isEmpty()
          ? new AvroSchema(raw, refs, resolved, null)
          : new AvroSchema(raw);

  @Before
  public void setUp() {
    targetRegistry = new MockSchemaRegistryClient();
    resolver = new BackupReferenceResolver(targetRegistry);
  }

  @Test
  public void registerSingleRef_registersAndRemapsVersion()
      throws Exception {
    SchemaReference directRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new HashMap<>();
    tree.put("com.example.Address", new BackupSchemaFetcher.RefTreeEntry(
        "address-value", 1, ADDRESS_SCHEMA,
        Collections.emptyList(), 42, "AVRO"));
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.singletonList(directRef), tree, avroFactory,
        remapped, resolved);
    assertEquals(1, remapped.size());
    SchemaReference remappedRef = remapped.get(0);
    assertEquals("com.example.Address", remappedRef.getName());
    assertEquals("address-value", remappedRef.getSubject());
    assertTrue(remappedRef.getVersion() > 0);
    assertEquals(1, resolved.size());
    assertEquals(ADDRESS_SCHEMA, resolved.get("com.example.Address"));
    int targetVersion = targetRegistry.getVersion(
        "address-value", new AvroSchema(ADDRESS_SCHEMA));
    assertEquals((int) remappedRef.getVersion(), targetVersion);
  }

  @Test
  public void registerNestedRefs_registersDepthFirst()
      throws Exception {
    SchemaReference countryRef = new SchemaReference(
        "com.example.Country", "country-value", 1);
    SchemaReference addressRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new HashMap<>();
    tree.put("com.example.Country",
        new BackupSchemaFetcher.RefTreeEntry(
            "country-value", 1, COUNTRY_SCHEMA,
            Collections.emptyList(), 10, "AVRO"));
    tree.put("com.example.Address",
        new BackupSchemaFetcher.RefTreeEntry(
            "address-value", 1, ADDRESS_SCHEMA,
            Collections.singletonList(countryRef), 20, "AVRO"));
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.singletonList(addressRef), tree, avroFactory,
        remapped, resolved);
    assertEquals(1, remapped.size());
    assertEquals("com.example.Address", remapped.get(0).getName());
    int countryVersion = targetRegistry.getVersion(
        "country-value", new AvroSchema(COUNTRY_SCHEMA));
    assertTrue(countryVersion > 0);
    int addressVersion = targetRegistry.getVersion(
        "address-value",
        new AvroSchema(ADDRESS_SCHEMA,
            Collections.singletonList(new SchemaReference(
                "com.example.Country", "country-value",
                countryVersion)),
            Collections.singletonMap(
                "com.example.Country", COUNTRY_SCHEMA), null));
    assertTrue(addressVersion > 0);
    assertEquals(addressVersion, (int) remapped.get(0).getVersion());
  }

  @Test
  public void registerRefs_cached_skipsReRegistration()
      throws Exception {
    SchemaReference directRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new HashMap<>();
    tree.put("com.example.Address", new BackupSchemaFetcher.RefTreeEntry(
        "address-value", 1, ADDRESS_SCHEMA,
        Collections.emptyList(), 42, "AVRO"));
    List<SchemaReference> remapped1 = new ArrayList<>();
    Map<String, String> resolved1 = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.singletonList(directRef), tree, avroFactory,
        remapped1, resolved1);
    List<SchemaReference> remapped2 = new ArrayList<>();
    Map<String, String> resolved2 = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.singletonList(directRef), tree, avroFactory,
        remapped2, resolved2);
    assertEquals(
        (int) remapped1.get(0).getVersion(),
        (int) remapped2.get(0).getVersion());
    assertEquals(resolved1, resolved2);
  }

  @Test
  public void registerRefs_missingRefInTree_throwsDataException() {
    SchemaReference directRef = new SchemaReference(
        "com.example.Missing", "missing-value", 1);
    try {
      resolver.registerRefsRecursive(
          Collections.singletonList(directRef), new HashMap<>(),
          avroFactory, new ArrayList<>(), new HashMap<>());
      fail("Expected DataException");
    } catch (DataException e) {
      assertTrue(e.getMessage().contains("not found in reference tree"));
      assertTrue(e.getMessage().contains("com.example.Missing"));
    }
  }

  @Test
  public void registerRefs_nullRefs_noOp() {
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        null, new HashMap<>(), avroFactory, remapped, resolved);
    assertTrue(remapped.isEmpty());
    assertTrue(resolved.isEmpty());
  }

  @Test
  public void registerRefs_emptyRefs_noOp() {
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.emptyList(), new HashMap<>(), avroFactory,
        remapped, resolved);
    assertTrue(remapped.isEmpty());
    assertTrue(resolved.isEmpty());
  }

  @Test
  public void parseReferenceTree_validJson_correctEntries() {
    String json = "{\"com.example.Address\":{"
        + "\"subject\":\"address-value\","
        + "\"version\":1,"
        + "\"schema\":" + quote(ADDRESS_SCHEMA) + ","
        + "\"references\":[],"
        + "\"globalId\":42,"
        + "\"schemaType\":\"AVRO\"}}";
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree =
        BackupReferenceResolver.parseReferenceTree(json);
    assertEquals(1, tree.size());
    BackupSchemaFetcher.RefTreeEntry entry =
        tree.get("com.example.Address");
    assertNotNull(entry);
    assertEquals("address-value", entry.getSubject());
    assertEquals(1L, (long) entry.getVersion());
    assertEquals(42L, (long) entry.getGlobalId());
    assertEquals("AVRO", entry.getSchemaType());
    assertNotNull(entry.getSchema());
  }

  @Test
  public void parseReferenceTree_withNestedRefs_correctChildRefs() {
    String json = "{"
        + "\"com.example.Country\":{"
        + "\"subject\":\"country-value\","
        + "\"version\":1,"
        + "\"schema\":" + quote(COUNTRY_SCHEMA) + ","
        + "\"references\":[],"
        + "\"globalId\":10,"
        + "\"schemaType\":\"AVRO\"},"
        + "\"com.example.Address\":{"
        + "\"subject\":\"address-value\","
        + "\"version\":1,"
        + "\"schema\":" + quote(ADDRESS_SCHEMA) + ","
        + "\"references\":["
        + "{\"name\":\"com.example.Country\","
        + "\"subject\":\"country-value\","
        + "\"version\":1}],"
        + "\"globalId\":20,"
        + "\"schemaType\":\"AVRO\"}}";
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree =
        BackupReferenceResolver.parseReferenceTree(json);
    assertEquals(2, tree.size());
    BackupSchemaFetcher.RefTreeEntry address =
        tree.get("com.example.Address");
    assertEquals(1, address.getReferences().size());
    assertEquals("com.example.Country",
        address.getReferences().get(0).getName());
    assertEquals("country-value",
        address.getReferences().get(0).getSubject());
    assertEquals(1L,
        (long) address.getReferences().get(0).getVersion());
  }

  @Test
  public void parseReferenceTree_nullJson_returnsEmpty() {
    assertTrue(BackupReferenceResolver
        .parseReferenceTree(null).isEmpty());
  }

  @Test
  public void parseReferenceTree_emptyJson_returnsEmpty() {
    assertTrue(BackupReferenceResolver
        .parseReferenceTree("").isEmpty());
  }

  @Test
  public void parseDirectRefs_validJson_correctRefs() {
    String json = "[{\"name\":\"com.example.Address\","
        + "\"subject\":\"address-value\","
        + "\"version\":1}]";
    List<SchemaReference> refs =
        BackupReferenceResolver.parseDirectRefs(json);
    assertEquals(1, refs.size());
    assertEquals("com.example.Address", refs.get(0).getName());
    assertEquals("address-value", refs.get(0).getSubject());
    assertEquals(1L, (long) refs.get(0).getVersion());
  }

  @Test
  public void parseDirectRefs_multipleRefs() {
    String json = "["
        + "{\"name\":\"com.example.Address\","
        + "\"subject\":\"address-value\",\"version\":1},"
        + "{\"name\":\"com.example.Country\","
        + "\"subject\":\"country-value\",\"version\":2}"
        + "]";
    List<SchemaReference> refs =
        BackupReferenceResolver.parseDirectRefs(json);
    assertEquals(2, refs.size());
    assertEquals(1L, (long) refs.get(0).getVersion());
    assertEquals(2L, (long) refs.get(1).getVersion());
  }

  @Test
  public void parseDirectRefs_nullJson_returnsEmpty() {
    assertTrue(BackupReferenceResolver
        .parseDirectRefs(null).isEmpty());
  }

  @Test
  public void parseDirectRefs_emptyJson_returnsEmpty() {
    assertTrue(BackupReferenceResolver
        .parseDirectRefs("").isEmpty());
  }

  @Test
  public void parseReferenceTree_invalidJson_throwsDataException() {
    try {
      BackupReferenceResolver.parseReferenceTree("{invalid json}");
      fail("Expected DataException");
    } catch (DataException e) {
      assertTrue(e.getMessage().contains("Cannot parse"));
    }
  }

  @Test
  public void parseDirectRefs_invalidJson_throwsDataException() {
    try {
      BackupReferenceResolver.parseDirectRefs("{not a list}");
      fail("Expected DataException");
    } catch (DataException e) {
      assertTrue(e.getMessage().contains("Cannot parse"));
    }
  }

  @Test
  public void endToEnd_backupAndRestore_versionsRemapped()
      throws Exception {
    SchemaRegistryClient sourceRegistry =
        new MockSchemaRegistryClient();
    AvroSchema addressSchema = new AvroSchema(ADDRESS_SCHEMA);
    sourceRegistry.register("address-value", addressSchema);
    SchemaReference addressRef = new SchemaReference(
        "com.example.Address", "address-value", 1);
    String userSchemaText =
        "{\"type\":\"record\",\"name\":\"User\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"address\",\"type\":\"com.example.Address\"}"
        + "]}";
    AvroSchema mainSchema = new AvroSchema(userSchemaText,
        Collections.singletonList(addressRef),
        Collections.singletonMap(
            "com.example.Address", ADDRESS_SCHEMA), null);
    int srcMainId = sourceRegistry.register(
        "user-value", mainSchema);
    BackupSchemaFetcher fetcher =
        new BackupSchemaFetcher(sourceRegistry);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(srcMainId);
    assertEquals(Integer.valueOf(1),
        info.getVersionForSubject("user-value"));
    assertNotNull(info.getReferenceTreeJson());
    assertNotNull(info.getDirectRefsJson());
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree =
        BackupReferenceResolver.parseReferenceTree(
            info.getReferenceTreeJson());
    List<SchemaReference> directRefs =
        BackupReferenceResolver.parseDirectRefs(
            info.getDirectRefsJson());
    assertEquals(1, tree.size());
    assertTrue(tree.containsKey("com.example.Address"));
    assertEquals(1, directRefs.size());
    assertEquals("com.example.Address",
        directRefs.get(0).getName());
    assertEquals(1L, (long) directRefs.get(0).getVersion());
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        directRefs, tree, avroFactory, remapped, resolved);
    assertEquals(1, remapped.size());
    SchemaReference targetRef = remapped.get(0);
    assertEquals("com.example.Address", targetRef.getName());
    assertEquals("address-value", targetRef.getSubject());
    assertTrue(targetRef.getVersion() > 0);
    int targetAddressVersion = targetRegistry.getVersion(
        "address-value", new AvroSchema(ADDRESS_SCHEMA));
    assertEquals((int) targetRef.getVersion(), targetAddressVersion);
    AvroSchema targetMainSchema = new AvroSchema(
        userSchemaText, remapped, resolved, null);
    targetRegistry.register("user-value", targetMainSchema);
    int targetMainVersion = targetRegistry.getVersion(
        "user-value", targetMainSchema);
    assertTrue(targetMainVersion > 0);
  }

  private static String quote(String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
