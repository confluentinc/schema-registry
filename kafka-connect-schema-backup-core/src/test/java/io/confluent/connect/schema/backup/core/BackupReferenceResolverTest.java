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

package io.confluent.connect.schema.backup.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

public class BackupReferenceResolverTest {

  private static final String ADDRESS_VALUE_SUBJECT = "address-value";
  private static final String ADDRESS_FQCN = "com.example.Address";
  private static final String COUNTRY_FQCN = "com.example.Country";
  private static final String COUNTRY_VALUE_SUBJECT = "country-value";
  private static final String EXPECTED_DATA_EXCEPTION = "Expected DataException";
  private static final String REGION_FQCN = "com.example.Region";
  private static final String REGION_VALUE_SUBJECT = "region-value";
  private static final String USER_VALUE_SUBJECT = "user-value";
  private static final String SCHEMA_TYPE_AVRO = "AVRO";
  private static final String TEST_DATA = "test-data";

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
  public void testRegisterSingleRef()
      throws Exception {
    SchemaReference directRef = new SchemaReference(
        ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1);
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new HashMap<>();
    tree.put(ADDRESS_FQCN, new BackupSchemaFetcher.RefTreeEntry(
        ADDRESS_VALUE_SUBJECT, 1, ADDRESS_SCHEMA,
        Collections.emptyList(), 42, SCHEMA_TYPE_AVRO));
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.singletonList(directRef), tree, avroFactory,
        remapped, resolved);
    assertEquals(1, remapped.size());
    SchemaReference remappedRef = remapped.get(0);
    assertEquals(ADDRESS_FQCN, remappedRef.getName());
    assertEquals(ADDRESS_VALUE_SUBJECT, remappedRef.getSubject());
    assertTrue(remappedRef.getVersion() > 0);
    assertEquals(1, resolved.size());
    assertEquals(ADDRESS_SCHEMA, resolved.get(ADDRESS_FQCN));
    int targetVersion = targetRegistry.getVersion(
        ADDRESS_VALUE_SUBJECT, new AvroSchema(ADDRESS_SCHEMA));
    assertEquals((int) remappedRef.getVersion(), targetVersion);
  }

  @Test
  public void testRegisterNestedRefs()
      throws Exception {
    SchemaReference countryRef = new SchemaReference(
        COUNTRY_FQCN, COUNTRY_VALUE_SUBJECT, 1);
    SchemaReference addressRef = new SchemaReference(
        ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1);
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new HashMap<>();
    tree.put(COUNTRY_FQCN,
        new BackupSchemaFetcher.RefTreeEntry(
            COUNTRY_VALUE_SUBJECT, 1, COUNTRY_SCHEMA,
            Collections.emptyList(), 10, SCHEMA_TYPE_AVRO));
    tree.put(ADDRESS_FQCN,
        new BackupSchemaFetcher.RefTreeEntry(
            ADDRESS_VALUE_SUBJECT, 1, ADDRESS_SCHEMA,
            Collections.singletonList(countryRef), 20, SCHEMA_TYPE_AVRO));
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.singletonList(addressRef), tree, avroFactory,
        remapped, resolved);
    assertEquals(1, remapped.size());
    assertEquals(ADDRESS_FQCN, remapped.get(0).getName());
    int countryVersion = targetRegistry.getVersion(
        COUNTRY_VALUE_SUBJECT, new AvroSchema(COUNTRY_SCHEMA));
    assertTrue(countryVersion > 0);
    int addressVersion = targetRegistry.getVersion(
        ADDRESS_VALUE_SUBJECT,
        new AvroSchema(ADDRESS_SCHEMA,
            Collections.singletonList(new SchemaReference(
                COUNTRY_FQCN, COUNTRY_VALUE_SUBJECT,
                countryVersion)),
            Collections.singletonMap(
                COUNTRY_FQCN, COUNTRY_SCHEMA), null));
    assertTrue(addressVersion > 0);
    assertEquals(addressVersion, (int) remapped.get(0).getVersion());
  }

  @Test
  public void testRegisterRefsCached()
      throws Exception {
    SchemaReference directRef = new SchemaReference(
        ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1);
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new HashMap<>();
    tree.put(ADDRESS_FQCN, new BackupSchemaFetcher.RefTreeEntry(
        ADDRESS_VALUE_SUBJECT, 1, ADDRESS_SCHEMA,
        Collections.emptyList(), 42, SCHEMA_TYPE_AVRO));
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
  public void testRegisterRefsMissingInTree() {
    SchemaReference directRef = new SchemaReference(
        "com.example.Missing", "missing-value", 1);
    try {
      resolver.registerRefsRecursive(
          Collections.singletonList(directRef), new HashMap<>(),
          avroFactory, new ArrayList<>(), new HashMap<>());
      fail(EXPECTED_DATA_EXCEPTION);
    } catch (DataException e) {
      assertTrue(e.getMessage().contains("not found in reference tree"));
      assertTrue(e.getMessage().contains("com.example.Missing"));
    }
  }

  @Test
  public void testRegisterRefsNull() {
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        null, new HashMap<>(), avroFactory, remapped, resolved);
    assertTrue(remapped.isEmpty());
    assertTrue(resolved.isEmpty());
  }

  @Test
  public void testRegisterRefsEmpty() {
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.emptyList(), new HashMap<>(), avroFactory,
        remapped, resolved);
    assertTrue(remapped.isEmpty());
    assertTrue(resolved.isEmpty());
  }

  @Test
  public void testParseReferenceTree() {
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
        tree.get(ADDRESS_FQCN);
    assertNotNull(entry);
    assertEquals(ADDRESS_VALUE_SUBJECT, entry.getSubject());
    assertEquals(1L, (long) entry.getVersion());
    assertEquals(42L, (long) entry.getGlobalId());
    assertEquals(SCHEMA_TYPE_AVRO, entry.getSchemaType());
    assertNotNull(entry.getSchema());
  }

  @Test
  public void testParseReferenceTreeNested() {
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
        tree.get(ADDRESS_FQCN);
    assertEquals(1, address.getReferences().size());
    assertEquals(COUNTRY_FQCN,
        address.getReferences().get(0).getName());
    assertEquals(COUNTRY_VALUE_SUBJECT,
        address.getReferences().get(0).getSubject());
    assertEquals(1L,
        (long) address.getReferences().get(0).getVersion());
  }

  @Test
  public void testParseReferenceTreeNull() {
    assertTrue(BackupReferenceResolver
        .parseReferenceTree(null).isEmpty());
  }

  @Test
  public void testParseReferenceTreeEmpty() {
    assertTrue(BackupReferenceResolver
        .parseReferenceTree("").isEmpty());
  }

  @Test
  public void testParseDirectRefs() {
    String json = "[{\"name\":\"com.example.Address\","
        + "\"subject\":\"address-value\","
        + "\"version\":1}]";
    List<SchemaReference> refs =
        BackupReferenceResolver.parseDirectRefs(json);
    assertEquals(1, refs.size());
    assertEquals(ADDRESS_FQCN, refs.get(0).getName());
    assertEquals(ADDRESS_VALUE_SUBJECT, refs.get(0).getSubject());
    assertEquals(1L, (long) refs.get(0).getVersion());
  }

  @Test
  public void testParseDirectRefsMultiple() {
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
  public void testParseDirectRefsNull() {
    assertTrue(BackupReferenceResolver
        .parseDirectRefs(null).isEmpty());
  }

  @Test
  public void parseDirectRefs_emptyJson_returnsEmpty() {
    assertTrue(BackupReferenceResolver
        .parseDirectRefs("").isEmpty());
  }

  @Test
  public void testParseReferenceTreeInvalidJson() {
    try {
      BackupReferenceResolver.parseReferenceTree("{invalid json}");
      fail(EXPECTED_DATA_EXCEPTION);
    } catch (DataException e) {
      assertTrue(e.getMessage().contains("Cannot parse"));
    }
  }

  @Test
  public void testParseDirectRefsInvalidJson() {
    try {
      BackupReferenceResolver.parseDirectRefs("{not a list}");
      fail(EXPECTED_DATA_EXCEPTION);
    } catch (DataException e) {
      assertTrue(e.getMessage().contains("Cannot parse"));
    }
  }

  @Test
  public void testEndToEndVersionRemapping()
      throws Exception {
    SchemaRegistryClient sourceRegistry =
        new MockSchemaRegistryClient();
    AvroSchema addressSchema = new AvroSchema(ADDRESS_SCHEMA);
    sourceRegistry.register(ADDRESS_VALUE_SUBJECT, addressSchema);
    SchemaReference addressRef = new SchemaReference(
        ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1);
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
            ADDRESS_FQCN, ADDRESS_SCHEMA), null);
    int srcMainId = sourceRegistry.register(
        USER_VALUE_SUBJECT, mainSchema);
    BackupSchemaFetcher fetcher =
        new BackupSchemaFetcher(sourceRegistry);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(srcMainId);
    assertEquals(Integer.valueOf(1),
        info.getVersionForSubject(USER_VALUE_SUBJECT));
    assertNotNull(info.getReferenceTreeJson());
    assertNotNull(info.getDirectRefsJson());
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree =
        BackupReferenceResolver.parseReferenceTree(
            info.getReferenceTreeJson());
    List<SchemaReference> directRefs =
        BackupReferenceResolver.parseDirectRefs(
            info.getDirectRefsJson());
    assertEquals(1, tree.size());
    assertTrue(tree.containsKey(ADDRESS_FQCN));
    assertEquals(1, directRefs.size());
    assertEquals(ADDRESS_FQCN,
        directRefs.get(0).getName());
    assertEquals(1L, (long) directRefs.get(0).getVersion());
    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        directRefs, tree, avroFactory, remapped, resolved);
    assertEquals(1, remapped.size());
    SchemaReference targetRef = remapped.get(0);
    assertEquals(ADDRESS_FQCN, targetRef.getName());
    assertEquals(ADDRESS_VALUE_SUBJECT, targetRef.getSubject());
    assertTrue(targetRef.getVersion() > 0);
    int targetAddressVersion = targetRegistry.getVersion(
        ADDRESS_VALUE_SUBJECT, new AvroSchema(ADDRESS_SCHEMA));
    assertEquals((int) targetRef.getVersion(), targetAddressVersion);
    AvroSchema targetMainSchema = new AvroSchema(
        userSchemaText, remapped, resolved, null);
    targetRegistry.register(USER_VALUE_SUBJECT, targetMainSchema);
    int targetMainVersion = targetRegistry.getVersion(
        USER_VALUE_SUBJECT, targetMainSchema);
    assertTrue(targetMainVersion > 0);
  }

  @Test
  public void testResolveFromWrapperNoRefs() {
    Schema dataSchema = Schema.STRING_SCHEMA;
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        1, 1, SCHEMA_TYPE_AVRO,"test-value", ADDRESS_SCHEMA, null, null);
    Struct wrapper = BackupWrapper.buildWrapper(
        wrapperSchema, TEST_DATA, fields);

    BackupReferenceResolver.ResolutionResult result =
        resolver.resolveFromWrapper(wrapperSchema, wrapper, avroFactory);

    assertFalse(result.hasReferences());
    assertTrue(result.getTargetRefs().isEmpty());
    assertTrue(result.getResolvedSchemas().isEmpty());
  }

  @Test
  public void testResolveFromWrapperWithRefs()
      throws Exception {
    String treeJson = "{\"" + ADDRESS_FQCN + "\":{"
        + "\"subject\":\"" + ADDRESS_VALUE_SUBJECT + "\","
        + "\"version\":1,"
        + "\"schema\":" + quote(ADDRESS_SCHEMA) + ","
        + "\"references\":[],"
        + "\"globalId\":42,"
        + "\"schemaType\":\"AVRO\"}}";
    String directRefsJson = "[{\"name\":\"" + ADDRESS_FQCN + "\","
        + "\"subject\":\"" + ADDRESS_VALUE_SUBJECT + "\","
        + "\"version\":1}]";

    Schema dataSchema = Schema.STRING_SCHEMA;
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        1, 1, SCHEMA_TYPE_AVRO,USER_VALUE_SUBJECT, ADDRESS_SCHEMA,
        treeJson, directRefsJson);
    Struct wrapper = BackupWrapper.buildWrapper(
        wrapperSchema, TEST_DATA, fields);

    BackupReferenceResolver.ResolutionResult result =
        resolver.resolveFromWrapper(wrapperSchema, wrapper, avroFactory);

    assertTrue(result.hasReferences());
    assertEquals(1, result.getTargetRefs().size());
    assertEquals(ADDRESS_FQCN, result.getTargetRefs().get(0).getName());
    assertEquals(ADDRESS_VALUE_SUBJECT,
        result.getTargetRefs().get(0).getSubject());
    assertTrue(result.getTargetRefs().get(0).getVersion() > 0);
    assertFalse(result.getResolvedSchemas().isEmpty());
    assertEquals(ADDRESS_SCHEMA,
        result.getResolvedSchemas().get(ADDRESS_FQCN));
  }

  @Test
  public void testResolveFromWrapperTreeWithoutDirectRefs() {
    String treeJson = "{\"" + ADDRESS_FQCN + "\":{"
        + "\"subject\":\"" + ADDRESS_VALUE_SUBJECT + "\","
        + "\"version\":1,"
        + "\"schema\":" + quote(ADDRESS_SCHEMA) + ","
        + "\"references\":[],"
        + "\"globalId\":42,"
        + "\"schemaType\":\"AVRO\"}}";

    Schema dataSchema = Schema.STRING_SCHEMA;
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        1, 1, SCHEMA_TYPE_AVRO,USER_VALUE_SUBJECT, ADDRESS_SCHEMA,
        treeJson, null);
    Struct wrapper = BackupWrapper.buildWrapper(
        wrapperSchema, TEST_DATA, fields);

    BackupReferenceResolver.ResolutionResult result =
        resolver.resolveFromWrapper(wrapperSchema, wrapper, avroFactory);

    assertFalse(result.hasReferences());
  }

  @Test
  public void testResolutionResultEmpty() {
    BackupReferenceResolver.ResolutionResult empty =
        BackupReferenceResolver.ResolutionResult.empty();
    assertFalse(empty.hasReferences());
    assertTrue(empty.getTargetRefs().isEmpty());
    assertTrue(empty.getResolvedSchemas().isEmpty());
  }

  @Test
  public void testTransitiveSchemasThreeLevels()
      throws Exception {
    String regionSchema =
        "{\"type\":\"record\",\"name\":\"Region\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":["
        + "{\"name\":\"id\",\"type\":\"string\"}"
        + "]}";

    SchemaReference regionRef = new SchemaReference(
        REGION_FQCN, REGION_VALUE_SUBJECT, 1);
    SchemaReference countryRef = new SchemaReference(
        COUNTRY_FQCN, COUNTRY_VALUE_SUBJECT, 1);
    SchemaReference addressRef = new SchemaReference(
        ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1);

    Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new HashMap<>();
    tree.put(REGION_FQCN,
        new BackupSchemaFetcher.RefTreeEntry(
            REGION_VALUE_SUBJECT, 1, regionSchema,
            Collections.emptyList(), 5, SCHEMA_TYPE_AVRO));
    tree.put(COUNTRY_FQCN,
        new BackupSchemaFetcher.RefTreeEntry(
            COUNTRY_VALUE_SUBJECT, 1, COUNTRY_SCHEMA,
            Collections.singletonList(regionRef), 10, SCHEMA_TYPE_AVRO));
    tree.put(ADDRESS_FQCN,
        new BackupSchemaFetcher.RefTreeEntry(
            ADDRESS_VALUE_SUBJECT, 1, ADDRESS_SCHEMA,
            Collections.singletonList(countryRef), 20, SCHEMA_TYPE_AVRO));

    List<SchemaReference> remapped = new ArrayList<>();
    Map<String, String> resolved = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.singletonList(addressRef), tree, avroFactory,
        remapped, resolved);

    assertEquals(1, remapped.size());
    assertEquals(ADDRESS_FQCN, remapped.get(0).getName());
    // All three schemas should be resolved transitively
    assertEquals(3, resolved.size());
    assertTrue(resolved.containsKey(ADDRESS_FQCN));
    assertTrue(resolved.containsKey(COUNTRY_FQCN));
    assertTrue(resolved.containsKey(REGION_FQCN));

    // Verify all registered in target
    assertTrue(targetRegistry.getVersion(
        REGION_VALUE_SUBJECT, new AvroSchema(regionSchema)) > 0);
    assertTrue(targetRegistry.getVersion(
        COUNTRY_VALUE_SUBJECT, new AvroSchema(COUNTRY_SCHEMA,
            Collections.singletonList(new SchemaReference(
                REGION_FQCN, REGION_VALUE_SUBJECT,
                targetRegistry.getVersion(
                    REGION_VALUE_SUBJECT,
                    new AvroSchema(regionSchema)))),
            Collections.singletonMap(
                REGION_FQCN, regionSchema), null)) > 0);
  }

  @Test
  public void testRegisterRefsCachedTransitive()
      throws Exception {
    // First registration populates cache
    SchemaReference addressRef = new SchemaReference(
        ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1);
    SchemaReference countryRef = new SchemaReference(
        COUNTRY_FQCN, COUNTRY_VALUE_SUBJECT, 1);
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree = new HashMap<>();
    tree.put(COUNTRY_FQCN,
        new BackupSchemaFetcher.RefTreeEntry(
            COUNTRY_VALUE_SUBJECT, 1, COUNTRY_SCHEMA,
            Collections.emptyList(), 10, SCHEMA_TYPE_AVRO));
    tree.put(ADDRESS_FQCN,
        new BackupSchemaFetcher.RefTreeEntry(
            ADDRESS_VALUE_SUBJECT, 1, ADDRESS_SCHEMA,
            Collections.singletonList(countryRef), 20, SCHEMA_TYPE_AVRO));

    List<SchemaReference> remapped1 = new ArrayList<>();
    Map<String, String> resolved1 = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.singletonList(addressRef), tree, avroFactory,
        remapped1, resolved1);

    // Second call uses cache path (addTransitiveSchemas)
    List<SchemaReference> remapped2 = new ArrayList<>();
    Map<String, String> resolved2 = new HashMap<>();
    resolver.registerRefsRecursive(
        Collections.singletonList(addressRef), tree, avroFactory,
        remapped2, resolved2);

    // Transitive schemas should still be resolved via cache path
    assertEquals(2, resolved2.size());
    assertTrue(resolved2.containsKey(ADDRESS_FQCN));
    assertTrue(resolved2.containsKey(COUNTRY_FQCN));
  }

  private static String quote(String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
