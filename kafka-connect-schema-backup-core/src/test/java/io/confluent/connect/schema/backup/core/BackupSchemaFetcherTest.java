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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class BackupSchemaFetcherTest {

  private static final String USERS_VALUE_SUBJECT = "users-value";
  private static final String USER_VALUE_SUBJECT = "user-value";
  private static final String ADDRESS_VALUE_SUBJECT = "address-value";
  private static final String COUNTRY_VALUE_SUBJECT = "country-value";
  private static final String ADDRESS_FQCN = "com.example.Address";
  private static final String COUNTRY_FQCN = "com.example.Country";

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

  private static final String USER_WITH_REF_SCHEMA =
      "{\"type\":\"record\",\"name\":\"User\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"name\",\"type\":\"string\"},"
      + "{\"name\":\"address\",\"type\":\"com.example.Address\"}"
      + "]}";

  private static final String ADDRESS_WITH_COUNTRY_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Address\","
      + "\"namespace\":\"com.example\","
      + "\"fields\":["
      + "{\"name\":\"street\",\"type\":\"string\"},"
      + "{\"name\":\"country\",\"type\":\"com.example.Country\"}"
      + "]}";

  private SchemaRegistryClient schemaRegistry;
  private BackupSchemaFetcher fetcher;

  @Before
  public void setUp() {
    schemaRegistry = new MockSchemaRegistryClient();
    fetcher = new BackupSchemaFetcher(schemaRegistry);
  }

  @Test
  public void testFetchSchemaInfo()
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
  public void testFetchSchemaInfoMultipleSubjects()
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
  public void testFetchSchemaInfoMultipleVersions()
      throws Exception {
    schemaRegistry.register(USERS_VALUE_SUBJECT, new AvroSchema(USER_SCHEMA));
    int id2 = schemaRegistry.register(USERS_VALUE_SUBJECT,
        new AvroSchema(ADDRESS_SCHEMA));
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(id2);
    assertEquals(Integer.valueOf(2),
        info.getVersionForSubject(USERS_VALUE_SUBJECT));
  }

  @Test
  public void testFetchSchemaInfoWithReference()
      throws Exception {
    AvroSchema addressSchema = new AvroSchema(ADDRESS_SCHEMA);
    int addressId = schemaRegistry.register(
        ADDRESS_VALUE_SUBJECT, addressSchema);
    SchemaReference addressRef = new SchemaReference(
        ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1);
    AvroSchema mainSchema = new AvroSchema(USER_WITH_REF_SCHEMA,
        Collections.singletonList(addressRef),
        Collections.singletonMap(
            ADDRESS_FQCN, ADDRESS_SCHEMA), null);
    int mainId = schemaRegistry.register(USER_VALUE_SUBJECT, mainSchema);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(mainId);
    assertNotNull(info.getReferenceTreeJson());
    assertNotNull(info.getDirectRefsJson());
    assertEquals(1, info.getDirectReferences().size());
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree =
        info.getReferenceTree();
    assertTrue(tree.containsKey(ADDRESS_FQCN));
    BackupSchemaFetcher.RefTreeEntry entry =
        tree.get(ADDRESS_FQCN);
    assertEquals(ADDRESS_VALUE_SUBJECT, entry.getSubject());
    assertEquals(1, entry.getVersion());
    assertEquals(addressId, entry.getGlobalId());
    assertNotNull(entry.getSchema());
  }

  @Test
  public void testFetchSchemaInfoNestedReferences()
      throws Exception {
    AvroSchema countrySchema = new AvroSchema(COUNTRY_SCHEMA);
    int countryId = schemaRegistry.register(
        COUNTRY_VALUE_SUBJECT, countrySchema);
    SchemaReference countryRef = new SchemaReference(
        COUNTRY_FQCN, COUNTRY_VALUE_SUBJECT, 1);
    AvroSchema addressSchema = new AvroSchema(ADDRESS_WITH_COUNTRY_SCHEMA,
        Collections.singletonList(countryRef),
        Collections.singletonMap(
            COUNTRY_FQCN, COUNTRY_SCHEMA), null);
    int addressId = schemaRegistry.register(
        ADDRESS_VALUE_SUBJECT, addressSchema);
    SchemaReference addressRef = new SchemaReference(
        ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1);
    Map<String, String> userResolved = new HashMap<>();
    userResolved.put(ADDRESS_FQCN, ADDRESS_WITH_COUNTRY_SCHEMA);
    userResolved.put(COUNTRY_FQCN, COUNTRY_SCHEMA);
    AvroSchema mainSchema = new AvroSchema(USER_WITH_REF_SCHEMA,
        Collections.singletonList(addressRef),
        userResolved, null);
    int mainId = schemaRegistry.register(USER_VALUE_SUBJECT, mainSchema);
    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(mainId);
    Map<String, BackupSchemaFetcher.RefTreeEntry> tree =
        info.getReferenceTree();
    assertEquals(2, tree.size());
    assertTrue(tree.containsKey(ADDRESS_FQCN));
    assertTrue(tree.containsKey(COUNTRY_FQCN));
    assertEquals(ADDRESS_VALUE_SUBJECT,
        tree.get(ADDRESS_FQCN).getSubject());
    assertEquals(addressId,
        tree.get(ADDRESS_FQCN).getGlobalId());
    assertEquals(COUNTRY_VALUE_SUBJECT,
        tree.get(COUNTRY_FQCN).getSubject());
    assertEquals(countryId,
        tree.get(COUNTRY_FQCN).getGlobalId());
  }

  @Test
  public void testFetchSchemaInfoCache()
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
  public void testFetchSchemaInfoNoReferences()
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
  public void testFetchSchemaInfoSchemaType()
      throws Exception {
    AvroSchema countrySchema = new AvroSchema(COUNTRY_SCHEMA);
    schemaRegistry.register(COUNTRY_VALUE_SUBJECT, countrySchema);
    SchemaReference countryRef = new SchemaReference(
        COUNTRY_FQCN, COUNTRY_VALUE_SUBJECT, 1);
    AvroSchema addressSchema = new AvroSchema(ADDRESS_WITH_COUNTRY_SCHEMA,
        Collections.singletonList(countryRef),
        Collections.singletonMap(
            COUNTRY_FQCN, COUNTRY_SCHEMA), null);
    schemaRegistry.register(ADDRESS_VALUE_SUBJECT, addressSchema);
    SchemaReference addressRef = new SchemaReference(
        ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1);
    Map<String, String> userResolved = new HashMap<>();
    userResolved.put(ADDRESS_FQCN, ADDRESS_WITH_COUNTRY_SCHEMA);
    userResolved.put(COUNTRY_FQCN, COUNTRY_SCHEMA);
    AvroSchema mainSchema = new AvroSchema(USER_WITH_REF_SCHEMA,
        Collections.singletonList(addressRef), userResolved, null);
    int mainId = schemaRegistry.register(USER_VALUE_SUBJECT, mainSchema);

    BackupSchemaFetcher.BackupSchemaInfo info =
        fetcher.fetchSchemaInfo(mainId);

    Map<String, BackupSchemaFetcher.RefTreeEntry> tree =
        info.getReferenceTree();
    assertEquals(2, tree.size());
    // Verify schemaType is populated for entries
    BackupSchemaFetcher.RefTreeEntry countryEntry =
        tree.get(COUNTRY_FQCN);
    assertNotNull(countryEntry);
    assertNotNull(countryEntry.getSchemaType());
    // Verify direct refs json is parseable
    assertNotNull(info.getDirectRefsJson());
    assertNotNull(info.getReferenceTreeJson());
  }

  @Test
  public void testFetchSchemaInfoVersionMapping()
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

  @Test
  public void testGetAllVersionsByIdFailurePropagates() throws Exception {
    final AvroSchema avro = new AvroSchema(USER_SCHEMA);
    BackupSchemaFetcher failingFetcher = new BackupSchemaFetcher(
        new MockSchemaRegistryClient() {
          @Override
          public ParsedSchema getSchemaById(int id) {
            return avro;
          }

          @Override
          public Collection<SubjectVersion> getAllVersionsById(int id) {
            throw new UncheckedIOException(new IOException("SR unreachable"));
          }
        });
    try {
      failingFetcher.fetchSchemaInfo(1);
      fail("Expected exception to propagate instead of being swallowed as WARN");
    } catch (UncheckedIOException expected) {
      assertTrue(expected.getCause() instanceof IOException);
    }
  }

  // ---------------- schemaType ----------------

  private static final String SCHEMA_TYPE_SUBJECT = "typed-value";

  @Test
  public void testFetchSchemaInfoRecordsRegistrySchemaType() throws Exception {
    int schemaId = schemaRegistry.register(SCHEMA_TYPE_SUBJECT, new AvroSchema(USER_SCHEMA));
    assertEquals(AvroSchema.TYPE, fetcher.fetchSchemaInfo(schemaId).getSchemaType());
  }

  @Test
  public void testFetchSchemaInfoByGuidRecordsRegistrySchemaType() throws Exception {
    String guid = schemaRegistry.registerWithResponse(
        SCHEMA_TYPE_SUBJECT, new AvroSchema(USER_SCHEMA), false).getGuid();
    BackupSchemaFetcher.BackupSchemaInfo info = fetcher.fetchSchemaInfoByGuid(guid);
    assertEquals(AvroSchema.TYPE, info.getSchemaType());
    assertNull(info.getVersionForSubject(SCHEMA_TYPE_SUBJECT));
  }

  @Test
  public void testExistingBackupSchemaInfoConstructorLeavesSchemaTypeNull() {
    BackupSchemaFetcher.BackupSchemaInfo info = new BackupSchemaFetcher.BackupSchemaInfo(
        USER_SCHEMA, Collections.emptyList(), Collections.emptyMap(), null, null,
        Collections.emptyMap());
    assertNull(info.getSchemaType());
  }

  // ---------------- Parsed-schema memo and bounded caches ----------------

  private static final String EVICTION_SUBJECT_PREFIX = "evict-";
  private static final String PARSE_FAILURE = "parse failure";

  private static final BackupReferenceResolver.ParsedSchemaFactory AVRO_FACTORY =
      (raw, refs, resolved) -> !refs.isEmpty()
          ? new AvroSchema(raw, refs, resolved, null)
          : new AvroSchema(raw);

  /** Reference implementation of parse(), uncached. */
  private static ParsedSchema previousInlineParse(
      BackupSchemaFetcher.BackupSchemaInfo info,
      BackupReferenceResolver.ParsedSchemaFactory factory) {
    Map<String, String> resolved = new LinkedHashMap<>();
    if (!info.getDirectReferences().isEmpty()) {
      for (Map.Entry<String, BackupSchemaFetcher.RefTreeEntry> e
          : info.getReferenceTree().entrySet()) {
        resolved.put(e.getKey(), e.getValue().getSchema());
      }
    }
    return factory.create(info.getRawSchema(), info.getDirectReferences(), resolved);
  }

  private int registerUserWithAddressRef() throws Exception {
    schemaRegistry.register(ADDRESS_VALUE_SUBJECT, new AvroSchema(ADDRESS_SCHEMA));
    AvroSchema main = new AvroSchema(USER_WITH_REF_SCHEMA,
        Collections.singletonList(new SchemaReference(ADDRESS_FQCN, ADDRESS_VALUE_SUBJECT, 1)),
        Collections.singletonMap(ADDRESS_FQCN, ADDRESS_SCHEMA), null);
    return schemaRegistry.register(USER_VALUE_SUBJECT, main);
  }

  @Test
  public void testParseWithoutReferencesMatchesPreviousInlineParse() throws Exception {
    int id = schemaRegistry.register(USERS_VALUE_SUBJECT, new AvroSchema(USER_SCHEMA));
    BackupSchemaFetcher.BackupSchemaInfo info = fetcher.fetchSchemaInfo(id);
    ParsedSchema parsed = info.parse(AVRO_FACTORY);
    assertEquals(previousInlineParse(info, AVRO_FACTORY), parsed);
    assertEquals(schemaRegistry.getSchemaById(id).canonicalString(), parsed.canonicalString());
  }

  @Test
  public void testParseWithReferencesMatchesPreviousInlineParse() throws Exception {
    int id = registerUserWithAddressRef();
    BackupSchemaFetcher.BackupSchemaInfo info = fetcher.fetchSchemaInfo(id);
    ParsedSchema parsed = info.parse(AVRO_FACTORY);
    assertEquals(previousInlineParse(info, AVRO_FACTORY), parsed);
    assertEquals(info.getDirectReferences(), parsed.references());
    assertEquals(schemaRegistry.getSchemaById(id), parsed);
  }

  @Test
  public void testParseReturnsSameInstanceForSameFactory() throws Exception {
    int id = registerUserWithAddressRef();
    BackupSchemaFetcher.BackupSchemaInfo info = fetcher.fetchSchemaInfo(id);
    assertSame(info.parse(AVRO_FACTORY), info.parse(AVRO_FACTORY));
  }

  @Test
  public void testParseWithAnotherFactoryParsesAgain() throws Exception {
    int id = schemaRegistry.register(USERS_VALUE_SUBJECT, new AvroSchema(USER_SCHEMA));
    BackupSchemaFetcher.BackupSchemaInfo info = fetcher.fetchSchemaInfo(id);
    BackupReferenceResolver.ParsedSchemaFactory other =
        (raw, refs, resolved) -> new AvroSchema(raw);
    ParsedSchema first = info.parse(AVRO_FACTORY);
    ParsedSchema second = info.parse(other);
    assertNotSame(first, second);
    assertEquals(first, second);
  }

  @Test
  public void testParseFailureIsNotRemembered() throws Exception {
    int id = schemaRegistry.register(USERS_VALUE_SUBJECT, new AvroSchema(USER_SCHEMA));
    BackupSchemaFetcher.BackupSchemaInfo info = fetcher.fetchSchemaInfo(id);
    BackupReferenceResolver.ParsedSchemaFactory failing = (raw, refs, resolved) -> {
      throw new IllegalStateException(PARSE_FAILURE);
    };
    try {
      info.parse(failing);
      fail("expected the factory failure to propagate");
    } catch (IllegalStateException e) {
      assertEquals(PARSE_FAILURE, e.getMessage());
    }
    assertEquals(new AvroSchema(USER_SCHEMA), info.parse(AVRO_FACTORY));
  }

  @Test
  public void testEvictedSchemaInfoIsRefetchedWithSameContent() throws Exception {
    int firstId = schemaRegistry.register(USERS_VALUE_SUBJECT, new AvroSchema(USER_SCHEMA));
    BackupSchemaFetcher.BackupSchemaInfo first = fetcher.fetchSchemaInfo(firstId);
    for (int i = 0; i <= BackupCaches.MAX_ENTRIES; i++) {
      String schema = "{\"type\":\"record\",\"name\":\"R" + i + "\",\"fields\":[]}";
      int id = schemaRegistry.register(EVICTION_SUBJECT_PREFIX + i, new AvroSchema(schema));
      fetcher.fetchSchemaInfo(id);
    }
    BackupSchemaFetcher.BackupSchemaInfo again = fetcher.fetchSchemaInfo(firstId);
    assertEquals(first.getRawSchema(), again.getRawSchema());
    assertEquals(first.getVersionForSubject(USERS_VALUE_SUBJECT),
        again.getVersionForSubject(USERS_VALUE_SUBJECT));
    assertEquals(first.getSchemaType(), again.getSchemaType());
  }
}
