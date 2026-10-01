/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.kafka.schemaregistry;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AvroSchemaProviderTest {

  @Test
  public void testResolveRecursiveReferences() throws RestClientException, IOException {
    AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
    SchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
    avroSchemaProvider.configure(Collections.singletonMap(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG,
            mockSchemaRegistryClient));
    String schemaTest1Str =   "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}";
    String schemaTest2Str =  "{ \"type\": \"record\", \"name\": \"test2\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}";
    String schemaTest3Str =  "{ \"type\": \"record\", \"name\": \"test3\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}";
    List<SchemaReference> referencesForTest1 = Arrays.asList(
            new SchemaReference("test2", "test2", -1)
    );
    List<SchemaReference> referencesForTest3 = Arrays.asList(
            new SchemaReference("test1", "test1", -1),
            new SchemaReference("test2", "test2", -1)
    );

    Schema schemaTest1 = new Schema("test1", 1, 1001, AvroSchema.TYPE, referencesForTest1, schemaTest1Str);
    Schema schemaTest3 = new Schema("test3", 1, 1001, AvroSchema.TYPE, referencesForTest3, schemaTest3Str);

    mockSchemaRegistryClient.register("test2", new AvroSchema(schemaTest2Str));
    mockSchemaRegistryClient.register("test1", new AvroSchema(schemaTest1Str, referencesForTest1, avroSchemaProvider.resolveReferences(schemaTest1), null));
    mockSchemaRegistryClient.register("test3", new AvroSchema(schemaTest3Str, referencesForTest3, avroSchemaProvider.resolveReferences(schemaTest3), null));

    List<SchemaReference> referencesForTest1Resolved = Arrays.asList(
            new SchemaReference("test2", "test2", 1)
    );
    List<SchemaReference> referencesForTest3Resolved = Arrays.asList(
            new SchemaReference("test1", "test1", 1),
            new SchemaReference("test2", "test2", 1)
    );
    assertEquals(
            mockSchemaRegistryClient.getByVersion("test1", 1, true).getReferences(),
            referencesForTest1Resolved);
    assertEquals(
            mockSchemaRegistryClient.getByVersion("test3", 1, true).getReferences(),
            referencesForTest3Resolved);
  }

  @Test
  public void testResolveRecursiveCircularReferences() throws RestClientException, IOException {
    AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
    SchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
    avroSchemaProvider.configure(Collections.singletonMap(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG,
            mockSchemaRegistryClient));
    String schemaTest1Str =   "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}";
    String schemaTest2Str =  "{ \"type\": \"record\", \"name\": \"test2\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}";
    String schemaTest3Str =  "{ \"type\": \"record\", \"name\": \"test3\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}";
    List<SchemaReference> referencesForTest1 = Arrays.asList(
            new SchemaReference("test2", "test2", -1)
    );
    List<SchemaReference> referencesForTest2 = Arrays.asList(
            new SchemaReference("test1", "test1", -1)
    );
    List<SchemaReference> referencesForTest3 = Arrays.asList(
            new SchemaReference("test1", "test1", -1),
            new SchemaReference("test2", "test2", -1)
    );

    Schema schemaTest1 = new Schema("test1", 1, 1001, AvroSchema.TYPE, referencesForTest1, schemaTest1Str);
    Schema schemaTest2 = new Schema("test2", 1, 1001, AvroSchema.TYPE, referencesForTest2, schemaTest2Str);
    Schema schemaTest3 = new Schema("test3", 1, 1001, AvroSchema.TYPE, referencesForTest3, schemaTest3Str);

    mockSchemaRegistryClient.register("test2", new AvroSchema(schemaTest2Str));
    mockSchemaRegistryClient.register("test1", new AvroSchema(schemaTest1Str, referencesForTest1, avroSchemaProvider.resolveReferences(schemaTest1), null));
    mockSchemaRegistryClient.register("test3", new AvroSchema(schemaTest3Str, referencesForTest3, avroSchemaProvider.resolveReferences(schemaTest3), null));


    schemaTest2Str =  "{ \"type\": \"record\", \"name\": \"test2U\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}";
    mockSchemaRegistryClient.register("test2", new AvroSchema(schemaTest2Str, referencesForTest2, avroSchemaProvider.resolveReferences(schemaTest2), null));

    List<SchemaReference> referencesForTest1Resolved = Arrays.asList(
            new SchemaReference("test2", "test2", 1)
    );
    List<SchemaReference> referencesForTest2Resolved = Arrays.asList(
            new SchemaReference("test1", "test1", 1)
    );
    List<SchemaReference> referencesForTest3Resolved = Arrays.asList(
            new SchemaReference("test1", "test1", 1),
            new SchemaReference("test2", "test2", 1)
    );
    assertEquals(
            mockSchemaRegistryClient.getByVersion("test1", 1, true).getReferences(),
            referencesForTest1Resolved);
    assertEquals(
            mockSchemaRegistryClient.getByVersion("test3", 1, true).getReferences(),
            referencesForTest3Resolved);
    assertEquals(
            mockSchemaRegistryClient.getByVersion("test2", 1, true).getReferences(),
            Arrays.asList());
    assertEquals(
            mockSchemaRegistryClient.getByVersion("test2", 2, true).getReferences(),
            referencesForTest2Resolved);
  }

  @Test
  public void testConflictingReferenceVersionsStrictRejects()
      throws RestClientException, IOException {
    AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
    SchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, mockSchemaRegistryClient);
    configs.put(AbstractSchemaProvider.REFERENCE_VERSIONS_STRICT_CONFIG, true);
    avroSchemaProvider.configure(configs);

    String innerSchemaV1 = "{ \"type\": \"record\", \"name\": \"Inner\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"f1\" }]}";
    String innerSchemaV2 = "{ \"type\": \"record\", \"name\": \"Inner\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"f1\" }, "
        + "{ \"type\": [\"null\", \"string\"], \"name\": \"f2\" }]}";
    String middleSchemaStr = "{ \"type\": \"record\", \"name\": \"Middle\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"m1\" }]}";
    String rootSchemaStr = "{ \"type\": \"record\", \"name\": \"Root\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"r1\" }]}";

    mockSchemaRegistryClient.register("inner", new AvroSchema(innerSchemaV1));
    mockSchemaRegistryClient.register("inner", new AvroSchema(innerSchemaV2));

    List<SchemaReference> middleRefs = Collections.singletonList(
        new SchemaReference("Inner", "inner", 2));
    Schema middleSchema = new Schema("middle", 1, 2001, AvroSchema.TYPE,
        middleRefs, middleSchemaStr);
    mockSchemaRegistryClient.register("middle", new AvroSchema(middleSchemaStr,
        middleRefs, avroSchemaProvider.resolveReferences(middleSchema), null));

    // Root references Inner at version 1 directly, and Middle which references Inner at version 2
    List<SchemaReference> rootRefs = Arrays.asList(
        new SchemaReference("Inner", "inner", 1),
        new SchemaReference("Middle", "middle", 1));
    Schema rootSchema = new Schema("root", 1, 3001, AvroSchema.TYPE, rootRefs, rootSchemaStr);

    try {
      avroSchemaProvider.resolveReferences(rootSchema);
      fail("Expected IllegalStateException for conflicting reference versions");
    } catch (IllegalStateException e) {
      assertEquals("Conflicting reference versions for \"Inner\": version 1 and version 2",
          e.getMessage());
    }
  }

  @Test
  public void testConflictingReferenceVersionsAllowedByDefault()
      throws RestClientException, IOException {
    AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
    SchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
    avroSchemaProvider.configure(Collections.singletonMap(
        SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, mockSchemaRegistryClient));

    String innerSchemaV1 = "{ \"type\": \"record\", \"name\": \"Inner\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"f1\" }]}";
    String innerSchemaV2 = "{ \"type\": \"record\", \"name\": \"Inner\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"f1\" }, "
        + "{ \"type\": [\"null\", \"string\"], \"name\": \"f2\" }]}";
    String middleSchemaStr = "{ \"type\": \"record\", \"name\": \"Middle\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"m1\" }]}";
    String rootSchemaStr = "{ \"type\": \"record\", \"name\": \"Root\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"r1\" }]}";

    mockSchemaRegistryClient.register("inner", new AvroSchema(innerSchemaV1));
    mockSchemaRegistryClient.register("inner", new AvroSchema(innerSchemaV2));

    List<SchemaReference> middleRefs = Collections.singletonList(
        new SchemaReference("Inner", "inner", 2));
    Schema middleSchema = new Schema("middle", 1, 2001, AvroSchema.TYPE,
        middleRefs, middleSchemaStr);
    mockSchemaRegistryClient.register("middle", new AvroSchema(middleSchemaStr,
        middleRefs, avroSchemaProvider.resolveReferences(middleSchema), null));

    // Root references Inner at version 1 directly, and Middle which references Inner at version 2
    List<SchemaReference> rootRefs = Arrays.asList(
        new SchemaReference("Inner", "inner", 1),
        new SchemaReference("Middle", "middle", 1));
    Schema rootSchema = new Schema("root", 1, 3001, AvroSchema.TYPE, rootRefs, rootSchemaStr);

    // Should not throw — default behavior allows conflicting versions
    avroSchemaProvider.resolveReferences(rootSchema);
  }

  @Test
  public void testSameReferenceVersionNotRejectedWhenStrict()
      throws RestClientException, IOException {
    AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
    SchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, mockSchemaRegistryClient);
    configs.put(AbstractSchemaProvider.REFERENCE_VERSIONS_STRICT_CONFIG, true);
    avroSchemaProvider.configure(configs);

    String innerSchemaStr = "{ \"type\": \"record\", \"name\": \"Inner\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"f1\" }]}";
    String middleSchemaStr = "{ \"type\": \"record\", \"name\": \"Middle\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"m1\" }]}";
    String rootSchemaStr = "{ \"type\": \"record\", \"name\": \"Root\", "
        + "\"fields\": [ { \"type\": \"string\", \"name\": \"r1\" }]}";

    mockSchemaRegistryClient.register("inner", new AvroSchema(innerSchemaStr));

    List<SchemaReference> middleRefs = Collections.singletonList(
        new SchemaReference("Inner", "inner", 1));
    Schema middleSchema = new Schema("middle", 1, 2001, AvroSchema.TYPE,
        middleRefs, middleSchemaStr);
    mockSchemaRegistryClient.register("middle", new AvroSchema(middleSchemaStr,
        middleRefs, avroSchemaProvider.resolveReferences(middleSchema), null));

    // Root references Inner at version 1, and Middle also references Inner at version 1 — no clash
    List<SchemaReference> rootRefs = Arrays.asList(
        new SchemaReference("Inner", "inner", 1),
        new SchemaReference("Middle", "middle", 1));
    Schema rootSchema = new Schema("root", 1, 3001, AvroSchema.TYPE, rootRefs, rootSchemaStr);

    // Should not throw — same version is not a conflict
    avroSchemaProvider.resolveReferences(rootSchema);
  }
}
