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
import java.util.List;

import static org.junit.Assert.assertEquals;

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
            new SchemaReference("test1", "test2", -1)
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
            new SchemaReference("test1", "test2", 1)
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
}
