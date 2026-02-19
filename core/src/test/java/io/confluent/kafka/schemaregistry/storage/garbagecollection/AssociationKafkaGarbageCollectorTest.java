/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage.garbagecollection;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.LookupFilter;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AssociationKafkaGarbageCollectorTest extends ClusterTestHarness {

  private SchemaRegistry schemaRegistry;

  private SchemaRegistryConfig config;
  private AssociationKafkaGarbageCollector garbageCollector;

  private String realTenant = "default";
  private String wrongTenant = "wrong-tenant";
  private String resource1 =  "resource1";
  private String resource2 = "resource2";
  private String resource3 = "resource3";
  private String subject1 = "subject1";
  private String subject2 = "subject2";
  private String subject3 = "subject3";
  private String resourceNamespace1 = "resourceNamespace1";
  private String resourceNamespace2 = "resourceNamespace2";

  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
    props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    // Disable the gc ingestion pipeline
    props.put(SchemaRegistryConfig.ASSOC_GC_ENABLE_CONFIG, false);
    this.config = new SchemaRegistryConfig(props);
    schemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    garbageCollector = new AssociationKafkaGarbageCollector(schemaRegistry);
    schemaRegistry.init();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (garbageCollector != null) {
      garbageCollector.close();
    }
    if (schemaRegistry != null) {
      schemaRegistry.close();
    }
    super.tearDown();
  }

  private Schema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
            "\"name\": \"User\"," +
            "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  private IndexedRecord createUserRecord() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  private AssociationCreateOrUpdateRequest getAssocCreateRequest(
          String resource, String resourceNamespace, String subject,
          String assocType, LifecyclePolicy policy) {
    IndexedRecord avroRecord = createUserRecord();
    RegisterSchemaRequest valueRequest =
            new RegisterSchemaRequest(new AvroSchema(avroRecord.getSchema()));
    return new AssociationCreateOrUpdateRequest(
            resource,
            resourceNamespace,
            resource,
            "topic",
            ImmutableList.of(
                    new AssociationCreateOrUpdateInfo(
                            subject,
                            assocType,
                            policy,
                            false,
                            valueRequest,
                            null
                    )
            )
    );
  }

  @Test
  public void testProcessDeletedResource_happyPath_weakAssociation() throws Exception {
    // create weak associations in the schema registry
    AssociationCreateOrUpdateRequest request = getAssocCreateRequest(
            resource1, resourceNamespace1, subject1, "value",
            LifecyclePolicy.WEAK
    );
    ThreadLocal<String> tenantId = new ThreadLocal<>();
    tenantId.set(realTenant);
    schemaRegistry.createAssociation("", false, request);
    List<Association> assocsBeforeDelete = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    tenantId.set(null);
    assertEquals(1, assocsBeforeDelete.size());

    // delete association through garbage collector
    garbageCollector.processDeletedResource(realTenant, resource1);
    // Threadlocal tenant should be set to null
    assertNull(tenantId.get());

    // getAssociation should return empty list
    tenantId.set(realTenant);
    List<Association> assocsAfterDelete = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    tenantId.set(null);
    assertEquals(0, assocsAfterDelete.size());

    // Subject should remain
    Iterator<SchemaKey> itr = schemaRegistry.getAllVersions(subject1, LookupFilter.DEFAULT);
    AtomicInteger size = new AtomicInteger();
    itr.forEachRemaining(schemaKey -> {
      size.set(size.get() + 1);
    });
    assertEquals(1, size.get());
  }

  @Test
  public void testProcessDeletedResource_happyPath_strongAssociation() throws Exception {
    // create weak associations in the schema registry
    AssociationCreateOrUpdateRequest request = getAssocCreateRequest(
            resource1, resourceNamespace1, subject1, "value",
            LifecyclePolicy.STRONG
    );
    ThreadLocal<String> tenantId = new ThreadLocal<>();
    tenantId.set(realTenant);
    schemaRegistry.createAssociation("", false, request);
    List<Association> assocsBeforeDelete = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    tenantId.set(null);
    assertEquals(1, assocsBeforeDelete.size());

    // delete association through garbage collector
    garbageCollector.processDeletedResource(realTenant, resource1);
    // Threadlocal tenant should be set to null
    assertNull(tenantId.get());

    // getAssociation should return empty list
    tenantId.set(realTenant);
    List<Association> assocsAfterDelete = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    tenantId.set(null);
    assertEquals(0, assocsAfterDelete.size());

    // Subject should be deleted
    Iterator<SchemaKey> itr = schemaRegistry.getAllVersions(subject1, LookupFilter.DEFAULT);
    AtomicInteger size = new AtomicInteger();
    itr.forEachRemaining(schemaKey -> {
      size.set(size.get() + 1);
    });
    assertEquals(0, size.get());
  }

  @Test
  public void testProcessDeletedResource_NoAssociationsFound() throws Exception {
    // Arrange - resource that doesn't exist
    ThreadLocal<String> tenantId = new ThreadLocal<>();
    tenantId.set(realTenant);
    
    // Verify no associations exist for this resource
    List<Association> assocsBeforeDelete = schemaRegistry.getAssociationsByResourceId(
            "nonexistent-resource", "topic", null, null);
    tenantId.set(null);
    assertEquals(0, assocsBeforeDelete.size());

    // Act - try to delete non-existent resource (should not throw exception)
    garbageCollector.processDeletedResource(realTenant, "nonexistent-resource");
    assertNull(tenantId.get());

    // Assert - verify nothing was deleted (no exception thrown)
    tenantId.set(realTenant);
    List<Association> assocsAfterDelete = schemaRegistry.getAssociationsByResourceId(
            "nonexistent-resource", "topic", null, null);
    tenantId.set(null);
    assertEquals(0, assocsAfterDelete.size());
  }

  @Test
  public void testProcessDeletedResource_MultipleAssociations() throws Exception {
    // Arrange - create two associations with same resource but different subjects
    AssociationCreateOrUpdateRequest request1 = getAssocCreateRequest(
            resource1, resourceNamespace1, subject1, "value",
            LifecyclePolicy.WEAK
    );
    AssociationCreateOrUpdateRequest request2 = getAssocCreateRequest(
            resource1, resourceNamespace1, subject2, "key",
            LifecyclePolicy.WEAK
    );
    
    ThreadLocal<String> tenantId = new ThreadLocal<>();
    tenantId.set(realTenant);
    schemaRegistry.createAssociation("", false, request1);
    schemaRegistry.createAssociation("", false, request2);
    
    List<Association> assocsBeforeDelete = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    tenantId.set(null);
    assertEquals(2, assocsBeforeDelete.size());

    // Act - delete all associations for this resource
    garbageCollector.processDeletedResource(realTenant, resource1);
    assertNull(tenantId.get());

    // Assert - all associations for this resource should be deleted
    tenantId.set(realTenant);
    List<Association> assocsAfterDelete = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    tenantId.set(null);
    assertEquals(0, assocsAfterDelete.size());
  }

  @Test
  public void testProcessDeletedResourceNamespace() throws Exception {
    // Arrange - create associations in same namespace and different namespace
    AssociationCreateOrUpdateRequest request1 = getAssocCreateRequest(
            resource1, resourceNamespace1, subject1, "value",
            LifecyclePolicy.WEAK
    );
    AssociationCreateOrUpdateRequest request2 = getAssocCreateRequest(
            resource2, resourceNamespace1, subject2, "value",
            LifecyclePolicy.WEAK
    );
    AssociationCreateOrUpdateRequest request3 = getAssocCreateRequest(
            resource3, resourceNamespace2, subject3, "value",
            LifecyclePolicy.WEAK
    );
    
    ThreadLocal<String> tenantId = new ThreadLocal<>();
    tenantId.set(realTenant);
    schemaRegistry.createAssociation("", false, request1);
    schemaRegistry.createAssociation("", false, request2);
    schemaRegistry.createAssociation("", false, request3);
    
    // Verify all associations exist
    List<Association> assocs1Before = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    List<Association> assocs2Before = schemaRegistry.getAssociationsByResourceId(
            resource2, "topic", null, null);
    List<Association> assocs3Before = schemaRegistry.getAssociationsByResourceId(
            "resource3", "topic", null, null);
    tenantId.set(null);
    assertEquals(1, assocs1Before.size());
    assertEquals(1, assocs2Before.size());
    assertEquals(1, assocs3Before.size());

    // Act - delete all resources in resourceNamespace1
    garbageCollector.processDeletedResourceNamespace(realTenant, resourceNamespace1);
    assertNull(tenantId.get());

    // Assert - resources in resourceNamespace1 should be deleted, but not resourceNamespace2
    tenantId.set(realTenant);
    List<Association> assocs1After = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    List<Association> assocs2After = schemaRegistry.getAssociationsByResourceId(
            resource2, "topic", null, null);
    List<Association> assocs3After = schemaRegistry.getAssociationsByResourceId(
            "resource3", "topic", null, null);
    tenantId.set(null);
    
    // resource1 and resource2 should be deleted (same namespace)
    assertEquals(0, assocs1After.size());
    assertEquals(0, assocs2After.size());
    // resource3 should still exist (different namespace)
    assertEquals(1, assocs3After.size());
  }

  @Test
  public void testProcessDeletedResourceNamespace_NoMatchingResources() throws Exception {
    // Arrange - create associations in different namespaces
    AssociationCreateOrUpdateRequest request1 = getAssocCreateRequest(
            resource1, resourceNamespace1, subject1, "value",
            LifecyclePolicy.WEAK
    );
    AssociationCreateOrUpdateRequest request2 = getAssocCreateRequest(
            resource2, resourceNamespace2, subject2, "value",
            LifecyclePolicy.WEAK
    );
    
    ThreadLocal<String> tenantId = new ThreadLocal<>();
    tenantId.set(realTenant);
    schemaRegistry.createAssociation("", false, request1);
    schemaRegistry.createAssociation("", false, request2);
    
    List<Association> assocs1Before = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    List<Association> assocs2Before = schemaRegistry.getAssociationsByResourceId(
            resource2, "topic", null, null);
    tenantId.set(null);
    assertEquals(1, assocs1Before.size());
    assertEquals(1, assocs2Before.size());

    // Act - try to delete resources in a non-existent namespace
    garbageCollector.processDeletedResourceNamespace(realTenant, "nonexistent-namespace");
    assertNull(tenantId.get());

    // Assert - all resources should still exist
    tenantId.set(realTenant);
    List<Association> assocs1After = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    List<Association> assocs2After = schemaRegistry.getAssociationsByResourceId(
            resource2, "topic", null, null);
    tenantId.set(null);
    assertEquals(1, assocs1After.size());
    assertEquals(1, assocs2After.size());
  }

  @Test
  public void testProcessDeletedResourceNamespace_EmptyStore() throws Exception {
    // Arrange - no associations exist in the store
    ThreadLocal<String> tenantId = new ThreadLocal<>();
    tenantId.set(realTenant);
    
    // Verify no associations exist
    List<Association> assocsBeforeDelete = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    tenantId.set(null);
    assertEquals(0, assocsBeforeDelete.size());

    // Act - try to delete namespace when no associations exist
    garbageCollector.processDeletedResourceNamespace(realTenant, resourceNamespace1);
    assertNull(tenantId.get());

    // Assert - nothing should happen (no exception thrown)
    tenantId.set(realTenant);
    List<Association> assocsAfterDelete = schemaRegistry.getAssociationsByResourceId(
            resource1, "topic", null, null);
    tenantId.set(null);
    assertEquals(0, assocsAfterDelete.size());
  }
}
