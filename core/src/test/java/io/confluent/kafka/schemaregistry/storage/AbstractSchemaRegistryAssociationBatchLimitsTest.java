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

package io.confluent.kafka.schemaregistry.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationDeleteOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOpRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpsertOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.AssociationBatchLimitExceededException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractSchemaRegistryAssociationBatchLimitsTest {

  private AbstractSchemaRegistry registry;

  @BeforeEach
  void setUp() throws Exception {
    registry = mock(AbstractSchemaRegistry.class, CALLS_REAL_METHODS);
    useConfig(new Properties());
    doReturn(Collections.emptyList()).when(registry)
        .getAssociationsByResourceId(any(), any(), any(), any());
    doReturn(Collections.emptyList()).when(registry)
        .getAssociationsByResourceName(any(), any(), any(), any(), any());
  }

  private void useConfig(Properties overrides) throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(SchemaRegistryConfig.ASSOCIATION_BATCH_MUTATE_LIMITS_ENABLED_CONFIG, "true");
    props.put(SchemaRegistryConfig.ASSOCIATION_BATCH_GET_LIMITS_ENABLED_CONFIG, "true");
    props.putAll(overrides);
    doReturn(new SchemaRegistryConfig(props)).when(registry).config();
  }

  private static List<Association> existing(int count) {
    List<Association> associations = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      associations.add(mock(Association.class));
    }
    return associations;
  }

  private static AssociationOp create(String associationType) {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":[]}");
    return new AssociationCreateOp(null, associationType, null, null, schema, null);
  }

  private static AssociationOp upsertWithoutSchema(String associationType) {
    return new AssociationUpsertOp("shared-" + associationType, associationType, null, null,
        null, null);
  }

  private static AssociationOpRequest topic(String resourceId, AssociationOp... ops) {
    return new AssociationOpRequest(resourceId + "-name", "default", resourceId, "topic",
        Arrays.asList(ops));
  }

  private static AssociationBatchRequest mutate(AssociationOpRequest... topics) {
    return new AssociationBatchRequest(Arrays.asList(topics));
  }

  private static AssociationBatchGetRequest get(AssociationGetRequest... queries) {
    return new AssociationBatchGetRequest(Arrays.asList(queries));
  }

  private void verifyNoLookups() throws Exception {
    verify(registry, never()).getAssociationsByResourceId(any(), any(), any(), any());
    verify(registry, never()).getAssociationsByResourceName(any(), any(), any(), any(), any());
  }

  // ---------- batchMutate ----------

  @Test
  void mutateLimitsDisabledEnforcesNothing() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.ASSOCIATION_BATCH_MUTATE_LIMITS_ENABLED_CONFIG, "false");
    useConfig(props);

    assertDoesNotThrow(() -> registry.checkAssociationBatchLimits(mutate(
        topic("t1", create("key"), create("value")),
        topic("t2", create("key"), create("value")))));
    verifyNoLookups();
  }

  @Test
  void eachOperationTypeHasItsOwnLimit() throws Exception {
    doReturn(existing(1)).when(registry)
        .getAssociationsByResourceId(eq("t3"), any(), any(), any());

    assertDoesNotThrow(() -> registry.checkAssociationBatchLimits(mutate(
        topic("t1", create("key"), create("value")),
        topic("t2", upsertWithoutSchema("key"), upsertWithoutSchema("value")),
        topic("t3", new AssociationDeleteOp("key"), new AssociationDeleteOp("value")))));
  }

  @Test
  void createsAreCountedAcrossTopics() {
    AssociationBatchLimitExceededException e = assertThrows(
        AssociationBatchLimitExceededException.class,
        () -> registry.checkAssociationBatchLimits(mutate(
            topic("t1", create("value")),
            topic("t2", create("value")),
            topic("t3", create("value")))));
    assertTrue(e.getMessage().contains("has 3 CREATE operations"), e.getMessage());
    assertTrue(e.getMessage().contains("maximum of 2 CREATE operations"), e.getMessage());
  }

  @Test
  void upsertsWithoutSchemasAreLimited() {
    AssociationBatchLimitExceededException e = assertThrows(
        AssociationBatchLimitExceededException.class,
        () -> registry.checkAssociationBatchLimits(mutate(
            topic("t1", upsertWithoutSchema("key"), upsertWithoutSchema("value")),
            topic("t2", upsertWithoutSchema("value")))));
    assertTrue(e.getMessage().contains("has 3 UPSERT operations"), e.getMessage());
  }

  @Test
  void createLimitIsCheckedBeforeAnyDeleteLookup() throws Exception {
    assertThrows(AssociationBatchLimitExceededException.class,
        () -> registry.checkAssociationBatchLimits(mutate(
            topic("t1", create("key"), create("value")),
            topic("t2", create("value"), new AssociationDeleteOp("key")))));
    verifyNoLookups();
  }

  @Test
  void deletesOfMissingAssociationsAreNotCounted() {
    assertDoesNotThrow(() -> registry.checkAssociationBatchLimits(mutate(
        topic("t1", new AssociationDeleteOp("key"), new AssociationDeleteOp("value")),
        topic("t2", new AssociationDeleteOp("key"), new AssociationDeleteOp("value")),
        topic("t3", new AssociationDeleteOp("value")))));
  }

  @Test
  void deletesWithoutResourceIdAreNotCounted() {
    AssociationOpRequest noId = new AssociationOpRequest("t1-name", "default", null, "topic",
        Arrays.asList(new AssociationDeleteOp("key"), new AssociationDeleteOp("value")));
    AssociationOpRequest emptyId = new AssociationOpRequest("t2-name", "default", "", "topic",
        Collections.singletonList(new AssociationDeleteOp("value")));

    assertDoesNotThrow(() -> registry.checkAssociationBatchLimits(mutate(noId, emptyId)));
  }

  @Test
  void deleteLookupsStopOnceTheLimitIsPassed() throws Exception {
    doReturn(existing(1)).when(registry)
        .getAssociationsByResourceId(any(), any(), any(), any());

    AssociationBatchLimitExceededException e = assertThrows(
        AssociationBatchLimitExceededException.class,
        () -> registry.checkAssociationBatchLimits(mutate(
            topic("t1", new AssociationDeleteOp("key"), new AssociationDeleteOp("value")),
            topic("t2", new AssociationDeleteOp("key"), new AssociationDeleteOp("value")))));
    assertTrue(e.getMessage().contains("at least 3 DELETE operations"), e.getMessage());
    verify(registry, times(3)).getAssociationsByResourceId(any(), any(), any(), any());
  }

  @Test
  void deleteLookupUsesTheOperationsAssociationTypeAndDefaults() throws Exception {
    AssociationOpRequest defaults = new AssociationOpRequest("t1-name", "default", "t1", null,
        Collections.singletonList(new AssociationDeleteOp(null)));

    registry.checkAssociationBatchLimits(mutate(
        defaults, topic("t2", new AssociationDeleteOp("key"))));

    verify(registry).getAssociationsByResourceId(
        "t1", "topic", Collections.singletonList("value"), null);
    verify(registry).getAssociationsByResourceId(
        "t2", "topic", Collections.singletonList("key"), null);
  }

  @Test
  void entriesWithDeserializationErrorsAreNotCounted() {
    AssociationOpRequest failed = topic("t1", create("value"), create("key"));
    failed.setError(new ErrorMessage(42212, "invalid"));

    assertDoesNotThrow(() -> registry.checkAssociationBatchLimits(mutate(
        failed, topic("t2", create("key"), create("value")))));
  }

  @Test
  void mutatePayloadLimitAppliesToASingleOperation() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.MAX_ASSOCIATION_MUTATE_BATCH_PAYLOAD_BYTES_CONFIG, "200");
    useConfig(props);
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("\"" + String.join("", Collections.nCopies(300, "x")) + "\"");

    AssociationBatchLimitExceededException e = assertThrows(
        AssociationBatchLimitExceededException.class,
        () -> registry.checkAssociationBatchLimits(mutate(topic("t1",
            new AssociationCreateOp(null, "value", null, null, schema, null)))));
    assertTrue(e.getMessage().contains("bytes per batch"), e.getMessage());
    verifyNoLookups();
  }

  // ---------- batchGet ----------

  @Test
  void getLimitsDisabledEnforcesNothing() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.ASSOCIATION_BATCH_GET_LIMITS_ENABLED_CONFIG, "false");
    useConfig(props);

    assertDoesNotThrow(() -> registry.checkAssociationBatchGetLimits(true, get(
        new AssociationGetRequest("t1", "topic", null, null),
        new AssociationGetRequest("t2", "topic", null, null),
        new AssociationGetRequest("t3", "topic", null, null))));
    verifyNoLookups();
  }

  @Test
  void getCountsOnlyAssociationsThatExist() throws Exception {
    doReturn(existing(2)).when(registry)
        .getAssociationsByResourceId(eq("t1"), any(), any(), any());

    assertDoesNotThrow(() -> registry.checkAssociationBatchGetLimits(true, get(
        new AssociationGetRequest("t1", "topic", null, null),
        new AssociationGetRequest("missing-1", "topic", null, null),
        new AssociationGetRequest("missing-2", "topic", null, null))));
  }

  @Test
  void getLookupsStopOnceTheLimitIsPassedEvenWithoutSchemas() throws Exception {
    doReturn(existing(2)).when(registry)
        .getAssociationsByResourceId(eq("t1"), any(), any(), any());
    doReturn(existing(1)).when(registry)
        .getAssociationsByResourceId(eq("t2"), any(), any(), any());
    doReturn(existing(2)).when(registry)
        .getAssociationsByResourceId(eq("t3"), any(), any(), any());

    AssociationBatchLimitExceededException e = assertThrows(
        AssociationBatchLimitExceededException.class,
        () -> registry.checkAssociationBatchGetLimits(false, get(
            new AssociationGetRequest("t1", "topic", null, null),
            new AssociationGetRequest("t2", "topic", null, null),
            new AssociationGetRequest("t3", "topic", null, null))));
    assertTrue(e.getMessage().contains("at least 3 associations"), e.getMessage());
    verify(registry, never()).getAssociationsByResourceId(eq("t3"), any(), any(), any());
  }

  @Test
  void getEntriesByNameUseTheNameLookup() throws Exception {
    doReturn(existing(2)).when(registry)
        .getAssociationsByResourceName(eq("orders"), eq("lkc-1"), any(), any(), any());
    doReturn(existing(1)).when(registry)
        .getAssociationsByResourceName(eq("payments"), eq("lkc-1"), any(), any(), any());

    assertThrows(AssociationBatchLimitExceededException.class,
        () -> registry.checkAssociationBatchGetLimits(true, get(
            new AssociationGetRequest("orders", "lkc-1", "topic", null, null),
            new AssociationGetRequest("payments", "lkc-1", "topic", null, null))));
    verify(registry).getAssociationsByResourceName(
        "orders", "lkc-1", "topic", Collections.emptyList(), null);
    verify(registry, never()).getAssociationsByResourceId(any(), any(), any(), any());
  }

  @Test
  void getLookupUsesTheQueryFilters() throws Exception {
    registry.checkAssociationBatchGetLimits(true, get(new AssociationGetRequest(
        "t1", null, Collections.singletonList("key"), null)));

    verify(registry).getAssociationsByResourceId(
        "t1", "topic", Collections.singletonList("key"), null);
  }

  @Test
  void getPayloadLimitIsCheckedBeforeAnyLookup() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.MAX_ASSOCIATION_GET_BATCH_PAYLOAD_BYTES_CONFIG, "200");
    useConfig(props);
    String longName = String.join("", Collections.nCopies(300, "x"));

    AssociationBatchLimitExceededException e = assertThrows(
        AssociationBatchLimitExceededException.class,
        () -> registry.checkAssociationBatchGetLimits(true, get(
            new AssociationGetRequest(longName, "lkc-1", "topic", null, null))));
    assertTrue(e.getMessage().contains("bytes per batch"), e.getMessage());
    verifyNoLookups();
  }
}
