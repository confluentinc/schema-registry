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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.rest.NamedURI;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;
import org.junit.jupiter.api.Test;

import java.util.*;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import static io.confluent.kafka.schemaregistry.storage.AbstractSchemaRegistry.getInterInstanceListener;
import static io.confluent.kafka.schemaregistry.storage.Mode.IMPORT;
import static io.confluent.kafka.schemaregistry.storage.Mode.READONLY;
import static io.confluent.kafka.schemaregistry.storage.Mode.READWRITE;
import static org.junit.jupiter.api.Assertions.*;

public class KafkaSchemaRegistryTest extends ClusterTestHarness {

  private SchemaRegistryConfig config;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    String listeners = "http://localhost:123, https://localhost:456";
    Properties props = new Properties();
    props.setProperty(RestConfig.PORT_CONFIG, "123");
    props.setProperty(RestConfig.LISTENERS_CONFIG, listeners);
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    config = new SchemaRegistryConfig(props);
  }

  @Test
  public void testGetPortForIdentityPrecedence() throws SchemaRegistryException, RestConfigException {
    String listeners = "http://localhost:456";
    Properties props = new Properties();
    props.setProperty(RestConfig.PORT_CONFIG, "123");
    props.setProperty(RestConfig.LISTENERS_CONFIG, listeners);
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);

    NamedURI listener =
        getInterInstanceListener(config.getListeners(), "", SchemaRegistryConfig.HTTP);
    assertEquals(456, listener.getUri().getPort(), "Expected listeners to take precedence over port.");
    assertEquals(SchemaRegistryConfig.HTTP, listener.getUri().getScheme());
  }

  @Test
  public void testGetPortForIdentityNoListeners() throws SchemaRegistryException, RestConfigException {
    String listeners = "";
    Properties props = new Properties();
    props.setProperty(RestConfig.PORT_CONFIG, "123");
    props.setProperty(RestConfig.LISTENERS_CONFIG, listeners);
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);

    NamedURI listener =
        getInterInstanceListener(config.getListeners(), "", SchemaRegistryConfig.HTTP);
    assertEquals(123, listener.getUri().getPort(), "Expected port to take the configured port value");
    assertEquals(SchemaRegistryConfig.HTTP, listener.getUri().getScheme());
  }

  @Test
  public void testGetPortForIdentityMultipleListenersWithHttps() throws SchemaRegistryException, RestConfigException {
    String listeners = "http://localhost:123, https://localhost:456";
    Properties props = new Properties();
    props.setProperty(RestConfig.PORT_CONFIG, "-1");
    props.setProperty(RestConfig.LISTENERS_CONFIG, listeners);
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);

    NamedURI listener =
        getInterInstanceListener(config.getListeners(), "", SchemaRegistryConfig.HTTPS);
    assertEquals(456, listener.getUri().getPort(), "Expected HTTPS listener's port to be returned");
    assertEquals(SchemaRegistryConfig.HTTPS, listener.getUri().getScheme());
  }

  @Test
  public void testGetPortForIdentityMultipleListeners() throws SchemaRegistryException, RestConfigException {
    String listeners = "http://localhost:123, http://localhost:456";
    Properties props = new Properties();
    props.setProperty(RestConfig.PORT_CONFIG, "-1");
    props.setProperty(RestConfig.LISTENERS_CONFIG, listeners);
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);

    NamedURI listener =
        getInterInstanceListener(config.getListeners(), "", SchemaRegistryConfig.HTTP);
    assertEquals(456, listener.getUri().getPort(), "Expected last listener's port to be returned");
    assertEquals(SchemaRegistryConfig.HTTP, listener.getUri().getScheme());
  }

  @Test
  public void testGetNamedInternalListener() throws SchemaRegistryException, RestConfigException {
    String listeners = "bob://localhost:123, http://localhost:456";
    String listenerProtocolMap = "bob:http";
    Properties props = new Properties();
    props.setProperty(RestConfig.PORT_CONFIG, "-1");
    props.setProperty(RestConfig.LISTENERS_CONFIG, listeners);
    props.setProperty(RestConfig.LISTENER_PROTOCOL_MAP_CONFIG, listenerProtocolMap);
    props.setProperty(SchemaRegistryConfig.INTER_INSTANCE_LISTENER_NAME_CONFIG, "bob");
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);

    NamedURI listener =
      getInterInstanceListener(config.getListeners(), config.interInstanceListenerName(), SchemaRegistryConfig.HTTP);
    assertEquals(123, listener.getUri().getPort());
    assertEquals("bob", listener.getName());
    assertEquals(SchemaRegistryConfig.HTTP, listener.getUri().getScheme());
  }

  @Test
  public void testMyIdentityWithoutPortOverride() throws RestConfigException, SchemaRegistryException {
    String listeners = "bob://localhost:123, http://localhost:456";
    String listenerProtocolMap = "bob:https";
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.HOST_NAME_CONFIG, "schema.registry-0.example.com");
    props.setProperty(RestConfig.LISTENERS_CONFIG, listeners);
    props.setProperty(RestConfig.LISTENER_PROTOCOL_MAP_CONFIG, listenerProtocolMap);
    props.setProperty(SchemaRegistryConfig.INTER_INSTANCE_LISTENER_NAME_CONFIG, "bob");
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    SchemaRegistryIdentity schemaRegistryIdentity = new
        SchemaRegistryIdentity("schema.registry-0.example.com", 123, true, "https");
    NamedURI internalListener = getInterInstanceListener(config.getListeners(),
        config.interInstanceListenerName(), SchemaRegistryConfig.HTTP);

    assertEquals(schemaRegistryIdentity,
        KafkaSchemaRegistry.getMyIdentity(internalListener, true, config));
  }

  @Test
  public void testMyIdentityWithPortOverride() throws RestConfigException, SchemaRegistryException {
    String listeners = "bob://localhost:123, http://localhost:456";
    String listenerProtocolMap = "bob:https";
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.HOST_NAME_CONFIG, "schema.registry-0.example.com");
    props.setProperty(SchemaRegistryConfig.HOST_PORT_CONFIG, "443");
    props.setProperty(RestConfig.LISTENERS_CONFIG, listeners);
    props.setProperty(RestConfig.LISTENER_PROTOCOL_MAP_CONFIG, listenerProtocolMap);
    props.setProperty(SchemaRegistryConfig.INTER_INSTANCE_LISTENER_NAME_CONFIG, "bob");
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    SchemaRegistryIdentity schemaRegistryIdentity = new
        SchemaRegistryIdentity("schema.registry-0.example.com", 443, true, "https");
    NamedURI internalListener = getInterInstanceListener(config.getListeners(),
        config.interInstanceListenerName(), SchemaRegistryConfig.HTTP);

    assertEquals(schemaRegistryIdentity,
        KafkaSchemaRegistry.getMyIdentity(internalListener, true, config));
  }

  @Test
  public void testRegister() throws SchemaRegistryException {
    SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();

    Schema expected = new Schema(
            "subject1",
            -1,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            StoreUtils.avroSchemaString(1));
    Schema actual = kafkaSchemaRegistry.register("subject1", expected);
    assertEquals(expected, actual);
    Schema schema = kafkaSchemaRegistry.get("subject1", 1, false);
    assertEquals(expected, schema);
  }

  @Test
  public void testGet() throws SchemaRegistryException {
    SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();

    Schema expected = new Schema(
            "subject1",
            -1,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            StoreUtils.avroSchemaString(1));
    kafkaSchemaRegistry.register("subject1", expected);

    Schema schema = kafkaSchemaRegistry.get("subject1", 1, false);
    assertEquals(expected, schema);
    schema = kafkaSchemaRegistry.get("subject1", -1, false);
    assertEquals(expected, schema);
    schema = kafkaSchemaRegistry.get("subject1", 2, false);
    assertNull(schema);
    schema = kafkaSchemaRegistry.get("subject2", 1, false);
    assertNull(schema);
  }

  @Test
  public void testDeleteSchema() throws SchemaRegistryException {
    SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();

    // Set global mode and config
    ConfigUpdateRequest globalConfigUpdateRequest = new ConfigUpdateRequest();
    globalConfigUpdateRequest.setCompatibilityLevel("FORWARD");
    kafkaSchemaRegistry.updateConfig(null, globalConfigUpdateRequest);
    assertEquals("FORWARD", kafkaSchemaRegistry.getConfig(null).getCompatibilityLevel());
    kafkaSchemaRegistry.setMode(null, new ModeUpdateRequest(READONLY.name()));
    assertEquals(READONLY, kafkaSchemaRegistry.getMode(null));

    // Register two schemas for the same subject
    Schema expected1 = new Schema(
            "subject1",
            -1,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            StoreUtils.avroSchemaString(1));
    Schema expected2 = new Schema(
            "subject1",
            -1,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            StoreUtils.avroSchemaString(2));

    // Set mode and config for the subject
    kafkaSchemaRegistry.setMode("subject1", new ModeUpdateRequest(READWRITE.name()));
    assertEquals(READWRITE, kafkaSchemaRegistry.getMode("subject1"));
    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest();
    configUpdateRequest.setCompatibilityLevel("FULL");
    kafkaSchemaRegistry.updateConfig("subject1", configUpdateRequest);
    assertEquals("FULL", kafkaSchemaRegistry.getConfig("subject1").getCompatibilityLevel());

    kafkaSchemaRegistry.register("subject1", expected1);
    kafkaSchemaRegistry.register("subject1", expected2);

    Schema schema1 = kafkaSchemaRegistry.get("subject1", 1, false);
    assertEquals(expected1, schema1);

    Schema schema2 = kafkaSchemaRegistry.get("subject1", 2, false);
    assertEquals(expected2, schema2);

    // Soft delete first version
    kafkaSchemaRegistry.deleteSchemaVersion("subject1", schema1, false);
    assertNull(kafkaSchemaRegistry.get("subject1", 1, false));
    assertEquals(expected1, kafkaSchemaRegistry.get("subject1", 1, true));

    // Mode and config should still exist
    assertEquals(READWRITE, kafkaSchemaRegistry.getMode("subject1"));
    assertEquals("FULL", kafkaSchemaRegistry.getConfig("subject1").getCompatibilityLevel());

    // Hard delete first version
    kafkaSchemaRegistry.deleteSchemaVersion("subject1", schema1, true);
    assertNull(kafkaSchemaRegistry.get("subject1", 1, true));

    // Mode and config should still exist (since version 2 still exists)
    assertEquals(READWRITE, kafkaSchemaRegistry.getMode("subject1"));
    assertEquals("FULL", kafkaSchemaRegistry.getConfig("subject1").getCompatibilityLevel());

    // Soft delete second version
    kafkaSchemaRegistry.deleteSchemaVersion("subject1", schema2, false);
    assertNull(kafkaSchemaRegistry.get("subject1", 2, false));
    assertEquals(expected2, kafkaSchemaRegistry.get("subject1", 2, true));

    // Mode and config should still exist
    assertEquals(READWRITE, kafkaSchemaRegistry.getMode("subject1"));
    assertEquals("FULL", kafkaSchemaRegistry.getConfig("subject1").getCompatibilityLevel());

    // Hard delete second version
    kafkaSchemaRegistry.deleteSchemaVersion("subject1", schema2, true);
    assertNull(kafkaSchemaRegistry.get("subject1", 2, true));

    // Now, mode and config should be deleted (since all versions are gone)
    assertNull(kafkaSchemaRegistry.getMode("subject1"));
    assertNull(kafkaSchemaRegistry.getConfig("subject1"));

    // Global mode and config should remain unchanged
    assertEquals(READONLY, kafkaSchemaRegistry.getMode(null));
    assertEquals("FORWARD", kafkaSchemaRegistry.getConfig(null).getCompatibilityLevel());
  }

  @Test
  public void testDeleteSubject() throws SchemaRegistryException {
    SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();

    // Set global mode and config
    ConfigUpdateRequest globalConfigUpdateRequest = new ConfigUpdateRequest();
    globalConfigUpdateRequest.setCompatibilityLevel("FORWARD");
    kafkaSchemaRegistry.updateConfig(null, globalConfigUpdateRequest);
    assertEquals("FORWARD", kafkaSchemaRegistry.getConfig(null).getCompatibilityLevel());
    kafkaSchemaRegistry.setMode(null, new ModeUpdateRequest(READONLY.name()));
    assertEquals(READONLY, kafkaSchemaRegistry.getMode(null));

    Schema expected = new Schema(
            "subject1",
            -1,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            StoreUtils.avroSchemaString(1));

    // Set mode and config for the subject
    kafkaSchemaRegistry.setMode("subject1", new ModeUpdateRequest(READWRITE.name()));
    assertEquals(READWRITE, kafkaSchemaRegistry.getMode("subject1"));
    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest();
    configUpdateRequest.setCompatibilityLevel("FULL");
    kafkaSchemaRegistry.updateConfig("subject1", configUpdateRequest);
    assertEquals("FULL", kafkaSchemaRegistry.getConfig("subject1").getCompatibilityLevel());

    kafkaSchemaRegistry.register("subject1", expected);
    Schema schema = kafkaSchemaRegistry.get("subject1", 1, false);
    assertEquals(expected, schema);

    // Soft deletion.
    kafkaSchemaRegistry.deleteSubject("subject1", false);
    Set<String> subjects = kafkaSchemaRegistry.subjects("subject1",false);
    assertEquals(0, subjects.size());
    subjects = kafkaSchemaRegistry.subjects("subject1",true);
    assertEquals(1, subjects.size());
    assertEquals("subject1", subjects.toArray()[0]);

    // Mode and config should still exist after soft delete
    assertEquals(READWRITE, kafkaSchemaRegistry.getMode("subject1"));
    assertEquals("FULL", kafkaSchemaRegistry.getConfig("subject1").getCompatibilityLevel());

    // Hard deletion.
    kafkaSchemaRegistry.deleteSubject("subject1",true);
    subjects = kafkaSchemaRegistry.subjects("subject1",true);
    assertEquals(0, subjects.size());

    // Mode and config should be deleted after hard delete
    assertNull(kafkaSchemaRegistry.getMode("subject1"));
    assertNull(kafkaSchemaRegistry.getConfig("subject1"));

    // Global mode and config should remain unchanged after hard delete
    assertEquals(READONLY, kafkaSchemaRegistry.getMode(null));
    assertEquals("FORWARD", kafkaSchemaRegistry.getConfig(null).getCompatibilityLevel());
  }

    @Test
    public void testGetByVersion() throws SchemaRegistryException {
      SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
      kafkaSchemaRegistry.init();

      Schema schema1 = new Schema(
              "subject1",
              0,
              -1,
              AvroSchema.TYPE,
              Collections.emptyList(),
              StoreUtils.avroSchemaString(1));
      kafkaSchemaRegistry.register("subject1", schema1);

      Schema actual = kafkaSchemaRegistry.getByVersion("subject1", 1, false);
      assertEquals(schema1, actual);
      actual = kafkaSchemaRegistry.getByVersion("subject1", 2, false);
      assertNull(actual);

      // Register the same schema again. No new version should have been created by this.
      Schema schema2 = new Schema(
              "subject1",
              0,
              -1,
              AvroSchema.TYPE,
              Collections.emptyList(),
              StoreUtils.avroSchemaString(1));
      kafkaSchemaRegistry.register("subject1", schema2);
      actual = kafkaSchemaRegistry.getByVersion("subject1", 2, false);
      assertNull(actual);

      // Register a new schema. A new version should have been created by this.
      Schema schema3 = new Schema(
              "subject1",
              0,
              -1,
              AvroSchema.TYPE,
              Collections.emptyList(),
              StoreUtils.avroSchemaString(2));
      kafkaSchemaRegistry.register("subject1", schema3);
      actual = kafkaSchemaRegistry.getByVersion("subject1", 2, false);
      assertEquals(schema3, actual);
      Iterator<SchemaKey>  itr = kafkaSchemaRegistry.getAllVersions("subject1", LookupFilter.DEFAULT);
      assertEquals(1, itr.next().getVersion());
      assertEquals(2, itr.next().getVersion());
      assertFalse(itr.hasNext());
    }

  @Test
  public void testSetMode() throws SchemaRegistryException {
    SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();
    kafkaSchemaRegistry.setMode("subject1", new ModeUpdateRequest(IMPORT.name()));
    assertEquals(IMPORT, kafkaSchemaRegistry.getMode("subject1"));
    assertEquals(IMPORT, kafkaSchemaRegistry.getModeInScope("subject1"));
    assertNull(kafkaSchemaRegistry.getMode("subject2"));

    kafkaSchemaRegistry.setModeOrForward("subject1", new ModeUpdateRequest(READONLY.name()), true, new HashMap<>());
    assertEquals(READONLY, kafkaSchemaRegistry.getMode("subject1"));
  }

  @Test
  public void testDeleteMode() throws SchemaRegistryException {
    SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();
    kafkaSchemaRegistry.setMode("subject1", new ModeUpdateRequest(READONLY.name()));
    assertEquals(READONLY, kafkaSchemaRegistry.getMode("subject1"));

    kafkaSchemaRegistry.deleteSubjectMode("subject1");
    assertNull(kafkaSchemaRegistry.getMode("subject1"));
  }

  @Test
  public void testGetVersionsWithSubjectPrefix() throws SchemaRegistryException {
    SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();

    // No subject yet.
    Iterator<ExtendedSchema> itr = kafkaSchemaRegistry.getVersionsWithSubjectPrefix("subject1", true, LookupFilter.DEFAULT, false, schema -> true);
    assertFalse(itr.hasNext());

    Schema schema1 = new Schema(
            "subject1",
            0,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            StoreUtils.avroSchemaString(1));
    kafkaSchemaRegistry.register("subject1", schema1);
    itr = kafkaSchemaRegistry.getVersionsWithSubjectPrefix("subject1", true, LookupFilter.DEFAULT, false, schema -> true);
    // Should match one subject.
    assertTrue(itr.hasNext());
    ExtendedSchema es = itr.next();
    assertEquals(schema1.getSchema(), es.getSchema());
    assertFalse(itr.hasNext());

    itr = kafkaSchemaRegistry.getVersionsWithSubjectPrefix("subject2", true, LookupFilter.DEFAULT, false, schema -> true);
    assertFalse(itr.hasNext());
  }

  @Test
  public void testIsCompatible() throws SchemaRegistryException {
    SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();

    // Register schema 1.
    Schema schema1 = new Schema(
            "subject1",
            0,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            StoreUtils.avroSchemaString(1));
    kafkaSchemaRegistry.register("subject1", schema1);

    // Schema 2 should be compatible and can be registered.
    Schema schema2 = new Schema(
            "subject1",
            0,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            StoreUtils.avroSchemaString(2));
    List<SchemaKey> list = new ArrayList<>();
    list.add(new SchemaKey("subject1", 1));
    List<String> errors = kafkaSchemaRegistry.isCompatible("subject1", schema2, list, true);
    assertTrue(errors.isEmpty());
    kafkaSchemaRegistry.register("subject1", schema2);
    list.add(new SchemaKey("subject1", 2));

    // Schema 3 should be compatible.
    Schema schema3 = new Schema(
            "subject1",
            0,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            StoreUtils.avroSchemaString(2));
    errors = kafkaSchemaRegistry.isCompatible("subject1", schema3, list, true);
    assertTrue(errors.isEmpty());

    // Schema 4 should be incompatible.
    Schema schema4 = new Schema(
            "subject1",
            0,
            -1,
            AvroSchema.TYPE,
            Collections.emptyList(),
            // Change the name to make the schema incompatible.
            StoreUtils.avroSchemaString(2).replace("Foo", "Bar"));
    errors = kafkaSchemaRegistry.isCompatible("subject1", schema4, list, true);
    assertFalse(errors.isEmpty());
  }

  @Test
  public void testGetAssociationsByResourceNamespace() throws SchemaRegistryException {
    SchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();

    // 6 resources: topic0+topic1 in lkc1, topic0+topic2 in lkc2, topic0+topic3 in lkc3
    // Each resource gets two associations (key/value), mixing STRONG and WEAK lifecycles.
    // Subjects are unique per resource to satisfy the one-STRONG-per-subject constraint.
    int schemaFieldCount = 1;
    String[][] resources = {
        // resourceName, namespace, resourceId, keyLifecycle, valueLifecycle
        {"topic0", "lkc1", "id-lkc1-topic0", "WEAK",   "STRONG"},
        {"topic1", "lkc1", "id-lkc1-topic1", "STRONG", "WEAK"},
        {"topic0", "lkc2", "id-lkc2-topic0", "WEAK",   "STRONG"},
        {"topic2", "lkc2", "id-lkc2-topic2", "STRONG", "WEAK"},
        {"topic0", "lkc3", "id-lkc3-topic0", "WEAK",   "STRONG"},
        {"topic3", "lkc3", "id-lkc3-topic3", "STRONG", "WEAK"},
    };
    for (String[] r : resources) {
      String resourceName = r[0], namespace = r[1], resourceId = r[2];
      LifecyclePolicy keyLifecycle = LifecyclePolicy.valueOf(r[3]);
      LifecyclePolicy valueLifecycle = LifecyclePolicy.valueOf(r[4]);
      String subjectPrefix = namespace + "-" + resourceName;

      RegisterSchemaRequest keyReq = new RegisterSchemaRequest();
      keyReq.setSchema(StoreUtils.avroSchemaString(schemaFieldCount++));
      RegisterSchemaRequest valueReq = new RegisterSchemaRequest();
      valueReq.setSchema(StoreUtils.avroSchemaString(schemaFieldCount++));

      kafkaSchemaRegistry.createAssociation(null, false, new AssociationCreateOrUpdateRequest(
          resourceName, namespace, resourceId, "topic",
          Arrays.asList(
              new AssociationCreateOrUpdateInfo(subjectPrefix + "-key", "key", keyLifecycle, false, keyReq, null),
              new AssociationCreateOrUpdateInfo(subjectPrefix + "-value", "value", valueLifecycle, false, valueReq, null)
          )
      ));
    }

    // Wildcard namespace ("-"): should return all 12 associations across all 3 namespaces
    List<Association> all = kafkaSchemaRegistry.getAssociationsByResourceNamespace(
        "-", "topic", Collections.emptyList(), null);
    assertEquals(12, all.size());
    assertTrue(all.stream().anyMatch(a -> "lkc1".equals(a.getResourceNamespace())));
    assertTrue(all.stream().anyMatch(a -> "lkc2".equals(a.getResourceNamespace())));
    assertTrue(all.stream().anyMatch(a -> "lkc3".equals(a.getResourceNamespace())));

    // lkc1 namespace: topic0/lkc1 (key=WEAK, value=STRONG) + topic1/lkc1 (key=STRONG, value=WEAK)
    List<Association> lkc1 = kafkaSchemaRegistry.getAssociationsByResourceNamespace(
        "lkc1", "topic", Collections.emptyList(), null);
    assertEquals(4, lkc1.size());
    assertTrue(lkc1.stream().allMatch(a -> "lkc1".equals(a.getResourceNamespace())));
    assertTrue(lkc1.stream().anyMatch(a ->
        "topic0".equals(a.getResourceName()) && "key".equals(a.getAssociationType()) && LifecyclePolicy.WEAK == a.getLifecycle()));
    assertTrue(lkc1.stream().anyMatch(a ->
        "topic0".equals(a.getResourceName()) && "value".equals(a.getAssociationType()) && LifecyclePolicy.STRONG == a.getLifecycle()));
    assertTrue(lkc1.stream().anyMatch(a ->
        "topic1".equals(a.getResourceName()) && "key".equals(a.getAssociationType()) && LifecyclePolicy.STRONG == a.getLifecycle()));
    assertTrue(lkc1.stream().anyMatch(a ->
        "topic1".equals(a.getResourceName()) && "value".equals(a.getAssociationType()) && LifecyclePolicy.WEAK == a.getLifecycle()));

    // lkc1 filtered to "key" associations only: topic0/key (WEAK) + topic1/key (STRONG)
    List<Association> lkc1Keys = kafkaSchemaRegistry.getAssociationsByResourceNamespace(
        "lkc1", "topic", List.of("key"), null);
    assertEquals(2, lkc1Keys.size());
    assertTrue(lkc1Keys.stream().allMatch(a -> "lkc1".equals(a.getResourceNamespace())));
    assertTrue(lkc1Keys.stream().allMatch(a -> "key".equals(a.getAssociationType())));
    assertTrue(lkc1Keys.stream().anyMatch(a ->
        "topic0".equals(a.getResourceName()) && LifecyclePolicy.WEAK == a.getLifecycle()));
    assertTrue(lkc1Keys.stream().anyMatch(a ->
        "topic1".equals(a.getResourceName()) && LifecyclePolicy.STRONG == a.getLifecycle()));

    // lkc2 namespace: topic0/lkc2 (key=WEAK, value=STRONG) + topic2/lkc2 (key=STRONG, value=WEAK)
    List<Association> lkc2 = kafkaSchemaRegistry.getAssociationsByResourceNamespace(
        "lkc2", "topic", Collections.emptyList(), null);
    assertEquals(4, lkc2.size());
    assertTrue(lkc2.stream().allMatch(a -> "lkc2".equals(a.getResourceNamespace())));
    assertTrue(lkc2.stream().anyMatch(a ->
        "topic0".equals(a.getResourceName()) && "key".equals(a.getAssociationType()) && LifecyclePolicy.WEAK == a.getLifecycle()));
    assertTrue(lkc2.stream().anyMatch(a ->
        "topic0".equals(a.getResourceName()) && "value".equals(a.getAssociationType()) && LifecyclePolicy.STRONG == a.getLifecycle()));
    assertTrue(lkc2.stream().anyMatch(a ->
        "topic2".equals(a.getResourceName()) && "key".equals(a.getAssociationType()) && LifecyclePolicy.STRONG == a.getLifecycle()));
    assertTrue(lkc2.stream().anyMatch(a ->
        "topic2".equals(a.getResourceName()) && "value".equals(a.getAssociationType()) && LifecyclePolicy.WEAK == a.getLifecycle()));

    // lkc3 namespace: topic0/lkc3 (key=WEAK, value=STRONG) + topic3/lkc3 (key=STRONG, value=WEAK)
    List<Association> lkc3 = kafkaSchemaRegistry.getAssociationsByResourceNamespace(
        "lkc3", "topic", Collections.emptyList(), null);
    assertEquals(4, lkc3.size());
    assertTrue(lkc3.stream().allMatch(a -> "lkc3".equals(a.getResourceNamespace())));
    assertTrue(lkc3.stream().anyMatch(a ->
        "topic0".equals(a.getResourceName()) && "key".equals(a.getAssociationType()) && LifecyclePolicy.WEAK == a.getLifecycle()));
    assertTrue(lkc3.stream().anyMatch(a ->
        "topic0".equals(a.getResourceName()) && "value".equals(a.getAssociationType()) && LifecyclePolicy.STRONG == a.getLifecycle()));
    assertTrue(lkc3.stream().anyMatch(a ->
        "topic3".equals(a.getResourceName()) && "key".equals(a.getAssociationType()) && LifecyclePolicy.STRONG == a.getLifecycle()));
    assertTrue(lkc3.stream().anyMatch(a ->
        "topic3".equals(a.getResourceName()) && "value".equals(a.getAssociationType()) && LifecyclePolicy.WEAK == a.getLifecycle()));
  }

  @Test
  public void testLeaderRestServiceIsForwardIsTrue() throws Exception {
    KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();

    // Create a leader identity
    SchemaRegistryIdentity leaderIdentity = new SchemaRegistryIdentity(
        "test-host", 8081, true, "http");

    // Set the leader
    kafkaSchemaRegistry.setLeader(leaderIdentity);

    // Get the leader rest service
    RestService leaderRestService = kafkaSchemaRegistry.leaderRestService();
    assertNotNull(leaderRestService, "Leader rest service should not be null");

    // Verify that isForward is set to true - this ensures that requests to the leader
    // will include the X-Forward header, which is critical for proper request forwarding
    assertTrue(leaderRestService.isForward(), "isForward should be true for leaderRestService");
  }
}
