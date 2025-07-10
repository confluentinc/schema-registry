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
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.rest.NamedURI;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;
import org.junit.jupiter.api.Test;

import java.util.*;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import static io.confluent.kafka.schemaregistry.storage.Mode.IMPORT;
import static io.confluent.kafka.schemaregistry.storage.Mode.READONLY;
import static org.junit.jupiter.api.Assertions.*;

public class KafkaSchemaRegistryTest extends ClusterTestHarness {

  private SchemaRegistryConfig config;

  @Override
  protected void setUp() throws Exception {
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
        KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(), "", SchemaRegistryConfig.HTTP);
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
        KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(), "", SchemaRegistryConfig.HTTP);
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
        KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(), "", SchemaRegistryConfig.HTTPS);
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
        KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(), "", SchemaRegistryConfig.HTTP);
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
      KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(), config.interInstanceListenerName(), SchemaRegistryConfig.HTTP);
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
    NamedURI internalListener = KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(),
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
    NamedURI internalListener = KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(),
        config.interInstanceListenerName(), SchemaRegistryConfig.HTTP);

    assertEquals(schemaRegistryIdentity,
        KafkaSchemaRegistry.getMyIdentity(internalListener, true, config));
  }

  @Test
  public void testRegister() throws SchemaRegistryException {
    KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
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
    KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
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
    KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
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

    // Soft deletion.
    kafkaSchemaRegistry.deleteSchemaVersion("subject1", schema,false);
    schema = kafkaSchemaRegistry.get("subject1", 1, false);
    assertNull(schema);
    schema = kafkaSchemaRegistry.get("subject1", 1, true);
    assertEquals(expected, schema);

    // Hard deletion.
    kafkaSchemaRegistry.deleteSchemaVersion("subject1", schema,true);
    schema = kafkaSchemaRegistry.get("subject1", 1, true);
    assertNull(schema);
  }

  @Test
  public void testDeleteSubject() throws SchemaRegistryException {
    KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
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

    // Soft deletion.
    kafkaSchemaRegistry.deleteSubject("subject1", false);
    Set<String> subjects = kafkaSchemaRegistry.subjects("subject1",false);
    assertEquals(0, subjects.size());
    subjects = kafkaSchemaRegistry.subjects("subject1",true);
    assertEquals(1, subjects.size());
    assertEquals("subject1", subjects.toArray()[0]);

    // Hard deletion.
    kafkaSchemaRegistry.deleteSchemaVersion("subject1", schema,true);
    subjects = kafkaSchemaRegistry.subjects("subject1",true);
    assertEquals(0, subjects.size());
  }

    @Test
    public void testGetByVersion() throws SchemaRegistryException {
      KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
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
    KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();
    kafkaSchemaRegistry.setMode("subject1", new ModeUpdateRequest(IMPORT.name()));
    assertEquals(IMPORT, kafkaSchemaRegistry.getMode("subject1"));
    assertEquals(IMPORT, kafkaSchemaRegistry.getModeInScope("subject1"));
    assertNull(kafkaSchemaRegistry.getMode("subject2"));

    kafkaSchemaRegistry.setModeOrForward("subject1", new ModeUpdateRequest(READONLY.name()), true, new HashMap<String, String>());
    assertEquals(READONLY, kafkaSchemaRegistry.getMode("subject1"));
  }

  @Test
  public void testDeleteMode() throws SchemaRegistryException {
    KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
    kafkaSchemaRegistry.init();
    kafkaSchemaRegistry.setMode("subject1", new ModeUpdateRequest(READONLY.name()));
    assertEquals(READONLY, kafkaSchemaRegistry.getMode("subject1"));

    kafkaSchemaRegistry.deleteSubjectMode("subject1");
    assertNull(kafkaSchemaRegistry.getMode("subject1"));
  }

  @Test
  public void testGetVersionsWithSubjectPrefix() throws SchemaRegistryException {
    KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
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
    KafkaSchemaRegistry kafkaSchemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
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
}
