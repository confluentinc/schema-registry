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

import io.confluent.rest.NamedURI;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;
import org.junit.Test;

import java.util.Properties;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import static org.junit.Assert.assertEquals;

public class KafkaSchemaRegistryTest {

  @Test
  public void testGetPortForIdentityPrecedence() throws SchemaRegistryException, RestConfigException {
    String listeners = "http://localhost:456";
    Properties props = new Properties();
    props.setProperty(RestConfig.PORT_CONFIG, "123");
    props.setProperty(RestConfig.LISTENERS_CONFIG, listeners);
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);

    NamedURI listener =
        KafkaSchemaRegistry.getInterInstanceListener(config.getListeners(), "", SchemaRegistryConfig.HTTP);
    assertEquals("Expected listeners to take precedence over port.", 456, listener.getUri().getPort());
    assertEquals("Expected Scheme match", SchemaRegistryConfig.HTTP, listener.getUri().getScheme());
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
    assertEquals("Expected port to take the configured port value", 123, listener.getUri().getPort());
    assertEquals("Expected Scheme match", SchemaRegistryConfig.HTTP, listener.getUri().getScheme());
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
    assertEquals("Expected HTTPS listener's port to be returned", 456, listener.getUri().getPort());
    assertEquals("Expected Scheme match", SchemaRegistryConfig.HTTPS, listener.getUri().getScheme());
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
    assertEquals("Expected last listener's port to be returned", 456, listener.getUri().getPort());
    assertEquals("Expected Scheme match", SchemaRegistryConfig.HTTP, listener.getUri().getScheme());
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
    assertEquals("Expected internal listener's port to be returned", 123, listener.getUri().getPort());
    assertEquals("Expected internal listener's name to be returned", "bob", listener.getName());
    assertEquals("Expected Scheme match", SchemaRegistryConfig.HTTP, listener.getUri().getScheme());
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
}
