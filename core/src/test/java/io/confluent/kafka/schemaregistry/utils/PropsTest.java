/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.utils;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryDeployment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class PropsTest {

      @Test
  public void testGetSchemaRegistryDeploymentWithNullProperty() {
    Map<String, Object> props = new HashMap<>();

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return default SchemaRegistryDeployment when property is not present", result);
    assertEquals("Should contain empty list as default", 0, result.getAttributes().size());
  }

      @Test
  public void testGetSchemaRegistryDeploymentWithExplicitNullProperty() {
    Map<String, Object> props = new HashMap<>();
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, null);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return default SchemaRegistryDeployment when property is explicitly null", result);
    assertEquals("Should contain empty list as default", 0, result.getAttributes().size());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithValidStringList() {
    Map<String, Object> props = new HashMap<>();
    List<String> attributes = Arrays.asList("confluent", "enterprise");
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment for valid string list", result);
    assertEquals("Should contain the same attributes", attributes, result.getAttributes());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithSingleStringInList() {
    Map<String, Object> props = new HashMap<>();
    List<String> attributes = Arrays.asList("opensource");
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment for single string list", result);
    assertEquals("Should contain the single attribute", attributes, result.getAttributes());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithEmptyList() {
    Map<String, Object> props = new HashMap<>();
    List<String> attributes = new ArrayList<>();
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment for empty list", result);
    assertEquals("Should contain empty list", attributes, result.getAttributes());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithListContainingNulls() {
    Map<String, Object> props = new HashMap<>();
    List<Object> attributes = Arrays.asList("confluent", null, "enterprise");
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    // This should throw NullPointerException because null.toString() fails
    assertThrows("Should throw NullPointerException when list contains null",
        NullPointerException.class,
        () -> Props.getSchemaRegistryDeployment(props));
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithIntegerInList() {
    Map<String, Object> props = new HashMap<>();
    List<Object> attributes = Arrays.asList("confluent", 123, "enterprise");
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment for list with integer", result);
    List<String> expected = Arrays.asList("confluent", "123", "enterprise");
    assertEquals("Should convert integer to string", expected, result.getAttributes());
  }

    @Test
  public void testGetSchemaRegistryDeploymentWithBooleanInList() {
    Map<String, Object> props = new HashMap<>();
    List<Object> attributes = Arrays.asList("confluent", true, "enterprise");
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment for list with boolean", result);
    List<String> expected = Arrays.asList("confluent", "true", "enterprise");
    assertEquals("Should convert boolean to string", expected, result.getAttributes());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithNonListProperty() {
    Map<String, Object> props = new HashMap<>();
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, "not-a-list");

    IllegalArgumentException exception = assertThrows(
        "Should throw IllegalArgumentException for non-list property",
        IllegalArgumentException.class,
        () -> Props.getSchemaRegistryDeployment(props)
    );

    assertEquals("Should have proper error message",
        "Invalid schema registry deployment: not-a-list",
        exception.getMessage());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithIntegerProperty() {
    Map<String, Object> props = new HashMap<>();
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, 42);

    IllegalArgumentException exception = assertThrows(
        "Should throw IllegalArgumentException for integer property",
        IllegalArgumentException.class,
        () -> Props.getSchemaRegistryDeployment(props)
    );

    assertEquals("Should have proper error message",
        "Invalid schema registry deployment: 42",
        exception.getMessage());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithBooleanProperty() {
    Map<String, Object> props = new HashMap<>();
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, false);

    IllegalArgumentException exception = assertThrows(
        "Should throw IllegalArgumentException for boolean property",
        IllegalArgumentException.class,
        () -> Props.getSchemaRegistryDeployment(props)
    );

    assertEquals("Should have proper error message",
        "Invalid schema registry deployment: false",
        exception.getMessage());
  }

  @Test
  public void testPropertyConstantValue() {
    assertEquals("Property constant should have correct value",
        "schema.registry.metadata.deployment.attributes",
        Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES);
  }

    @Test
  public void testGetSchemaRegistryDeploymentWithMixedCaseStrings() {
    Map<String, Object> props = new HashMap<>();
    List<String> attributes = Arrays.asList("CONFLUENT", "Enterprise", "opensource");
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment for mixed case strings", result);
    List<String> expected = Arrays.asList("CONFLUENT", "Enterprise", "opensource");
    assertEquals("Should convert to lowercase", expected, result.getAttributes());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithEmptyStrings() {
    Map<String, Object> props = new HashMap<>();
    List<String> attributes = Arrays.asList("confluent", "", "enterprise");
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment for list with empty strings", result);
    List<String> expected = Arrays.asList("confluent", "", "enterprise");
    assertEquals("Should preserve empty strings after trimming and lowercasing", expected, result.getAttributes());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithWhitespaceStrings() {
    Map<String, Object> props = new HashMap<>();
    List<String> attributes = Arrays.asList("confluent", "   ", "enterprise");
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment for list with whitespace strings", result);
    List<String> expected = Arrays.asList("confluent", "", "enterprise");
    assertEquals("Should trim whitespace", expected, result.getAttributes());
  }

  @Test
  public void testGetSchemaRegistryDeploymentWithComplexObjects() {
    Map<String, Object> props = new HashMap<>();
    List<Object> attributes = Arrays.asList("confluent", 42, true, 3.14);
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment for list with mixed types", result);
    List<String> expected = Arrays.asList("confluent", "42", "true", "3.14");
    assertEquals("Should convert all types to lowercase strings", expected, result.getAttributes());
  }

  @Test
  public void testGetSchemaRegistryDeploymentTrims() {
    Map<String, Object> props = new HashMap<>();
    List<String> attributes = Arrays.asList("  CONFLUENT  ", "Enterprise ", " opensource");
    props.put(Props.PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES, attributes);

    SchemaRegistryDeployment result = Props.getSchemaRegistryDeployment(props);
    assertNotNull("Should return SchemaRegistryDeployment", result);
    List<String> expected = Arrays.asList("CONFLUENT", "Enterprise", "opensource");
    assertEquals("Should trim whitespace and convert to lowercase", expected, result.getAttributes());
  }
}
