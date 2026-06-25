/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationInfo;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import org.junit.Test;

/**
 * Verifies that {@code frozen} is serialized only when it differs from the lifecycle default
 * (STRONG defaults to {@code true}, WEAK defaults to {@code false}), and that round-tripping
 * preserves the effective {@code isFrozen()} value.
 */
public class AssociationSerializationTest {

  private static final ObjectMapper MAPPER = JacksonMapper.INSTANCE;

  private static Association association(LifecyclePolicy lifecycle, Boolean frozen) {
    return new Association(
        "test-subject", "guid-1", "resource", "ns", "id", "topic", "value", lifecycle, frozen);
  }

  private static AssociationInfo info(LifecyclePolicy lifecycle, Boolean frozen) {
    return new AssociationInfo("test-subject", "value", lifecycle, frozen, null);
  }

  private static boolean hasFrozen(Object o) throws IOException {
    JsonNode node = MAPPER.readTree(MAPPER.writeValueAsString(o));
    return node.has("frozen");
  }

  // Association

  @Test
  public void testAssociationStrongFrozenTrueOmitted() throws IOException {
    // STRONG default is true, so frozen=true matches the default and is hidden
    assertFalse(hasFrozen(association(LifecyclePolicy.STRONG, true)));
  }

  @Test
  public void testAssociationStrongFrozenFalseSerialized() throws IOException {
    // STRONG default is true, so frozen=false differs and is shown
    JsonNode node = MAPPER.readTree(
        MAPPER.writeValueAsString(association(LifecyclePolicy.STRONG, false)));
    assertTrue(node.has("frozen"));
    assertFalse(node.get("frozen").asBoolean());
  }

  @Test
  public void testAssociationStrongFrozenNullOmitted() throws IOException {
    // null frozen falls back to the lifecycle default, so it matches and is hidden
    assertFalse(hasFrozen(association(LifecyclePolicy.STRONG, null)));
  }

  @Test
  public void testAssociationWeakFrozenFalseOmitted() throws IOException {
    // WEAK default is false, so frozen=false matches the default and is hidden
    assertFalse(hasFrozen(association(LifecyclePolicy.WEAK, false)));
  }

  @Test
  public void testAssociationWeakFrozenTrueSerialized() throws IOException {
    // WEAK default is false, so frozen=true differs and is shown
    JsonNode node = MAPPER.readTree(
        MAPPER.writeValueAsString(association(LifecyclePolicy.WEAK, true)));
    assertTrue(node.has("frozen"));
    assertTrue(node.get("frozen").asBoolean());
  }

  @Test
  public void testAssociationStrongDefaultRoundTrip() throws IOException {
    // STRONG with frozen omitted from JSON deserializes to an effective frozen of true
    Association deserialized = MAPPER.readValue(
        MAPPER.writeValueAsString(association(LifecyclePolicy.STRONG, true)), Association.class);
    assertTrue(deserialized.isFrozen());
  }

  @Test
  public void testAssociationStrongNonDefaultRoundTrip() throws IOException {
    Association deserialized = MAPPER.readValue(
        MAPPER.writeValueAsString(association(LifecyclePolicy.STRONG, false)), Association.class);
    assertFalse(deserialized.isFrozen());
  }

  @Test
  public void testAssociationWeakDefaultRoundTrip() throws IOException {
    Association deserialized = MAPPER.readValue(
        MAPPER.writeValueAsString(association(LifecyclePolicy.WEAK, false)), Association.class);
    assertFalse(deserialized.isFrozen());
  }

  // AssociationInfo

  @Test
  public void testInfoStrongFrozenTrueOmitted() throws IOException {
    assertFalse(hasFrozen(info(LifecyclePolicy.STRONG, true)));
  }

  @Test
  public void testInfoStrongFrozenFalseSerialized() throws IOException {
    JsonNode node = MAPPER.readTree(MAPPER.writeValueAsString(info(LifecyclePolicy.STRONG, false)));
    assertTrue(node.has("frozen"));
    assertFalse(node.get("frozen").asBoolean());
  }

  @Test
  public void testInfoWeakFrozenFalseOmitted() throws IOException {
    assertFalse(hasFrozen(info(LifecyclePolicy.WEAK, false)));
  }

  @Test
  public void testInfoStrongDefaultRoundTrip() throws IOException {
    AssociationInfo deserialized = MAPPER.readValue(
        MAPPER.writeValueAsString(info(LifecyclePolicy.STRONG, true)), AssociationInfo.class);
    assertTrue(deserialized.isFrozen());
  }

  @Test
  public void testInfoStrongNonDefaultRoundTrip() throws IOException {
    AssociationInfo deserialized = MAPPER.readValue(
        MAPPER.writeValueAsString(info(LifecyclePolicy.STRONG, false)), AssociationInfo.class);
    assertFalse(deserialized.isFrozen());
  }
}
