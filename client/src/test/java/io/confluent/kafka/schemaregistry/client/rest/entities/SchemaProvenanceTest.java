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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SchemaProvenanceTest {

  private static final ObjectMapper MAPPER = JacksonMapper.INSTANCE;

  @Test
  public void serializesToTheDocumentedShape() throws Exception {
    SchemaProvenance provenance = new SchemaProvenance("orders-value", Arrays.asList(
        new ProvenanceVersion(1, 1001, Arrays.asList(
            field(path(0), names("id"), 1, null),
            field(path(1), names("name"), 2, null),
            field(path(2), names("region"), 3, null))),
        new ProvenanceVersion(2, 1002, Arrays.asList(
            field(path(0), names("id"), 1, null),
            field(path(1), names("full_name"), 2, null),
            field(path(2), names("tier"), 4, "standard")))));

    assertEquals(MAPPER.readTree(
        "{\"subject\":\"orders-value\",\"versions\":["
            + "{\"version\":1,\"id\":1001,\"fields\":["
            + "{\"path\":[0],\"names\":[\"id\"],\"pid\":1},"
            + "{\"path\":[1],\"names\":[\"name\"],\"pid\":2},"
            + "{\"path\":[2],\"names\":[\"region\"],\"pid\":3}]},"
            + "{\"version\":2,\"id\":1002,\"fields\":["
            + "{\"path\":[0],\"names\":[\"id\"],\"pid\":1},"
            + "{\"path\":[1],\"names\":[\"full_name\"],\"pid\":2},"
            + "{\"path\":[2],\"names\":[\"tier\"],\"pid\":4,\"default\":\"standard\"}]}]}"),
        MAPPER.valueToTree(provenance));
  }

  @Test
  public void namesAreOmittedWhenNotVerbose() {
    JsonNode json = MAPPER.valueToTree(field(path(0), null, 1, null));
    assertFalse(json.has("names"));
  }

  @Test
  public void anEmptyDefaultIsStillADefault() {
    // Protobuf's implicit defaults are exactly these; dropping them as "empty" would make them
    // indistinguishable from having no default.
    assertEquals("", MAPPER.valueToTree(field(path(0), null, 1, "")).get("default").asText());
    assertTrue(MAPPER.valueToTree(field(path(0), null, 1, Collections.emptyList()))
        .get("default").isArray());
    assertFalse(MAPPER.valueToTree(field(path(0), null, 1, null)).has("default"));
  }

  @Test
  public void aVersionWithNoMembersSaysSo() {
    JsonNode json = MAPPER.valueToTree(new ProvenanceVersion(1, 1001, Collections.emptyList()));
    assertTrue(json.get("fields").isArray());
    assertEquals(0, json.get("fields").size());
  }

  @Test
  public void roundTripsAndIgnoresUnknownProperties() throws Exception {
    String json = "{\"subject\":\"s\",\"future\":true,\"versions\":["
        + "{\"version\":3,\"id\":7,\"fields\":["
        + "{\"path\":[0,1],\"pid\":2,\"default\":0,\"alsoFuture\":1}]}]}";
    SchemaProvenance read = MAPPER.readValue(json, SchemaProvenance.class);

    assertEquals(new SchemaProvenance("s", Collections.singletonList(
        new ProvenanceVersion(3, 7, Collections.singletonList(
            field(path(0, 1), null, 2, 0))))), read);
  }

  // -------------------------------------------------------------------------------------------
  // ProvenanceDefaults
  // -------------------------------------------------------------------------------------------

  @Test
  public void scalarsPassThrough() {
    assertEquals("x", ProvenanceDefaults.encode("x"));
    assertEquals(true, ProvenanceDefaults.encode(true));
    assertEquals(7, ProvenanceDefaults.encode(7));
    assertEquals(Long.MAX_VALUE, ProvenanceDefaults.encode(Long.MAX_VALUE));
    assertEquals(1.5d, ProvenanceDefaults.encode(1.5d));
    assertEquals(null, ProvenanceDefaults.encode(null));
  }

  @Test
  public void nonFiniteFloatsHaveNamesBecauseJsonHasNoNumberForThem() {
    assertEquals("NaN", ProvenanceDefaults.encode(Double.NaN));
    assertEquals("Infinity", ProvenanceDefaults.encode(Float.POSITIVE_INFINITY));
    assertEquals("-Infinity", ProvenanceDefaults.encode(Double.NEGATIVE_INFINITY));
  }

  @Test
  public void aDecimalIsAPlainStringNeverExponentNotation() {
    assertEquals("1000", ProvenanceDefaults.encode(new BigDecimal("1E+3")));
    assertEquals("0.00", ProvenanceDefaults.encode(new BigDecimal("0.00")));
  }

  @Test
  public void temporalsKeepEveryFractionalDigitTheyNeed() {
    assertEquals("2026-01-01", ProvenanceDefaults.encode(LocalDate.of(2026, 1, 1)));
    assertEquals("14:30:00", ProvenanceDefaults.encode(LocalTime.of(14, 30)));
    assertEquals("14:30:00.123456",
        ProvenanceDefaults.encode(LocalTime.of(14, 30, 0, 123_456_000)));
    assertEquals("2026-01-01T14:30:00.123",
        ProvenanceDefaults.encode(LocalDateTime.of(2026, 1, 1, 14, 30, 0, 123_000_000)));
    assertEquals("2026-01-01T14:30:00.123456789Z",
        ProvenanceDefaults.encode(Instant.parse("2026-01-01T14:30:00.123456789Z")));
    assertEquals("2026-01-01T00:00:00Z",
        ProvenanceDefaults.encode(Instant.parse("2026-01-01T00:00:00Z")));
  }

  @Test
  public void bytesAreBase64() {
    assertEquals("AQID", ProvenanceDefaults.encode(new byte[] {1, 2, 3}));
  }

  @Test
  public void collectionsEncodeTheirElements() {
    assertEquals(Arrays.asList("0.5", "2026-01-01"), ProvenanceDefaults.encode(
        Arrays.asList(new BigDecimal("0.5"), LocalDate.of(2026, 1, 1))));

    Map<String, Object> stringKeys = new LinkedHashMap<>();
    stringKeys.put("a", new BigDecimal("1.0"));
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("a", "1.0");
    assertEquals(expected, ProvenanceDefaults.encode(stringKeys));
  }

  @Test
  public void aMapWithNonStringKeysBecomesPairs() {
    Map<Integer, String> intKeys = new LinkedHashMap<>();
    intKeys.put(1, "one");
    Map<String, Object> pair = new LinkedHashMap<>();
    pair.put("key", 1);
    pair.put("value", "one");
    assertEquals(Collections.singletonList(pair), ProvenanceDefaults.encode(intKeys));
  }

  @Test
  public void anUnknownTypeIsRejectedNotStringified() {
    assertThrows(IllegalArgumentException.class, () -> ProvenanceDefaults.encode(new Object()));
  }

  private static ProvenanceField field(
      List<Integer> path, List<String> names, int id, Object defaultValue) {
    return new ProvenanceField(path, names, id, defaultValue);
  }

  private static List<Integer> path(Integer... steps) {
    return Arrays.asList(steps);
  }

  private static List<String> names(String... names) {
    return Arrays.asList(names);
  }
}
