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

package io.confluent.kafka.schemaregistry.avro;

import org.apache.avro.AvroTypeException;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

// The Avro spec says the default value for a union field must match the first
// branch of the union.  Avro's own parser validation (setValidateDefaults)
// only requires the default to match some branch, so these tests pin down the
// stricter first-branch rule for schemas parsed as new (validateAsNew=true).
public class AvroSchemaValidateDefaultsTest {

  private static AvroSchema parseAsNew(String schema) {
    return new AvroSchema(schema, Collections.emptyList(),
        Collections.emptyMap(), null, true);
  }

  private static String recordWithField(String fieldType, String defaultJson) {
    return "{\"type\":\"record\",\"name\":\"Example\",\"fields\":[{\"name\":\"f\","
        + "\"type\":" + fieldType + ",\"default\":" + defaultJson + "}]}";
  }

  // The repro from issue #4112: ["int", "null"] with a null default must be
  // rejected because null does not match the first branch (int).
  @Test
  public void nullDefaultForIntFirstUnionRejected() {
    String schema = "{\"type\":\"record\",\"namespace\":\"ru.rudikov.example\","
        + "\"name\":\"Example\",\"fields\":[{\"name\":\"Age\","
        + "\"type\":[\"int\",\"null\"],\"default\":null}]}";
    assertThrows(AvroTypeException.class, () -> parseAsNew(schema));
  }

  @Test
  public void defaultMatchingOnlySecondBranchRejected() {
    assertThrows(AvroTypeException.class,
        () -> parseAsNew(recordWithField("[\"string\",\"int\"]", "5")));
  }

  @Test
  public void stringDefaultForIntFirstUnionRejected() {
    assertThrows(AvroTypeException.class,
        () -> parseAsNew(recordWithField("[\"int\",\"string\"]", "\"hello\"")));
  }

  @Test
  public void nullDefaultForArrayFirstUnionRejected() {
    assertThrows(AvroTypeException.class,
        () -> parseAsNew(recordWithField(
            "[{\"type\":\"array\",\"items\":\"int\"},\"null\"]", "null")));
  }

  @Test
  public void badUnionDefaultInNestedRecordRejected() {
    String schema = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"Inner\","
        + "\"fields\":[{\"name\":\"g\",\"type\":[\"int\",\"null\"],\"default\":null}]}}]}";
    assertThrows(AvroTypeException.class, () -> parseAsNew(schema));
  }

  @Test
  public void recordDefaultWithBadUnionValueRejected() {
    String schema = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"f\",\"type\":{\"type\":\"record\",\"name\":\"Inner\","
        + "\"fields\":[{\"name\":\"g\",\"type\":[\"int\",\"null\"]}]},"
        + "\"default\":{\"g\":null}}]}";
    assertThrows(AvroTypeException.class, () -> parseAsNew(schema));
  }

  @Test
  public void arrayOfUnionDefaultElementRejected() {
    assertThrows(AvroTypeException.class,
        () -> parseAsNew(recordWithField(
            "{\"type\":\"array\",\"items\":[\"int\",\"null\"]}", "[5,null]")));
  }

  @Test
  public void mapOfUnionDefaultValueRejected() {
    assertThrows(AvroTypeException.class,
        () -> parseAsNew(recordWithField(
            "{\"type\":\"map\",\"values\":[\"int\",\"null\"]}", "{\"a\":null}")));
  }

  @Test
  public void unionOfRecordsDefaultMatchingSecondRecordRejected() {
    String schema = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"f\",\"type\":["
        + "{\"type\":\"record\",\"name\":\"R1\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]},"
        + "{\"type\":\"record\",\"name\":\"R2\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"}]}],"
        + "\"default\":{\"b\":\"x\"}}]}";
    assertThrows(AvroTypeException.class, () -> parseAsNew(schema));
  }

  @Test
  public void recursiveRecordWithBadUnionDefaultRejected() {
    String schema = "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"v\",\"type\":[\"int\",\"null\"],\"default\":null},"
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}";
    assertThrows(AvroTypeException.class, () -> parseAsNew(schema));
  }

  @Test
  public void nullDefaultForNullFirstUnionAccepted() {
    assertNotNull(parseAsNew(recordWithField("[\"null\",\"int\"]", "null"))
        .rawSchema());
  }

  @Test
  public void intDefaultForIntFirstUnionAccepted() {
    assertNotNull(parseAsNew(recordWithField("[\"int\",\"null\"]", "5"))
        .rawSchema());
  }

  @Test
  public void intDefaultForLongFirstUnionAccepted() {
    assertNotNull(parseAsNew(recordWithField("[\"long\",\"null\"]", "5"))
        .rawSchema());
  }

  @Test
  public void stringDefaultForStringFirstUnionAccepted() {
    assertNotNull(parseAsNew(recordWithField("[\"string\",\"int\"]", "\"hello\""))
        .rawSchema());
  }

  @Test
  public void emptyArrayDefaultForArrayFirstUnionAccepted() {
    assertNotNull(parseAsNew(recordWithField(
        "[{\"type\":\"array\",\"items\":\"int\"},\"null\"]", "[]")).rawSchema());
  }

  @Test
  public void arrayOfUnionValidDefaultAccepted() {
    assertNotNull(parseAsNew(recordWithField(
        "{\"type\":\"array\",\"items\":[\"int\",\"null\"]}", "[1,2]")).rawSchema());
  }

  @Test
  public void mapOfUnionValidDefaultAccepted() {
    assertNotNull(parseAsNew(recordWithField(
        "{\"type\":\"map\",\"values\":[\"int\",\"null\"]}", "{\"a\":3}"))
        .rawSchema());
  }

  @Test
  public void unionOfRecordsDefaultMatchingFirstRecordAccepted() {
    String schema = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"f\",\"type\":["
        + "{\"type\":\"record\",\"name\":\"R1\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]},"
        + "{\"type\":\"record\",\"name\":\"R2\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"}]}],"
        + "\"default\":{\"a\":1}}]}";
    assertNotNull(parseAsNew(schema).rawSchema());
  }

  @Test
  public void recordDefaultFallingBackToFieldDefaultsAccepted() {
    String schema = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"f\",\"type\":{\"type\":\"record\",\"name\":\"Inner\","
        + "\"fields\":[{\"name\":\"g\",\"type\":[\"int\",\"null\"],\"default\":7}]},"
        + "\"default\":{}}]}";
    assertNotNull(parseAsNew(schema).rawSchema());
  }

  @Test
  public void recursiveRecordWithValidDefaultsAccepted() {
    String schema = "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"v\",\"type\":\"int\",\"default\":0},"
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}";
    assertNotNull(parseAsNew(schema).rawSchema());
  }

  @Test
  public void unionFieldWithoutDefaultAccepted() {
    String schema = "{\"type\":\"record\",\"name\":\"Example\",\"fields\":["
        + "{\"name\":\"f\",\"type\":[\"int\",\"null\"]}]}";
    assertNotNull(parseAsNew(schema).rawSchema());
  }

  // Schemas that are not new keep the historical lax behavior, so that
  // already-registered schemas with such defaults can still be read back.
  @Test
  public void badUnionDefaultAcceptedWhenNotNew() {
    String schema = "{\"type\":\"record\",\"name\":\"Example\",\"fields\":["
        + "{\"name\":\"Age\",\"type\":[\"int\",\"null\"],\"default\":null}]}";
    AvroSchema parsed = new AvroSchema(schema, Collections.emptyList(),
        Collections.emptyMap(), null, false);
    assertNotNull(parsed.rawSchema());
  }
}
