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

import org.apache.avro.SchemaParseException;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class AvroSchemaValidateNamesTest {

  // isNew=false path -> NO_VALIDATION + validateNames -> strict names, lax namespaces.

  @Test
  public void laxNamespaceAcceptedWhenNotNew() {
    String schema = "{\"type\":\"record\",\"name\":\"MyRec\",\"namespace\":\"foo-bar.x\","
        + "\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}";
    AvroSchema parsed = new AvroSchema(schema, Collections.emptyList(),
        Collections.emptyMap(), null, false);
    assertNotNull(parsed.rawSchema());
    assertEquals("foo-bar.x.MyRec", parsed.rawSchema().getFullName());
  }

  @Test
  public void laxNamespaceInFullNameAcceptedWhenNotNew() {
    String schema = "{\"type\":\"record\",\"name\":\"foo-bar.x.MyRec\","
        + "\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}";
    AvroSchema parsed = new AvroSchema(schema, Collections.emptyList(),
        Collections.emptyMap(), null, false);
    assertEquals("foo-bar.x.MyRec", parsed.rawSchema().getFullName());
  }

  @Test
  public void badRecordNameRejectedWhenNotNew() {
    String schema = "{\"type\":\"record\",\"name\":\"bad-name\","
        + "\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}";
    assertThrows(SchemaParseException.class,
        () -> new AvroSchema(schema, Collections.emptyList(),
            Collections.emptyMap(), null, false));
  }

  @Test
  public void badFieldNameRejectedWhenNotNew() {
    String schema = "{\"type\":\"record\",\"name\":\"MyRec\","
        + "\"fields\":[{\"name\":\"bad-field\",\"type\":\"string\"}]}";
    assertThrows(SchemaParseException.class,
        () -> new AvroSchema(schema, Collections.emptyList(),
            Collections.emptyMap(), null, false));
  }

  @Test
  public void badEnumSymbolRejectedWhenNotNew() {
    String schema = "{\"type\":\"record\",\"name\":\"MyRec\",\"fields\":["
        + "{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\","
        + "\"symbols\":[\"OK\",\"BAD-ONE\"]}}]}";
    assertThrows(SchemaParseException.class,
        () -> new AvroSchema(schema, Collections.emptyList(),
            Collections.emptyMap(), null, false));
  }

  @Test
  public void badNestedRecordNameRejectedWhenNotNew() {
    String schema = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"bad-inner\","
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}}]}";
    assertThrows(SchemaParseException.class,
        () -> new AvroSchema(schema, Collections.emptyList(),
            Collections.emptyMap(), null, false));
  }

  @Test
  public void badFixedNameRejectedWhenNotNew() {
    String schema = "{\"type\":\"record\",\"name\":\"MyRec\",\"fields\":["
        + "{\"name\":\"f\",\"type\":{\"type\":\"fixed\",\"name\":\"bad-fixed\",\"size\":4}}]}";
    assertThrows(SchemaParseException.class,
        () -> new AvroSchema(schema, Collections.emptyList(),
            Collections.emptyMap(), null, false));
  }

  @Test
  public void validSchemaAcceptedWhenNotNew() {
    String schema = "{\"type\":\"record\",\"name\":\"MyRec\",\"namespace\":\"a.b\","
        + "\"fields\":[{\"name\":\"f\",\"type\":\"string\"},"
        + "{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\","
        + "\"symbols\":[\"A\",\"B\"]}}]}";
    AvroSchema parsed = new AvroSchema(schema, Collections.emptyList(),
        Collections.emptyMap(), null, false);
    assertEquals("a.b.MyRec", parsed.rawSchema().getFullName());
  }

  // isNew=true path uses Avro's STRICT_VALIDATOR, so namespaces are still rejected.

  @Test
  public void laxNamespaceRejectedWhenNew() {
    String schema = "{\"type\":\"record\",\"name\":\"MyRec\",\"namespace\":\"foo-bar.x\","
        + "\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}";
    assertThrows(SchemaParseException.class,
        () -> new AvroSchema(schema, Collections.emptyList(),
            Collections.emptyMap(), null, true));
  }

  @Test
  public void validSchemaAcceptedWhenNew() {
    String schema = "{\"type\":\"record\",\"name\":\"MyRec\",\"namespace\":\"a.b\","
        + "\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}";
    AvroSchema parsed = new AvroSchema(schema, Collections.emptyList(),
        Collections.emptyMap(), null, true);
    assertEquals("a.b.MyRec", parsed.rawSchema().getFullName());
  }

  @Test
  public void validateNamesIgnoresUnnamedTypes() {
    AvroSchemaUtils.validateNames(org.apache.avro.Schema.create(
        org.apache.avro.Schema.Type.STRING));
    AvroSchemaUtils.validateNames(org.apache.avro.Schema.createArray(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)));
  }

  @Test
  public void validateNamesDirectOnParsedSchema() {
    org.apache.avro.Schema ok = new org.apache.avro.Schema.Parser(
        org.apache.avro.NameValidator.NO_VALIDATION)
        .parse("{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"a-b.c\","
            + "\"fields\":[{\"name\":\"f\",\"type\":\"int\"}]}");
    AvroSchemaUtils.validateNames(ok);

    org.apache.avro.Schema bad = new org.apache.avro.Schema.Parser(
        org.apache.avro.NameValidator.NO_VALIDATION)
        .parse("{\"type\":\"record\",\"name\":\"R\","
            + "\"fields\":[{\"name\":\"bad-field\",\"type\":\"int\"}]}");
    assertThrows(SchemaParseException.class, () -> AvroSchemaUtils.validateNames(bad));
  }
}
