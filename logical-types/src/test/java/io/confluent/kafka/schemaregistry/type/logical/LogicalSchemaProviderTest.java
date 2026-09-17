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

package io.confluent.kafka.schemaregistry.type.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class LogicalSchemaProviderTest {

  private static final String SUBJECT = "TestSubject-value";
  private static final String DDL = "TYPE STRUCT<name STRING, age INT>";
  private static final String AVRO = "{\"type\":\"record\",\"name\":\"Foo\",\"fields\":"
      + "[{\"name\":\"f1\",\"type\":\"string\"}]}";

  private Schema schema(String schemaType, String body) {
    return new Schema(SUBJECT, null, null, schemaType, Collections.emptyList(), body);
  }

  private ParsedSchema parse(SchemaProvider provider, String body) {
    return provider.parseSchemaOrElseThrow(schema(provider.schemaType(), body), false, false);
  }

  // --- classification ---

  @Test
  void recognizesDdl() {
    assertTrue(LogicalSchemaProvider.isLogical(DDL));
    assertTrue(LogicalSchemaProvider.isLogical("STRUCT User (name STRING, age INT); TYPE User"));
  }

  @Test
  void rejectsNonDdl() {
    assertFalse(LogicalSchemaProvider.isLogical("["));
    assertFalse(LogicalSchemaProvider.isLogical(AVRO));
    assertFalse(LogicalSchemaProvider.isLogical(""));
    assertFalse(LogicalSchemaProvider.isLogical("   \n "));
    assertFalse(LogicalSchemaProvider.isLogical(null));
  }

  // --- native bodies are untouched ---

  @Test
  void schemaTypeIsTheDelegates() {
    assertEquals(AvroSchema.TYPE, new LogicalAvroSchemaProvider().schemaType());
    assertEquals(JsonSchema.TYPE, new LogicalJsonSchemaProvider().schemaType());
    assertEquals(ProtobufSchema.TYPE, new LogicalProtobufSchemaProvider().schemaType());
  }

  @Test
  void parsesNativeSchemaThroughDelegate() {
    ParsedSchema parsed = parse(new LogicalAvroSchemaProvider(), AVRO);
    assertEquals(AvroSchema.TYPE, parsed.schemaType());
    assertEquals("Foo", parsed.name());
  }

  @Test
  void reportsNativeErrorWhenBodyIsNeither() {
    // The Avro failure is the useful one; a DDL parse error here would be misleading.
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> parse(new LogicalAvroSchemaProvider(), "{ not a schema"));
    assertFalse(e instanceof ValidationException, "should surface the native error");
  }

  // --- DDL conversion ---

  @Test
  void convertsDdlToAvro() {
    ParsedSchema parsed = parse(new LogicalAvroSchemaProvider(), DDL);
    assertEquals(AvroSchema.TYPE, parsed.schemaType());
    // The root record is named after the subject, as the registry names it. It is reached through
    // a nullable union, so the name lives in the canonical form rather than on name().
    assertTrue(parsed.canonicalString().contains("\"name\":\"TestSubject_value\""),
        parsed.canonicalString());
  }

  @Test
  void convertsDdlToJson() {
    assertEquals(JsonSchema.TYPE, parse(new LogicalJsonSchemaProvider(), DDL).schemaType());
  }

  @Test
  void convertsDdlToProtobuf() {
    assertEquals(ProtobufSchema.TYPE, parse(new LogicalProtobufSchemaProvider(), DDL).schemaType());
  }

  @Test
  void convertsNamedTypes() {
    ParsedSchema parsed = parse(
        new LogicalAvroSchemaProvider(), "STRUCT User (name STRING, age INT); TYPE User");
    assertEquals(AvroSchema.TYPE, parsed.schemaType());
  }

  @Test
  void rejectsSemanticallyInvalidDdl() {
    // Parses as DDL, so the DDL error is what should surface rather than a native one.
    assertThrows(ValidationException.class,
        () -> parse(new LogicalAvroSchemaProvider(), "STRUCT User (name STRING); TYPE Missing"));
  }

  @Test
  void carriesConversionAndRequestedMetadata() {
    Schema schema = new Schema(SUBJECT, null, null, JsonSchema.TYPE, Collections.emptyList(),
        new Metadata(null, Collections.singletonMap("owner", "payments"), null), null, DDL);
    ParsedSchema parsed =
        new LogicalJsonSchemaProvider().parseSchemaOrElseThrow(schema, false, false);

    // Merged, not replaced -- the same result the registry produces for the same body.
    assertEquals("payments", parsed.metadata().getProperties().get("owner"));
    assertEquals("2", parsed.metadata().getProperties().get("confluent:edition"));
  }

  @Test
  void jsonAcceptsExternalImports() {
    // External imports are a JSON construct: they name a target an FQN cannot address, and the
    // JSON writer emits the URI as the reference. Only Avro and Protobuf, which cannot express
    // one, reject them -- so the target format decides, exactly as the registry lets it.
    ParsedSchema parsed = parse(new LogicalJsonSchemaProvider(),
        "USING TYPE Ext FOR REF 'http://example.com/ext.json'; TYPE STRUCT<f Ext>");
    assertTrue(parsed.canonicalString().contains("http://example.com/ext.json"),
        parsed.canonicalString());
  }

  @Test
  void avroRejectsExternalImports() {
    // External imports are a JSON-only construct, so the target format decides, not this provider.
    ValidationException e = assertThrows(ValidationException.class,
        () -> parse(new LogicalAvroSchemaProvider(),
            "USING TYPE Ext FOR REF 'http://example.com/ext.json'; TYPE STRUCT<f Ext>"));
    assertTrue(e.getMessage().contains("external imports"), e.getMessage());
  }

  @Test
  void resolvesReferences() {
    Schema referenced = new Schema("Address-value", 1, 1, AvroSchema.TYPE,
        Collections.emptyList(), "{\"type\":\"record\",\"name\":\"Address\",\"fields\":"
        + "[{\"name\":\"street\",\"type\":\"string\"}]}");
    SchemaProvider provider = new LogicalAvroSchemaProvider();
    provider.configure(Collections.singletonMap(
        SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, fetcherFor(referenced)));

    Schema schema = new Schema(SUBJECT, null, null, AvroSchema.TYPE,
        Collections.singletonList(new SchemaReference("Address", "Address-value", 1)),
        "TYPE STRUCT<home Address>");
    ParsedSchema parsed = provider.parseSchemaOrElseThrow(schema, false, false);

    assertEquals(AvroSchema.TYPE, parsed.schemaType());
    assertTrue(parsed.canonicalString().contains("Address"), parsed.canonicalString());
  }

  private SchemaVersionFetcher fetcherFor(Schema referenced) {
    return new SchemaVersionFetcher() {
      @Override
      public String tenant() {
        return QualifiedSubject.DEFAULT_TENANT;
      }

      @Override
      public Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
        return referenced.getSubject().equals(subject) ? referenced : null;
      }
    };
  }

  @Test
  void namesRootFromDefaultWhenSubjectIsAbsent() {
    // The entry points that take a bare schema string carry no subject to name the root after.
    SchemaProvider provider = new LogicalAvroSchemaProvider();
    ParsedSchema parsed = provider.parseSchema(DDL, Collections.emptyList()).orElseThrow();
    assertTrue(parsed.canonicalString().contains("\"name\":\"Envelope\""),
        parsed.canonicalString());
  }
}
