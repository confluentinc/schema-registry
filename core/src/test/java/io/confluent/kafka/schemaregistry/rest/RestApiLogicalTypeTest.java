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

package io.confluent.kafka.schemaregistry.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Int64Value;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Registers schemas written as logical types DDL and reads them back, over a live registry.
 */
@Tag("IntegrationTest")
public abstract class RestApiLogicalTypeTest {

  private static final String LOGICAL = "logical";

  protected RestApp restApp = null;

  public void setRestApp(RestApp restApp) {
    this.restApp = restApp;
  }

  /**
   * A union's branch names survive only in metadata. Avro has no way to name a union member, so
   * {@code ["string","int"]} alone cannot say which branch was {@code mystr} and which was
   * {@code myint} -- the conversion records that under {@code confluent:union}, and reading the
   * schema back as DDL reconstructs the branches from it. Losing that metadata on the way into
   * storage would leave a union that still registers and still reads, but comes back naming its
   * branches something else.
   */
  @Test
  public void testAvroUnionRoundTripsThroughLogicalFormat() throws Exception {
    String subject = "union-value";
    String ddl = "TYPE UNION(mystr STRING, myint INT)";

    restApp.restClient.registerSchema(ddl, AvroSchema.TYPE, Collections.emptyList(), subject);

    // Stored natively: the registry converts the DDL, so what it holds is Avro, not the script.
    Schema stored = restApp.restClient.getVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, 1);
    assertEquals(AvroSchema.TYPE, stored.getSchemaType());
    assertTrue(stored.getSchema().contains("\"string\""), stored.getSchema());
    assertTrue(stored.getSchema().contains("\"int\""), stored.getSchema());

    assertNotNull(stored.getMetadata(), "the conversion's metadata must be stored");
    assertTrue(stored.getMetadata().getProperties().containsKey("confluent:union"),
        "branch identity lives here: " + stored.getMetadata().getProperties());

    // Read back as DDL: the branch names come from the metadata, not from the Avro text.
    Schema logical = restApp.restClient.getVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, 1, LOGICAL, false, null);
    assertTrue(logical.getSchema().contains("UNION"), logical.getSchema());
    assertTrue(logical.getSchema().contains("mystr"), logical.getSchema());
    assertTrue(logical.getSchema().contains("myint"), logical.getSchema());
  }

  /**
   * The declared schema type picks the format a DDL body is converted to, and it is matched
   * without regard to case.
   */
  @Test
  public void testLowerCaseSchemaTypeStillConverts() throws Exception {
    String subject = "lowercase-value";

    restApp.restClient.registerSchema(
        "TYPE STRUCT(id STRING)", "avro", Collections.emptyList(), subject);

    Schema stored = restApp.restClient.getVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, 1);
    assertEquals(AvroSchema.TYPE, stored.getSchemaType());
    assertTrue(stored.getSchema().contains("\"record\""), stored.getSchema());
  }

  /**
   * A DDL body is recognized by its own syntax, so a native schema submitted under the same
   * schemaType must still be stored as it was written.
   */
  @Test
  public void testAvroSchemaIsNotTreatedAsLogical() throws Exception {
    String subject = "native-value";
    String avro = "{\"type\":\"record\",\"name\":\"Widget\",\"fields\":"
        + "[{\"name\":\"id\",\"type\":\"string\"}]}";

    restApp.restClient.registerSchema(avro, AvroSchema.TYPE, Collections.emptyList(), subject);

    Schema stored = restApp.restClient.getVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, 1);
    assertEquals(AvroSchema.TYPE, stored.getSchemaType());
    assertTrue(stored.getSchema().contains("Widget"), stored.getSchema());
  }

  /**
   * A primitive Protobuf root is stored as an empty file publicly importing wrappers.proto. A
   * generated wrapper class's schema, the whole of wrappers.proto, must still find it, whether
   * looked up or registered.
   */
  @Test
  public void testGeneratedWrapperFindsAProtobufPrimitiveRoot() throws Exception {
    String subject = "bigint-key";
    int id = restApp.restClient.registerSchema(
        "TYPE BIGINT", ProtobufSchema.TYPE, Collections.emptyList(), subject).getId();

    String generated = new ProtobufSchema(Int64Value.getDescriptor()).canonicalString();
    // Normalized, the lookup still finds it, though wrappers.proto is stored only in its raw form
    assertEquals(id, (int) restApp.restClient.lookUpSubjectVersion(
        generated, ProtobufSchema.TYPE, Collections.emptyList(), subject, true, false).getId());
    assertFoundAsVersionOne(id, generated, Collections.emptyList(), subject);
  }

  /**
   * An external Protobuf root is stored as an empty file publicly importing the referenced file.
   * A schema made from the root's generated class, with the references that file was registered
   * with, must still find it.
   */
  @Test
  public void testGeneratedClassFindsAProtobufExternalRoot() throws Exception {
    assertGeneratedClassFindsAProtobufExternalRoot("");
  }

  /**
   * In a named context, the references resolve within it, and so must the lookup's comparison.
   */
  @Test
  public void testGeneratedClassFindsAProtobufExternalRootInAContext() throws Exception {
    assertGeneratedClassFindsAProtobufExternalRoot(":.ctx:");
  }

  private void assertGeneratedClassFindsAProtobufExternalRoot(String context) throws Exception {
    String other = "syntax = \"proto3\";\npackage com;\nmessage Other {\n  int32 n = 1;\n}\n";
    String leaf = "syntax = \"proto3\";\npackage com;\nimport \"other.proto\";\n"
        + "message Foo {\n  string id = 1;\n}\nmessage Bar {\n  com.Other o = 1;\n}\n";
    List<SchemaReference> otherRef =
        Collections.singletonList(new SchemaReference("other.proto", "other", 1));
    restApp.restClient.registerSchema(
        other, ProtobufSchema.TYPE, Collections.emptyList(), context + "other");
    restApp.restClient.registerSchema(leaf, ProtobufSchema.TYPE, otherRef, context + "leaf");

    String subject = context + "bar-value";
    int id = restApp.restClient.registerSchema("TYPE com.Bar NOT NULL", ProtobufSchema.TYPE,
        Collections.singletonList(new SchemaReference("com.Bar", "leaf", 1)), subject).getId();
    Schema stored = restApp.restClient.getVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, 1);
    assertTrue(stored.getSchema().contains("import public"), stored.getSchema());

    assertFoundAsVersionOne(id, leaf, otherRef, subject);
  }

  private void assertFoundAsVersionOne(
      int id, String schema, List<SchemaReference> references, String subject) throws Exception {
    assertEquals(id, (int) restApp.restClient.lookUpSubjectVersion(
        schema, ProtobufSchema.TYPE, references, subject, false).getId());
    assertEquals(id, restApp.restClient.registerSchema(
        schema, ProtobufSchema.TYPE, references, subject).getId());
    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(subject));
  }
}
