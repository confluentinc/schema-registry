/*
 * Copyright 2020 Confluent Inc.
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
 *
 */

package io.confluent.kafka.schemaregistry.rest.protobuf;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTags;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.utils.ResourceLoader;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.serializers.protobuf.test.Root;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

public class RestApiTest extends ClusterTestHarness {

  private static final Random random = new Random();

  public RestApiTest() {
    super(1, true);
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty("schema.providers", ProtobufSchemaProvider.class.getName());
    return props;
  }

  @Test
  public void testBasic() throws Exception {
    String subject1 = "testTopic1";
    String subject2 = "testTopic2";
    int schemasInSubject1 = 10;
    List<Integer> allVersionsInSubject1 = new ArrayList<Integer>();
    List<String> allSchemasInSubject1 = getRandomProtobufSchemas(schemasInSubject1);
    int schemasInSubject2 = 5;
    List<Integer> allVersionsInSubject2 = new ArrayList<Integer>();
    List<String> allSchemasInSubject2 = getRandomProtobufSchemas(schemasInSubject2);
    List<String> allSubjects = new ArrayList<String>();

    // test getAllSubjects with no existing data
    assertEquals("Getting all subjects should return empty",
        allSubjects,
        restApp.restClient.getAllSubjects()
    );

    // test registering and verifying new schemas in subject1
    int schemaIdCounter = 1;
    for (int i = 0; i < schemasInSubject1; i++) {
      String schema = allSchemasInSubject1.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter, subject1);
      schemaIdCounter++;
      allVersionsInSubject1.add(expectedVersion);
    }
    allSubjects.add(subject1);

    // test re-registering existing schemas
    for (int i = 0; i < schemasInSubject1; i++) {
      int expectedId = i + 1;
      String schemaString = allSchemasInSubject1.get(i);
      int foundId = restApp.restClient.registerSchema(schemaString,
          ProtobufSchema.TYPE,
          Collections.emptyList(),
          subject1
      ).getId();
      assertEquals("Re-registering an existing schema should return the existing version",
          expectedId,
          foundId
      );
    }

    // test registering schemas in subject2
    for (int i = 0; i < schemasInSubject2; i++) {
      String schema = allSchemasInSubject2.get(i);
      int expectedVersion = i + 1;
      registerAndVerifySchema(restApp.restClient, schema, schemaIdCounter, subject2);
      schemaIdCounter++;
      allVersionsInSubject2.add(expectedVersion);
    }
    allSubjects.add(subject2);

    // test getAllVersions with existing data
    assertEquals(
        "Getting all versions from subject1 should match all registered versions",
        allVersionsInSubject1,
        restApp.restClient.getAllVersions(subject1)
    );
    assertEquals(
        "Getting all versions from subject2 should match all registered versions",
        allVersionsInSubject2,
        restApp.restClient.getAllVersions(subject2)
    );

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
        allSubjects,
        restApp.restClient.getAllSubjects()
    );
  }

  @Test
  public void testSchemaReferences() throws Exception {
    Map<String, String> schemas = getProtobufSchemaWithDependencies();
    String subject = "confluent/meta.proto";
    registerAndVerifySchema(restApp.restClient, schemas.get("confluent/meta.proto"), 1, subject);
    subject = "reference";
    registerAndVerifySchema(restApp.restClient, schemas.get("ref.proto"), 2, subject);

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get("root.proto"));
    request.setSchemaType(ProtobufSchema.TYPE);
    SchemaReference ref = new SchemaReference("ref.proto", "reference", 1);
    SchemaReference meta = new SchemaReference("confluent/meta.proto", "confluent/meta.proto", 1);
    List<SchemaReference> refs = Arrays.asList(ref, meta);
    request.setReferences(refs);
    int registeredId = restApp.restClient.registerSchema(request, "referrer", false).getId();
    assertEquals("Registering a new schema should succeed", 3, registeredId);

    SchemaString schemaString = restApp.restClient.getId(3);
    // the newly registered schema should be immediately readable on the leader
    assertEquals("Registered schema should be found",
        schemas.get("root.proto"),
        schemaString.getSchemaString()
    );

    assertEquals("Schema dependencies should be found",
        refs,
        schemaString.getReferences()
    );

    Root.ReferrerMessage referrer = Root.ReferrerMessage.newBuilder().build();
    ProtobufSchema schema = ProtobufSchemaUtils.getSchema(referrer);
    schema = schema.copy(refs);
    Schema registeredSchema = restApp.restClient.lookUpSubjectVersion(schema.canonicalString(),
            ProtobufSchema.TYPE, schema.references(), "referrer", false);
    assertEquals("Registered schema should be found", 3, registeredSchema.getId().intValue());
  }

  @Test
  public void testSchemaReferencesPkg() throws Exception {
    String msg1 = "syntax = \"proto3\";\n" +
        "package pkg1;\n" +
        "\n" +
        "option go_package = \"pkg1pb\";\n" +
        "option java_multiple_files = true;\n" +
        "option java_outer_classname = \"Msg1Proto\";\n" +
        "option java_package = \"com.pkg1\";\n" +
        "\n" +
        "message Message1 {\n" +
        "  string s = 1;\n" +
        "}\n";
    String subject = "pkg1/msg1.proto";
    registerAndVerifySchema(restApp.restClient, msg1, 1, subject);
    subject = "pkg2/msg2.proto";
    String msg2 = "syntax = \"proto3\";\n" +
        "package pkg2;\n" +
        "\n" +
        "option go_package = \"pkg2pb\";\n" +
        "option java_multiple_files = true;\n" +
        "option java_outer_classname = \"Msg2Proto\";\n" +
        "option java_package = \"com.pkg2\";\n" +
        "\n" +
        "import \"pkg1/msg1.proto\";\n" +
        "\n" +
        "message Message2 {\n" +
        "  map<string, pkg1.Message1> map = 1;\n" +
        "  pkg1.Message1 f2 = 2;\n" +
        "}\n";
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(msg2);
    request.setSchemaType(ProtobufSchema.TYPE);
    SchemaReference meta = new SchemaReference("pkg1/msg1.proto", "pkg1/msg1.proto", 1);
    List<SchemaReference> refs = Arrays.asList(meta);
    request.setReferences(refs);
    int registeredId = restApp.restClient.registerSchema(request, subject, false).getId();
    assertEquals("Registering a new schema should succeed", 2, registeredId);
  }

  @Test(expected = RestClientException.class)
  public void testSchemaMissingReferences() throws Exception {
    Map<String, String> schemas = getProtobufSchemaWithDependencies();

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get("root.proto"));
    request.setSchemaType(ProtobufSchema.TYPE);
    request.setReferences(Collections.emptyList());
    restApp.restClient.registerSchema(request, "referrer", false);
  }

  @Test
  public void testIncompatibleSchema() throws Exception {
    String subject = "testSubject";

    // Make two incompatible schemas - field 'myField2' has different types
    String schema1String = "syntax = \"proto3\";\n" +
                             "package pkg3;\n" +
                             "\n" +
                             "message Schema1 {\n" +
                             "  string f1 = 1;\n" +
                             "  string f2 = 2;\n" +
                             "}\n";

    RegisterSchemaRequest registerRequest = new RegisterSchemaRequest();
    registerRequest.setSchema(schema1String);
    registerRequest.setSchemaType(ProtobufSchema.TYPE);

    String schema2String = "syntax = \"proto3\";\n" +
                             "package pkg3;\n" +
                             "\n" +
                             "message Schema1 {\n" +
                             "  string f1 = 1;\n" +
                             "  int32 f2 = 2;\n" +
                             "}\n";

    // ensure registering incompatible schemas will raise an error
    restApp.restClient.updateCompatibility(
      CompatibilityLevel.FULL.name, subject);

    // test that compatibility check for incompatible schema returns false and the appropriate
    // error response from Avro
    int idOfRegisteredSchema1Subject1 = restApp.restClient.registerSchema(registerRequest, subject, true).getId();

    try {
      registerRequest.setSchema(schema2String);
      registerRequest.setSchemaType(ProtobufSchema.TYPE);
      restApp.restClient.registerSchema(registerRequest, subject, true);
      fail("Registering incompatible schema should fail with "
             + Errors.INCOMPATIBLE_SCHEMA_ERROR_CODE);
    } catch (RestClientException e) {
      assertTrue(e.getMessage().length() > 0);
      assertTrue(e.getMessage().contains("oldSchemaVersion:"));
      assertTrue(e.getMessage().contains("oldSchema:"));
      assertTrue(e.getMessage().contains("compatibility:"));
    }

    List<String> response = restApp.restClient.testCompatibility(registerRequest, subject,
      String.valueOf(
        idOfRegisteredSchema1Subject1),
      false,
      true);
    assertTrue(response.size() > 0);
    assertTrue(response.get(2).contains("oldSchemaVersion:"));
    assertTrue(response.get(3).contains("oldSchema:"));
    assertTrue(response.get(4).contains("compatibility:"));
  }

  @Test
  public void testSchemaNormalization() throws Exception {
    String subject1 = "testSubject1";

    String msg1 = "syntax = \"proto3\";\n" +
        "package pkg1;\n" +
        "\n" +
        "option go_package = \"pkg1pb\";\n" +
        "option java_multiple_files = true;\n" +
        "option java_outer_classname = \"Msg1Proto\";\n" +
        "option java_package = \"com.pkg1\";\n" +
        "\n" +
        "message Message1 {\n" +
        "  string s = 1;\n" +
        "}\n";
    String subject = "pkg1/msg1.proto";
    registerAndVerifySchema(restApp.restClient, msg1, 1, subject);
    String msg2 = "syntax = \"proto3\";\n" +
        "package pkg2;\n" +
        "\n" +
        "option go_package = \"pkg2pb\";\n" +
        "option java_multiple_files = true;\n" +
        "option java_outer_classname = \"Msg2Proto\";\n" +
        "option java_package = \"com.pkg2\";\n" +
        "\n" +
        "message Message2 {\n" +
        "  string s = 1;\n" +
        "}\n";
    subject = "pkg2/msg2.proto";
    registerAndVerifySchema(restApp.restClient, msg2, 2, subject);

    String msg3 = "syntax = \"proto3\";\n" +
        "package pkg3;\n" +
        "\n" +
        "option go_package = \"pkg3pb\";\n" +
        "option java_multiple_files = true;\n" +
        "option java_outer_classname = \"Msg3Proto\";\n" +
        "option java_package = \"com.pkg3\";\n" +
        "\n" +
        "import \"pkg1/msg1.proto\";\n" +
        "import \"pkg2/msg2.proto\";\n" +
        "\n" +
        "message Message3 {\n" +
        "  map<string, pkg1.Message1> map = 1;\n" +
        "  pkg1.Message1 f1 = 2;\n" +
        "  pkg2.Message2 f2 = 3;\n" +
        "}\n";
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(msg3);
    request.setSchemaType(ProtobufSchema.TYPE);
    SchemaReference ref1 = new SchemaReference("pkg1/msg1.proto", "pkg1/msg1.proto", 1);
    SchemaReference ref2 = new SchemaReference("pkg2/msg2.proto", "pkg2/msg2.proto", 1);
    List<SchemaReference> refs = Arrays.asList(ref1, ref2);
    request.setReferences(refs);
    int registeredId = restApp.restClient.registerSchema(request, subject1, true).getId();
    assertEquals("Registering a new schema should succeed", 3, registeredId);

    // Alternate version of same schema
    msg3 = "syntax = \"proto3\";\n" +
        "package pkg3;\n" +
        "\n" +
        "option java_package = \"com.pkg3\";\n" +
        "option java_outer_classname = \"Msg3Proto\";\n" +
        "option java_multiple_files = true;\n" +
        "option go_package = \"pkg3pb\";\n" +
        "\n" +
        "import \"pkg2/msg2.proto\";\n" +
        "import \"pkg1/msg1.proto\";\n" +
        "\n" +
        "message Message3 {\n" +
        "  pkg2.Message2 f2 = 3;\n" +
        "  pkg1.Message1 f1 = 2;\n" +
        "  map<string, pkg1.Message1> map = 1;\n" +
        "}\n";

    RegisterSchemaRequest lookUpRequest = new RegisterSchemaRequest();
    lookUpRequest.setSchema(msg3);
    lookUpRequest.setSchemaType(ProtobufSchema.TYPE);
    lookUpRequest.setReferences(Arrays.asList(ref2, ref1));
    int versionOfRegisteredSchema1Subject1 =
        restApp.restClient.lookUpSubjectVersion(lookUpRequest, subject1, true, false).getVersion();
    assertEquals("1st schema under subject1 should have version 1", 1,
        versionOfRegisteredSchema1Subject1);
    assertEquals("1st schema registered globally should have id 3", 3,
        registeredId);
  }

  @Test
  public void testBad() throws Exception {
    String subject1 = "testTopic1";
    List<String> allSubjects = new ArrayList<String>();

    // test getAllSubjects with no existing data
    assertEquals("Getting all subjects should return empty",
        allSubjects,
        restApp.restClient.getAllSubjects()
    );

    try {
      registerAndVerifySchema(restApp.restClient, getBadSchema(), 1, subject1);
      fail("Registering bad schema should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals("Invalid schema",
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode());
    }

    try {
      registerAndVerifySchema(restApp.restClient, getRandomProtobufSchemas(1).get(0),
          Arrays.asList(new SchemaReference("bad", "bad", 100)), 1, subject1);
      fail("Registering bad reference should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals("Invalid schema",
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode());
    }

    // test getAllSubjects with existing data
    assertEquals("Getting all subjects should match all registered subjects",
        allSubjects,
        restApp.restClient.getAllSubjects()
    );
  }

  @Test
  public void testCustomOption() throws Exception {
    String subject = "test-proto";
    String enumOptionSchemaString = "syntax = \"proto3\";\n"
        + "\n"
        + "import \"google/protobuf/descriptor.proto\";\n"
        + "\n"
        + "option java_package = \"io.confluent.kafka.serializers.protobuf.test\";\n"
        + "option java_outer_classname = \"TestEnumProtos\";\n"
        + "option php_namespace = \"Bug\\\\V1\";\n"
        + "\n"
        + "message TestEnum {\n"
        + "  option (some_ref) = \"https://test.com\";\n"
        + "\n"
        + "  Suit suit = 1;\n"
        + "\n"
        + "  oneof test_oneof {\n"
        + "    option (some_ref) = \"https://test.com\";\n"
        + "  \n"
        + "    string name = 2;\n"
        + "    int32 age = 3;\n"
        + "  }\n"
        + "\n"
        + "  enum Suit {\n"
        + "    option (some_ref) = \"https://test.com\";\n"
        + "    SPADES = 0;\n"
        + "    HEARTS = 1;\n"
        + "    DIAMONDS = 2;\n"
        + "    CLUBS = 3;\n"
        + "  }\n"
        + "}\n";

    registerAndVerifySchema(restApp.restClient, enumOptionSchemaString, 1, subject);
  }

  @Test
  public void testRegisterSchemaTagsBasic() throws Exception {
    String subject = "test";
    String schemaString = "syntax = \"proto3\";\n" +
        "package com.example;\n" +
        "\n" +
        "message Message1 {\n" +
        "  string f1 = 1;\n" +
        "  string f2 = 2;\n" +
        "}\n";
    registerAndVerifySchema(restApp.restClient, schemaString, 1, subject);

    TagSchemaRequest tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setTagsToAdd(Arrays.asList(
        new SchemaTags(new SchemaEntity("Message1.f1", SchemaEntity.EntityType.SR_FIELD),
            Arrays.asList("TAG1")),
        new SchemaTags(new SchemaEntity(".Message1.f1", SchemaEntity.EntityType.SR_FIELD),
            Arrays.asList("TAG2"))
        ));

    String expectedSchema = "syntax = \"proto3\";\n" +
        "package com.example;\n" +
        "\n" +
        "message Message1 {\n" +
        "  string f1 = 1 [(confluent.field_meta) = {\n" +
        "    tags: [\n" +
        "      \"TAG1\",\n" +
        "      \"TAG2\"\n" +
        "    ]\n" +
        "  }];\n" +
        "  string f2 = 2;\n" +
        "}\n";
    RegisterSchemaResponse responses = restApp.restClient
        .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(2, responses.getId());

    Schema result = restApp.restClient.getLatestVersion(subject);
    assertEquals(expectedSchema, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setNewVersion(3);
    tagSchemaRequest.setTagsToRemove(Collections.singletonList(
        new SchemaTags(new SchemaEntity("Message1.f1", SchemaEntity.EntityType.SR_FIELD),
            Arrays.asList("TAG2"))));

    expectedSchema = "syntax = \"proto3\";\n" +
        "package com.example;\n" +
        "\n" +
        "message Message1 {\n" +
        "  string f1 = 1 [(confluent.field_meta) = {\n" +
        "    tags: [\n" +
        "      \"TAG1\"\n" +
        "    ]\n" +
        "  }];\n" +
        "  string f2 = 2;\n" +
        "}\n";
    responses = restApp.restClient
        .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(3, responses.getId());

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(expectedSchema, result.getSchema());
    assertEquals((Integer) 3, result.getVersion());
    assertEquals("3", responses.getMetadata().getProperties().get("confluent:version"));
  }

  public static void registerAndVerifySchema(
      RestService restService,
      String schemaString,
      int expectedId,
      String subject
  ) throws IOException, RestClientException {
    registerAndVerifySchema(
        restService, schemaString, Collections.emptyList(), expectedId, subject);
  }

  public static void registerAndVerifySchema(
      RestService restService,
      String schemaString,
      List<SchemaReference> references,
      int expectedId,
      String subject
  ) throws IOException, RestClientException {
    int registeredId = restService.registerSchema(schemaString,
        ProtobufSchema.TYPE,
        references,
        subject
    ).getId();
    Assert.assertEquals(
        "Registering a new schema should succeed",
        (long) expectedId,
        (long) registeredId
    );
    Assert.assertEquals(
        "Registered schema should be found",
        schemaString.trim(),
        restService.getId(expectedId).getSchemaString().trim()
    );
  }

  public static List<String> getRandomProtobufSchemas(int num) {
    List<String> schemas = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      String schema =
          "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n\n"
              + "message MyMessage {\n  string f"
              + random.nextInt(Integer.MAX_VALUE)
              + " = 1;\n  bool is_active = 2;\n}\n";
      schemas.add(schema);
    }
    return schemas;
  }

  public static Map<String, String> getProtobufSchemaWithDependencies() {
    Map<String, String> schemas = new HashMap<>();
    String meta = ResourceLoader.DEFAULT.toString("confluent/meta.proto");
    schemas.put("confluent/meta.proto", meta);
    String reference =
        "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n\n"
            + "message ReferencedMessage {\n  string ref_id = 1;\n  bool is_active = 2;\n}\n";
    schemas.put("ref.proto", reference);
    String schemaString = "syntax = \"proto3\";\n"
        + "package io.confluent.kafka.serializers.protobuf.test;\n"
        + "\n"
        + "import \"ref.proto\";\n"
        + "import \"confluent/meta.proto\";\n"
        + "\n"
        + "message ReferrerMessage {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    doc: \"ReferrerMessage\"\n"
        + "  };\n"
        + "\n"
        + "  string root_id = 1;\n"
        + "  .io.confluent.kafka.serializers.protobuf.test.ReferencedMessage ref = 2 [(confluent.field_meta) = {\n"
        + "    doc: \"ReferencedMessage\"\n"
        + "  }];\n"
        + "}\n";
    schemas.put("root.proto", schemaString);
    return schemas;
  }

  public static String getBadSchema() {
    String schema =
        "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n\n"
            + "bad-message MyMessage {\n  string f"
            + random.nextInt(Integer.MAX_VALUE)
            + " = 1;\n  bool is_active = 2;\n}\n";
    return schema;
  }
}

