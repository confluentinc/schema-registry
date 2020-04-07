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
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
      );
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
    String subject = "reference";
    registerAndVerifySchema(restApp.restClient, schemas.get("ref.proto"), 1, subject);

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get("root.proto"));
    request.setSchemaType(ProtobufSchema.TYPE);
    SchemaReference ref = new SchemaReference("ref.proto", "reference", 1);
    request.setReferences(Collections.singletonList(ref));
    int registeredId = restApp.restClient.registerSchema(request, "referrer");
    assertEquals("Registering a new schema should succeed", 2, registeredId);

    SchemaString schemaString = restApp.restClient.getId(2);
    // the newly registered schema should be immediately readable on the master
    assertEquals("Registered schema should be found",
        schemas.get("root.proto"),
        schemaString.getSchemaString()
    );

    assertEquals("Schema dependencies should be found",
        Collections.singletonList(ref),
        schemaString.getReferences()
    );
  }

  @Test(expected = RestClientException.class)
  public void testSchemaMissingReferences() throws Exception {
    Map<String, String> schemas = getProtobufSchemaWithDependencies();

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get("root.proto"));
    request.setSchemaType(ProtobufSchema.TYPE);
    request.setReferences(Collections.emptyList());
    restApp.restClient.registerSchema(request, "referrer");
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
    );
    Assert.assertEquals(
        "Registering a new schema should succeed",
        (long) expectedId,
        (long) registeredId
    );
    Assert.assertEquals(
        "Registered schema should be found",
        schemaString,
        restService.getId(expectedId).getSchemaString()
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
    String reference =
        "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n\n"
            + "message ReferencedMessage {\n  string ref_id = 1;\n  bool is_active = 2;\n}\n";
    schemas.put("ref.proto", reference);
    String schemaString =
        "syntax = \"proto3\";\npackage io.confluent.kafka.serializers.protobuf.test;\n\n"
            + "import \"ref.proto\";\n\n"
            + "message ReferrerMessage {\n  string root_id = 1;\n  ReferencedMessage ref = 2;\n}\n";
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

