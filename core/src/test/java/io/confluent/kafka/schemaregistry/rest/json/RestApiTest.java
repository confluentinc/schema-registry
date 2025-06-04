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
 */

package io.confluent.kafka.schemaregistry.rest.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;

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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RestApiTest extends ClusterTestHarness {

  private static final Random random = new Random();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public RestApiTest() {
    super(1, true);
  }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty("schema.providers", JsonSchemaProvider.class.getName());
    return props;
  }

  @Test
  public void testBasic() throws Exception {
    String subject1 = "testTopic1";
    String subject2 = "testTopic2";
    int schemasInSubject1 = 10;
    List<Integer> allVersionsInSubject1 = new ArrayList<Integer>();
    List<String> allSchemasInSubject1 = getRandomJsonSchemas(schemasInSubject1);
    int schemasInSubject2 = 5;
    List<Integer> allVersionsInSubject2 = new ArrayList<Integer>();
    List<String> allSchemasInSubject2 = getRandomJsonSchemas(schemasInSubject2);
    List<String> allSubjects = new ArrayList<String>();

    // test getAllSubjects with no existing data
    assertEquals(
        allSubjects,
        restApp.restClient.getAllSubjects(),
        "Getting all subjects should return empty"
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
          JsonSchema.TYPE,
          Collections.emptyList(),
          subject1
      ).getId();
      assertEquals(
          expectedId,
          foundId,
          "Re-registering an existing schema should return the existing version"
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
        allVersionsInSubject1,
        restApp.restClient.getAllVersions(subject1),
        "Getting all versions from subject1 should match all registered versions"
    );
    assertEquals(
        allVersionsInSubject2,
        restApp.restClient.getAllVersions(subject2),
        "Getting all versions from subject2 should match all registered versions"
    );

    // test getAllSubjects with existing data
    assertEquals(
        allSubjects,
        restApp.restClient.getAllSubjects(),
        "Getting all subjects should match all registered subjects"
    );
  }

  @Test
  public void testSchemaReferences() throws Exception {
    Map<String, String> schemas = getJsonSchemaWithReferences();
    String subject = "reference";
    registerAndVerifySchema(restApp.restClient, schemas.get("ref.json"), 1, subject);

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get("main.json"));
    request.setSchemaType(JsonSchema.TYPE);
    SchemaReference ref = new SchemaReference("ref.json", "reference", 1);
    request.setReferences(Collections.singletonList(ref));
    int registeredId = restApp.restClient.registerSchema(request, "referrer", false).getId();
    assertEquals(2, registeredId, "Registering a new schema should succeed");

    SchemaString schemaString = restApp.restClient.getId(2);
    // the newly registered schema should be immediately readable on the leader
    assertEquals(
        MAPPER.readTree(schemas.get("main.json")),
        MAPPER.readTree(schemaString.getSchemaString()),
        "Registered schema should be found"
    );

    assertEquals(
        Collections.singletonList(ref),
        schemaString.getReferences(),
        "Schema references should be found"
    );

    List<Integer> refs = restApp.restClient.getReferencedBy("reference", 1);
    assertEquals(2, refs.get(0).intValue());

    CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
            restApp.restClient, 10,
            Collections.singletonList(new JsonSchemaProvider()), new HashMap<>(), null);
    SchemaHolder holder = new SchemaHolder();
    JsonSchema schema = JsonSchemaUtils.getSchema(holder, schemaRegistryClient);
    Schema registeredSchema = restApp.restClient.lookUpSubjectVersion(schema.canonicalString(),
            JsonSchema.TYPE, schema.references(), "referrer", false);
    assertEquals(2, registeredSchema.getId().intValue(), "Registered schema should be found");

    try {
      restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
          "reference",
          String.valueOf(1)
      );
      fail("Deleting reference should fail with " + Errors.REFERENCE_EXISTS_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.REFERENCE_EXISTS_ERROR_CODE,
          rce.getErrorCode(),
          "Reference found"
      );
    }

    assertEquals((Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, "referrer", "1"));

    refs = restApp.restClient.getReferencedBy("reference", 1);
    assertTrue(refs.isEmpty());

    assertEquals((Integer) 1, restApp.restClient
        .deleteSchemaVersion
            (RestService.DEFAULT_REQUEST_PROPERTIES, "reference", "1"));
  }

  @io.confluent.kafka.schemaregistry.annotations.Schema(value="{"
          + "\"$id\": \"https://acme.com/referrer.json\","
          + "\"$schema\": \"http://json-schema.org/draft-07/schema#\","
          + "\"type\":\"object\","
          + "\"properties\":{\"Ref\":"
          + "{\"$ref\":\"ref.json#/definitions/ExternalType\"}},\"additionalProperties\":false}",
          refs={@io.confluent.kafka.schemaregistry.annotations.SchemaReference(
                  name="ref.json", subject="reference")})
  static class SchemaHolder {
      // This is a dummy schema holder to be used for its annotations
  }

  @Test
  public void testSchemaMissingReferences() throws Exception {
    assertThrows(RestClientException.class, () -> {
      Map<String, String> schemas = getJsonSchemaWithReferences();

      RegisterSchemaRequest request = new RegisterSchemaRequest();
      request.setSchema(schemas.get("main.json"));
      request.setSchemaType(JsonSchema.TYPE);
      request.setReferences(Collections.emptyList());
      restApp.restClient.registerSchema(request, "referrer", false);
    });
  }

  @Test
  public void testSchemaNormalization() throws Exception {
    String subject1 = "testSubject1";

    String reference1 = "{\"type\":\"object\",\"additionalProperties\":false,\"definitions\":"
        + "{\"ExternalType\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},"
        + "\"additionalProperties\":false}}}";
    registerAndVerifySchema(restApp.restClient, reference1, 1, "ref1");
    String reference2 = "{\"type\":\"object\",\"additionalProperties\":false,\"definitions\":"
        + "{\"ExternalType2\":{\"type\":\"object\",\"properties\":{\"name2\":{\"type\":\"string\"}},"
        + "\"additionalProperties\":false}}}";
    registerAndVerifySchema(restApp.restClient, reference2, 2, "ref2");

    SchemaReference ref1 = new SchemaReference("ref1.json", "ref1", 1);
    SchemaReference ref2 = new SchemaReference("ref2.json", "ref2", 1);

    // Two versions of same schema
    String schemaString1 = "{"
        + "\"$id\": \"https://acme.com/referrer.json\","
        + "\"$schema\": \"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{"
        + "\"Ref1\": {\"$ref\":\"ref1.json#/definitions/ExternalType\"},"
        + "\"Ref2\": {\"$ref\":\"ref2.json#/definitions/ExternalType2\"}"
        + "},\"additionalProperties\":false}";
    String schemaString2 = "{"
        + "\"$schema\": \"http://json-schema.org/draft-07/schema#\","
        + "\"$id\": \"https://acme.com/referrer.json\","
        + "\"type\":\"object\",\"properties\":{"
        + "\"Ref2\": {\"$ref\":\"ref2.json#/definitions/ExternalType2\"},"
        + "\"Ref1\": {\"$ref\":\"ref1.json#/definitions/ExternalType\"}"
        + "},\"additionalProperties\":false}";

    RegisterSchemaRequest registerRequest = new RegisterSchemaRequest();
    registerRequest.setSchema(schemaString1);
    registerRequest.setSchemaType(JsonSchema.TYPE);
    registerRequest.setReferences(Arrays.asList(ref1, ref2));
    int idOfRegisteredSchema1Subject1 =
        restApp.restClient.registerSchema(registerRequest, subject1, true).getId();
    RegisterSchemaRequest lookUpRequest = new RegisterSchemaRequest();
    lookUpRequest.setSchema(schemaString2);
    lookUpRequest.setSchemaType(JsonSchema.TYPE);
    lookUpRequest.setReferences(Arrays.asList(ref2, ref1));
    int versionOfRegisteredSchema1Subject1 =
        restApp.restClient.lookUpSubjectVersion(lookUpRequest, subject1, true, false).getVersion();
    assertEquals(
        1,
        versionOfRegisteredSchema1Subject1,
        "1st schema under subject1 should have version 1"
    );
    assertEquals(
        3,
        idOfRegisteredSchema1Subject1,
        "1st schema registered globally should have id 3"
    );
  }

  @Test
  public void testBad() throws Exception {
    String subject1 = "testTopic1";
    List<String> allSubjects = new ArrayList<String>();

    // test getAllSubjects with no existing data
    assertEquals(
        allSubjects,
        restApp.restClient.getAllSubjects(),
        "Getting all subjects should return empty"
    );

    try {
      registerAndVerifySchema(restApp.restClient, getBadSchema(), 1, subject1);
      fail("Registering bad schema should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode(),
          "Invalid schema"
      );
    }

    try {
      registerAndVerifySchema(restApp.restClient, getRandomJsonSchemas(1).get(0),
          Arrays.asList(new SchemaReference("bad", "bad", 100)), 1, subject1);
      fail("Registering bad reference should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode(),
          "Invalid schema"
      );
    }

    // test getAllSubjects with existing data
    assertEquals(
        allSubjects,
        restApp.restClient.getAllSubjects(),
        "Getting all subjects should match all registered subjects"
    );
  }

  @Test
  public void testIncompatibleSchema() throws Exception {
    String subject = "testSubject";

    // Make two incompatible schemas - field 'myField2' has different types
    String schema1String = "{"
                            + "\"$schema\": \"http://json-schema.org/draft-07/schema#\","
                            + "\"$id\": \"https://acme.com/referrer.json\","
                            + "\"type\":\"object\",\"properties\":{"
                            + "\"myField1\": {\"type\":\"string\"},"
                            + "\"myField2\": {\"type\":\"number\"}"
                            + "},\"additionalProperties\":false"
                            + "}";

    RegisterSchemaRequest registerRequest = new RegisterSchemaRequest();
    registerRequest.setSchema(schema1String);
    registerRequest.setSchemaType(JsonSchema.TYPE);

    String schema2String = "{"
                             + "\"$schema\": \"http://json-schema.org/draft-07/schema#\","
                             + "\"$id\": \"https://acme.com/referrer.json\","
                             + "\"type\":\"object\",\"properties\":{"
                             + "\"myField1\": {\"type\":\"string\"},"
                             + "\"myField2\": {\"type\":\"string\"}"
                             + "},\"additionalProperties\":false"
                             + "}";

    // ensure registering incompatible schemas will raise an error
    restApp.restClient.updateCompatibility(
      CompatibilityLevel.FULL.name, subject);

    // test that compatibility check for incompatible schema returns false and the appropriate
    // error response from Avro
    int idOfRegisteredSchema1Subject1 = restApp.restClient.registerSchema(registerRequest, subject, true).getId();

    try {
      registerRequest.setSchema(schema2String);
      registerRequest.setSchemaType(JsonSchema.TYPE);
      restApp.restClient.registerSchema(registerRequest, subject, true);
      fail("Registering incompatible schema should fail with "
             + Errors.INCOMPATIBLE_SCHEMA_ERROR_CODE);
    } catch(RestClientException e) {
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
  public void testConfluentVersion() throws Exception {
    String subject = "test";
    String schemaString = "{\"id\":\"urn:jsonschema:com:MySchema\",\"properties\":{\"myEnum\":{\"enum\":[\"YES_VALUE\",\"NO_VALUE\"],\"type\":\"string\"}},\"type\":\"object\"}";

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchemaType(JsonSchema.TYPE);
    request.setSchema(schemaString);
    // Register with null version
    registerAndVerifySchema(restApp.restClient, request, 1, subject);

    Schema result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 1, result.getVersion());
    assertNull(result.getMetadata());

    // Register schema with version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 1, subject);

    // Register schema with version 2
    request.setVersion(2);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 2, subject);

    // Register schema with version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 2, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    // Lookup schema with null version
    request.setVersion(null);
    request.setMetadata(null);
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 1, result.getVersion());
    assertNull(result.getMetadata());

    // Lookup schema with confluent:version 1 (should return one without metadata)
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "1"), null));
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 1, result.getVersion());
    assertNull(result.getMetadata());

    // Lookup schema with confluent:version 2
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "2"), null));
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    // Delete version 1
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1");

    // Lookup schema with null version
    request.setVersion(null);
    request.setMetadata(null);
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with null version
    registerAndVerifySchema(restApp.restClient, request, 2, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with version 3
    request.setVersion(3);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 3, subject);

    // Register schema with version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 3, subject);

    // Register schema with version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 3, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 3, result.getVersion());
    assertEquals("3", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with version 3
    request.setVersion(3);
    request.setMetadata(null);
    try {
      registerAndVerifySchema(restApp.restClient, request, 3, subject);
      fail("Registering version that is not next version should fail with " + Errors.INVALID_SCHEMA_ERROR_CODE);
    } catch (RestClientException rce) {
      assertEquals(
          Errors.INVALID_SCHEMA_ERROR_CODE,
          rce.getErrorCode(),
          "Invalid schema"
      );
    }

    // Register schema with version 4
    request.setVersion(4);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 4, subject);

    // Lookup schema with null version
    request.setVersion(null);
    request.setMetadata(null);
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 4, result.getVersion());
    assertEquals("4", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with confluent:version -1
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "-1"), null));
    registerAndVerifySchema(restApp.restClient, request, 5, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 5, result.getVersion());
    assertEquals("5", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with confluent:version 2
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "2"), null));
    registerAndVerifySchema(restApp.restClient, request, 2, subject);

    // Register schema with confluent:version 3
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "3"), null));
    registerAndVerifySchema(restApp.restClient, request, 3, subject);

    // Register schema with confluent:version 0
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("confluent:version", "0"), null));
    registerAndVerifySchema(restApp.restClient, request, 6, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 6, result.getVersion());
    assertEquals("6", result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with empty metadata
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.emptyMap(), null));
    registerAndVerifySchema(restApp.restClient, request, 6, subject);

    // Register schema with new metadata
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("mykey", "myvalue"), null));
    registerAndVerifySchema(restApp.restClient, request, 7, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 7, result.getVersion());
    assertNull(result.getMetadata().getProperties().get("confluent:version"));

    // Register schema with confluent:version -1
    request.setVersion(-1);
    request.setMetadata(null);
    registerAndVerifySchema(restApp.restClient, request, 8, subject);

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 8, result.getVersion());
    assertEquals("8", result.getMetadata().getProperties().get("confluent:version"));

    // Lookup schema with new metadata
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("mykey", "myvalue"), null));
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 7, result.getVersion());
    assertNull(result.getMetadata().getProperties().get("confluent:version"));

    // Delete version 7
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "7");

    // Lookup schema with new metadata
    request.setVersion(null);
    request.setMetadata(new Metadata(null, Collections.singletonMap("mykey", "myvalue"), null));
    result = restApp.restClient.lookUpSubjectVersion(request, subject, false, false);
    assertEquals(schemaString, result.getSchema());
    assertEquals((Integer) 8, result.getVersion());
    assertEquals("8", result.getMetadata().getProperties().get("confluent:version"));
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
    int registeredId = restService.registerSchema(
        schemaString,
        JsonSchema.TYPE,
        references,
        subject
    ).getId();
    assertEquals(
        expectedId,
        registeredId,
        "Registering a new schema should succeed"
    );
    assertEquals(
        MAPPER.readTree(schemaString),
        MAPPER.readTree(restService.getId(expectedId).getSchemaString()),
        "Registered schema should be found"
    );
  }

  public static void registerAndVerifySchema(
      RestService restService,
      RegisterSchemaRequest request,
      int expectedId,
      String subject
  ) throws IOException, RestClientException {
    int registeredId = restService.registerSchema(request, subject, false).getId();
    assertEquals(
        (long) expectedId,
        (long) registeredId,
        "Registering a new schema should succeed"
    );
    assertEquals(
        request.getSchema().trim(),
        restService.getId(expectedId).getSchemaString().trim(),
        "Registered schema should be found"
    );
  }

  public static List<String> getRandomJsonSchemas(int num) {
    List<String> schemas = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      String schema = "{\"type\":\"object\",\"properties\":{\"f"
          + random.nextInt(Integer.MAX_VALUE)
          + "\":"
          + "{\"type\":\"string\"}},\"additionalProperties\":false}";
      schemas.add(schema);
    }
    return schemas;
  }

  public static Map<String, String> getJsonSchemaWithReferences() {
    Map<String, String> schemas = new HashMap<>();
    String reference = "{\"type\":\"object\",\"additionalProperties\":false,\"definitions\":"
        + "{\"ExternalType\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},"
        + "\"additionalProperties\":false}}}";
    schemas.put("ref.json", new JsonSchema(reference).canonicalString());
    String schemaString = "{"
        + "\"$id\": \"https://acme.com/referrer.json\","
        + "\"$schema\": \"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"Ref\":"
        + "{\"$ref\":\"ref.json#/definitions/ExternalType\"}},\"additionalProperties\":false}";
    schemas.put("main.json", schemaString);
    return schemas;
  }

  public static String getBadSchema() {
    String schema = "{\"type\":\"bad-object\",\"properties\":{\"f"
        + random.nextInt(Integer.MAX_VALUE)
        + "\":"
        + "{\"type\":\"string\"}},\"additionalProperties\":false}";
    return schema;
  }
}

