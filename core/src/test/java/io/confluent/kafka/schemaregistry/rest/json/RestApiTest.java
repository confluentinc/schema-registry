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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
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
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;

import static org.junit.Assert.assertEquals;

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
          JsonSchema.TYPE,
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
    Map<String, String> schemas = getJsonSchemaWithReferences();
    String subject = "reference";
    registerAndVerifySchema(restApp.restClient, schemas.get("ref.json"), 1, subject);

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schemas.get("main.json"));
    request.setSchemaType(JsonSchema.TYPE);
    SchemaReference ref = new SchemaReference("ref.json", "reference", 1);
    request.setReferences(Collections.singletonList(ref));
    int registeredId = restApp.restClient.registerSchema(request, "referrer");
    assertEquals("Registering a new schema should succeed", 2, registeredId);

    SchemaString schemaString = restApp.restClient.getId(2);
    // the newly registered schema should be immediately readable on the master
    assertEquals("Registered schema should be found",
        MAPPER.readTree(schemas.get("main.json")),
        MAPPER.readTree(schemaString.getSchemaString())
    );

    assertEquals("Schema references should be found",
        Collections.singletonList(ref),
        schemaString.getReferences()
    );
  }

  public static void registerAndVerifySchema(
      RestService restService,
      String schemaString,
      int expectedId,
      String subject
  ) throws IOException, RestClientException {
    int registeredId = restService.registerSchema(
        schemaString,
        JsonSchema.TYPE,
        Collections.emptyList(),
        subject
    );
    Assert.assertEquals(
        "Registering a new schema should succeed",
        expectedId,
        registeredId
    );
    Assert.assertEquals("Registered schema should be found",
        MAPPER.readTree(schemaString),
        MAPPER.readTree(restService.getId(expectedId).getSchemaString())
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
        /*
        String reference = "{\n  \"type\": \"object\",\n"
            + "  \"additionalProperties\": false,\n"
            + "  \"definitions\": {\n"
            + "    \"ExternalType\": {\n"
            + "      \"type\": \"object\",\n"
            + "      \"additionalProperties\": false,\n"
            + "      \"properties\": {\n"
            + "        \"name\": {\n"
            + "          \"type\": \"string\"\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}\n";
         */
    schemas.put("ref.json", new JsonSchema(reference).canonicalString());
    String schemaString = "{\"type\":\"object\",\"properties\":{\"Ref\":"
        + "{\"$ref\":\"ref.json#/definitions/ExternalType\"}},\"additionalProperties\":false}";
        /*
        String schemaString = "{\n  \"type\": \"object\",\n"
            + "  \"additionalProperties\": false,\n"
            + "  \"properties\": {\n"
            + "    \"Ref\": {\n"
            + "      \"$ref\": \"ref.json#/definitions/ExternalType\"\n"
            + "    }\n"
            + "  }\n"
            + "}\n";
         */
    schemas.put("main.json", schemaString);
    return schemas;
  }
}

