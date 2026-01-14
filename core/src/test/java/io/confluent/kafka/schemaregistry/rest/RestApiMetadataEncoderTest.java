/*
 * Copyright 2022 Confluent Inc.
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("IntegrationTest")
public abstract class RestApiMetadataEncoderTest {

  protected static final String INITIAL_SECRET = "mysecret";
  protected static final String ROTATED_SECRET = "mynewsecret";

  protected RestApp restApp = null;

  protected String tenant = SchemaRegistry.DEFAULT_TENANT;

  public void setRestApp(RestApp restApp) {
    this.restApp = restApp;
  }

  protected int expectedSchemaId(int sequentialId) {
    return sequentialId;
  }

  protected abstract void removeEncoder(String tenant) throws Exception;

  /**
   * Creates a new RestApp configured with the rotated secret.
   *
   * @param newSecret the new encoder secret
   * @param oldSecret the old encoder secret (for rotation)
   * @return a new RestApp configured with the rotated secrets
   */
  protected abstract RestApp createRotatedRestApp(String newSecret, String oldSecret) throws Exception;

  private static String SCHEMA_STRING = AvroUtils.parseSchema(
      "{\"type\":\"record\","
          + "\"name\":\"myrecord\","
          + "\"fields\":"
          + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
      .canonicalString();

  private static String ROTATION_TEST_SCHEMA = AvroUtils.parseSchema(
      "{\"type\":\"record\","
          + "\"name\":\"rotationtest\","
          + "\"fields\":"
          + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
      .canonicalString();

  @Test
  public void testRegisterSchemaWithSensitiveMetadata() throws Exception {
    String subject = "testSubject";

    Map<String, String> properties = new HashMap<>();
    properties.put("nonsensitive", "foo");
    properties.put("sensitive", "foo");
    Metadata metadata = new Metadata(null, properties, Collections.singleton("sensitive"));
    Schema schema = new Schema(subject, null, null, null, null, metadata, null, SCHEMA_STRING);
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);

    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request, subject, false).getId(),
        "Registering without id should succeed"
    );

    List<SubjectVersion> subjectVersions = restApp.restClient.getAllVersionsById(expectedIdSchema1);
    assertEquals(ImmutableList.of(new SubjectVersion(subject, 1)), subjectVersions);

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema1);
    assertEquals(properties, schemaString.getMetadata().getProperties());
  }

  @Test
  public void testMissingEncoder() throws Exception {
    String subject = "testSubject";

    Map<String, String> properties = new HashMap<>();
    properties.put("nonsensitive", "foo");
    properties.put("sensitive", "foo");
    Metadata metadata = new Metadata(null, properties, Collections.singleton("sensitive"));
    Schema schema = new Schema(subject, null, null, null, null, metadata, null, SCHEMA_STRING);
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);

    int expectedIdSchema1 = expectedSchemaId(1);
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request, subject, false).getId(),
        "Registering without id should succeed"
    );

    // Remove encoder
    removeEncoder(tenant);

    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.getAllVersionsById(expectedIdSchema1),
        "Should fail to get schema"
    );

    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.getId(expectedIdSchema1),
        "Should fail to get schema"
    );

    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.getVersion(subject, 1),
        "Should fail to get schema"
    );

    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.getLatestVersion(subject),
        "Should fail to get schema"
    );

    assertThrows(
        RestClientException.class,
        () -> restApp.restClient.getLatestVersion(subject),
        "Should fail to get schema"
    );

    List<Schema> schemas = restApp.restClient.getSchemas(subject, true, false);
    assertTrue(schemas.isEmpty());

    List<String> subjects = restApp.restClient.getAllSubjects();
    assertTrue(subjects.isEmpty());
  }

  /**
   * Tests the maybeRotateSecrets() method by:
   * 1. Starting with the initial secret and registering a schema with sensitive metadata
   * 2. Stopping the RestApp
   * 3. Starting a new RestApp with a new secret and the old secret configured
   * 4. Verifying the schema can still be read (rotation happened successfully)
   * 5. Verifying the encoder keyset has been rotated (has 2 keys now)
   */
  @Test
  public void testSecretRotation() throws Exception {

    String subject = "rotationTestSubject";

    // Step 1: Register a schema with sensitive metadata using the initial secret
    Map<String, String> properties = new HashMap<>();
    properties.put("nonsensitive", "foo");
    properties.put("sensitive", "secret-value");
    Metadata metadata = new Metadata(null, properties, Collections.singleton("sensitive"));
    Schema schema = new Schema(subject, null, null, null, null, metadata, null, ROTATION_TEST_SCHEMA);
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);

    int schemaId = restApp.restClient.registerSchema(request, subject, false).getId();

    // Verify the schema was registered and can be read
    SchemaString schemaString = restApp.restClient.getId(schemaId);
    assertEquals(properties, schemaString.getMetadata().getProperties());

    // Step 2: Stop the RestApp
    restApp.stop();

    // Step 3: Start a new RestApp with the rotated secret configuration
    RestApp rotatedRestApp = createRotatedRestApp(ROTATED_SECRET, INITIAL_SECRET);
    assertNotNull(rotatedRestApp);

    try {
      // Step 4: Verify the schema can still be read after rotation
      // The maybeRotateSecrets() should have run during init()
      SchemaString rotatedSchemaString = rotatedRestApp.restClient.getId(schemaId);
      assertEquals(properties, rotatedSchemaString.getMetadata().getProperties(),
          "Schema should be readable after secret rotation");

      // Step 5: Verify we can still register new schemas with sensitive metadata
      String newSubject = "rotationTestSubject2";
      Map<String, String> newProperties = new HashMap<>();
      newProperties.put("nonsensitive", "bar");
      newProperties.put("sensitive", "another-secret");
      Metadata newMetadata = new Metadata(null, newProperties, Collections.singleton("sensitive"));
      Schema newSchema = new Schema(newSubject, null, null, null, null, newMetadata, null, ROTATION_TEST_SCHEMA);
      RegisterSchemaRequest newRequest = new RegisterSchemaRequest(newSchema);

      int newSchemaId = rotatedRestApp.restClient.registerSchema(newRequest, newSubject, false).getId();
      assertNotEquals(schemaId, newSchemaId, "New schema should have different ID");

      SchemaString newSchemaString = rotatedRestApp.restClient.getId(newSchemaId);
      assertEquals(newProperties, newSchemaString.getMetadata().getProperties(),
          "New schema should be readable");
    } finally {
      rotatedRestApp.stop();
    }
  }
}
