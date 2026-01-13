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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.storage.encoder.MetadataEncoderService;
import org.junit.Test;

public class RestApiMetadataEncoderTest extends ClusterTestHarness {

  protected static final String INITIAL_SECRET = "mysecret";
  protected static final String ROTATED_SECRET = "mynewsecret";

  private static final String SCHEMA_STRING = AvroUtils.parseSchema(
      "{\"type\":\"record\","
          + "\"name\":\"myrecord\","
          + "\"fields\":"
          + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
      .canonicalString();

  private static final String ROTATION_TEST_SCHEMA = AvroUtils.parseSchema(
          "{\"type\":\"record\","
              + "\"name\":\"rotationtest\","
              + "\"fields\":"
              + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
      .canonicalString();

  public RestApiMetadataEncoderTest() {
    super(1, true, CompatibilityLevel.BACKWARD.name);
  }

  @Override
  protected Properties getSchemaRegistryProperties() throws Exception {
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG, "mysecret");
    return props;
  }

  @Test
  public void testRegisterSchemaWithSensitiveMetadata() throws Exception {
    String subject = "testSubject";

    Map<String, String> properties = new HashMap<>();
    properties.put("nonsensitive", "foo");
    properties.put("sensitive", "foo");
    Metadata metadata = new Metadata(null, properties, Collections.singleton("sensitive"));
    Schema schema = new Schema(subject, null, null, null, null, metadata, null, SCHEMA_STRING);
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);

    int expectedIdSchema1 = 1;
    assertEquals("Registering without id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request, subject, false).getId());

    List<SubjectVersion> subjectVersions = restApp.restClient.getAllVersionsById(1);
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

    int expectedIdSchema1 = 1;
    assertEquals("Registering without id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request, subject, false).getId());

    // Remove encoder
    ((KafkaSchemaRegistry) restApp.schemaRegistry()).getMetadataEncoder().getEncoders()
        .remove(KafkaSchemaRegistry.DEFAULT_TENANT);

    assertThrows("Should fail to get schema",
        RestClientException.class,
        () -> restApp.restClient.getAllVersionsById(1));

    assertThrows("Should fail to get schema",
        RestClientException.class,
        () -> restApp.restClient.getId(expectedIdSchema1));

    assertThrows("Should fail to get schema",
        RestClientException.class,
        () -> restApp.restClient.getVersion(subject, 1));

    assertThrows("Should fail to get schema",
        RestClientException.class,
        () -> restApp.restClient.getLatestVersion(subject));

    assertThrows("Should fail to get schema",
        RestClientException.class,
        () -> restApp.restClient.getLatestVersion(subject));

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

    // Get the encoder keyset size before rotation (should be 1 key)
    MetadataEncoderService metadataEncoder =
        ((KafkaSchemaRegistry) restApp.schemaRegistry()).getMetadataEncoder();
    int keyCountBeforeRotation = metadataEncoder
        .getEncoder(KafkaSchemaRegistry.DEFAULT_TENANT).size();
    assertEquals("Should have 1 key before rotation", 1, keyCountBeforeRotation);

    // Step 2: Stop the RestApp (but keep Kafka running)
    restApp.stop();

    // Step 3: Start a new RestApp with the rotated secret configuration
    RestApp rotatedRestApp = createRotatedRestApp(ROTATED_SECRET, INITIAL_SECRET);
    assertNotNull(rotatedRestApp);

    try {
      // Step 4: Verify the schema can still be read after rotation
      // The maybeRotateSecrets() should have run during init()
      SchemaString rotatedSchemaString = rotatedRestApp.restClient.getId(schemaId);
      assertEquals("Schema should be readable after secret rotation",
          properties, rotatedSchemaString.getMetadata().getProperties());

      // Step 5: Verify the encoder keyset has been rotated (should now have 2 keys)
      MetadataEncoderService rotatedMetadataEncoder =
          ((KafkaSchemaRegistry) rotatedRestApp.schemaRegistry()).getMetadataEncoder();
      int keyCountAfterRotation = rotatedMetadataEncoder
          .getEncoder(KafkaSchemaRegistry.DEFAULT_TENANT).size();
      assertEquals("Should have 2 keys after rotation (old + new primary)",
          2, keyCountAfterRotation);

      // Verify we can still register new schemas with sensitive metadata
      String newSubject = "rotationTestSubject2";
      Map<String, String> newProperties = new HashMap<>();
      newProperties.put("nonsensitive", "bar");
      newProperties.put("sensitive", "another-secret");
      Metadata newMetadata = new Metadata(null, newProperties, Collections.singleton("sensitive"));
      Schema newSchema = new Schema(newSubject, null, null, null, null, newMetadata, null, ROTATION_TEST_SCHEMA);
      RegisterSchemaRequest newRequest = new RegisterSchemaRequest(newSchema);

      int newSchemaId = rotatedRestApp.restClient.registerSchema(newRequest, newSubject, false).getId();
      assertNotEquals("New schema should have different ID", schemaId, newSchemaId);

      SchemaString newSchemaString = rotatedRestApp.restClient.getId(newSchemaId);
      assertEquals("New schema should be readable",
          newProperties, newSchemaString.getMetadata().getProperties());
    } finally {
      rotatedRestApp.stop();
    }
  }

  protected RestApp createRotatedRestApp(String newSecret, String oldSecret) throws Exception {
    Properties rotatedProps = new Properties();
    int port = choosePort();
    rotatedProps.setProperty(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG, newSecret);
    rotatedProps.setProperty(SchemaRegistryConfig.METADATA_ENCODER_OLD_SECRET_CONFIG, oldSecret);
    rotatedProps.put(SchemaRegistryConfig.LISTENERS_CONFIG,
        getSchemaRegistryProtocol() + "://0.0.0.0:" + port);
    rotatedProps.put(SchemaRegistryConfig.MODE_MUTABILITY, true);

    RestApp rotatedRestApp = new RestApp(
        port,
        null,
        bootstrapServers,
        ClusterTestHarness.KAFKASTORE_TOPIC,
        CompatibilityLevel.BACKWARD.name,
        true,
        rotatedProps);
    rotatedRestApp.start();
    return rotatedRestApp;
  }
}
