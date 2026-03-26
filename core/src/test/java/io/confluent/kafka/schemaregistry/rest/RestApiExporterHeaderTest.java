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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration tests to verify that X-Exporter-Name header is properly handled
 * across all schema registry endpoints.
 */
public class RestApiExporterHeaderTest extends ClusterTestHarness {

  private Map<String, String> exporterHeaders;

  public RestApiExporterHeaderTest() {
    super(1, true);
  }

  @Before
  public void setUpHeaders() {
    exporterHeaders = new HashMap<>();
    exporterHeaders.put("X-Exporter-Name", "test-exporter");
  }

  @After
  public void tearDownHeaders() {
    // Clear headers after each test
    restApp.restClient.setHttpHeaders(null);
  }

  @Test
  public void testSubjectVersionsRegisterWithExporterHeader() throws Exception {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    // Set the exporter header
    restApp.restClient.setHttpHeaders(exporterHeaders);

    // Register a schema with the exporter header
    int id = restApp.restClient.registerSchema(schemaString, subject);

    // Verify the schema was registered successfully
    assertNotNull("Schema ID should not be null", id);
    assertEquals("Schema ID should be 1", 1, id);
  }

  @Test
  public void testSubjectVersionsDeleteWithExporterHeader() throws Exception {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    // Register a schema first
    int id = restApp.restClient.registerSchema(schemaString, subject);
    assertEquals(1, id);

    // Set the exporter header
    restApp.restClient.setHttpHeaders(exporterHeaders);

    // Delete the schema version with the exporter header
    int deletedVersion = restApp.restClient.deleteSchemaVersion(
        Collections.emptyMap(), subject, "1");

    // Verify the version was deleted
    assertEquals("Deleted version should be 1", 1, deletedVersion);
  }

  @Test
  public void testSubjectsDeleteWithExporterHeader() throws Exception {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    // Register a schema first
    int id = restApp.restClient.registerSchema(schemaString, subject);
    assertEquals(1, id);

    // Set the exporter header
    restApp.restClient.setHttpHeaders(exporterHeaders);

    // Delete the subject with the exporter header
    restApp.restClient.deleteSubject(Collections.emptyMap(), subject);

    // Verify subject was deleted by checking it no longer exists
    assertEquals("Subjects list should be empty",
        Collections.emptyList(),
        restApp.restClient.getAllSubjects());
  }

  @Test
  public void testConfigUpdateWithExporterHeader() throws Exception {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    // Register a schema first
    int id = restApp.restClient.registerSchema(schemaString, subject);
    assertEquals(1, id);

    // Set the exporter header
    restApp.restClient.setHttpHeaders(exporterHeaders);

    // Update subject-level config with the exporter header
    ConfigUpdateRequest response =
        restApp.restClient.updateCompatibility("BACKWARD", subject);

    // Verify config was updated
    assertNotNull("Config response should not be null", response);
    assertEquals("BACKWARD", response.getCompatibilityLevel());
  }

  @Test
  public void testConfigDeleteWithExporterHeader() throws Exception {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    // Register a schema and set config
    int id = restApp.restClient.registerSchema(schemaString, subject);
    assertEquals(1, id);

    restApp.restClient.updateCompatibility("BACKWARD", subject);

    // Set the exporter header
    restApp.restClient.setHttpHeaders(exporterHeaders);

    // Delete subject-level config with the exporter header
    restApp.restClient.deleteConfig(subject);

    // Verify config was deleted - get default/top-level config
    assertEquals("NONE", restApp.restClient.getConfig(null).getCompatibilityLevel());
  }

  @Test
  public void testModeUpdateWithExporterHeader() throws Exception {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    // Register a schema first
    int id = restApp.restClient.registerSchema(schemaString, subject);
    assertEquals(1, id);

    // Set the exporter header
    restApp.restClient.setHttpHeaders(exporterHeaders);

    // Update mode with the exporter header
    ModeUpdateRequest response = restApp.restClient.setMode("READONLY", subject);

    // Verify mode was updated
    assertNotNull("Mode response should not be null", response);
    assertEquals("READONLY", response.getMode());
  }

  @Test
  public void testModeDeleteWithExporterHeader() throws Exception {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    // Register a schema and set mode
    int id = restApp.restClient.registerSchema(schemaString, subject);
    assertEquals(1, id);

    restApp.restClient.setMode("READONLY", subject);

    // Set the exporter header
    restApp.restClient.setHttpHeaders(exporterHeaders);

    // Delete subject-level mode with the exporter header
    restApp.restClient.deleteSubjectMode(subject);

    // Verify mode was deleted - get default/top-level mode
    assertEquals("READWRITE", restApp.restClient.getMode(null).getMode());
  }

  @Test
  public void testOperationsWithoutExporterHeader() throws Exception {
    String subject = "testSubject";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    // Do NOT set the exporter header
    // This tests that operations still work when the header is absent

    // Register a schema without the exporter header
    int id = restApp.restClient.registerSchema(schemaString, subject);

    // Verify the schema was registered successfully
    assertNotNull("Schema ID should not be null", id);
    assertEquals("Schema ID should be 1", 1, id);

    // Update config without the exporter header
    ConfigUpdateRequest configResponse =
        restApp.restClient.updateCompatibility("BACKWARD", subject);

    assertNotNull("Config response should not be null", configResponse);
    assertEquals("BACKWARD", configResponse.getCompatibilityLevel());
  }
}
