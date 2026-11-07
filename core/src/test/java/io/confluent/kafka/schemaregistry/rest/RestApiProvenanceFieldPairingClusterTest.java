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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Registration under LOGICAL with {@code provenance.field.pairing=CHECK_ID}: fields pair by
 * provenance check id, so a rename by alias continues the field rather than deleting it, as it
 * does by name ({@link RestApiLogicalProvenanceTest}).
 */
@Tag("IntegrationTest")
public class RestApiProvenanceFieldPairingClusterTest {

  private static final String BEFORE = "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"ns\","
      + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"a\",\"type\":\"string\"}]}";
  private static final String RENAMED = "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"ns\","
      + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"},"
      + "{\"name\":\"b\",\"type\":\"string\",\"aliases\":[\"a\"]}]}";

  private final ClusterTestHarness harness;

  public RestApiProvenanceFieldPairingClusterTest() {
    harness = new ClusterTestHarness(1, true, CompatibilityLevel.BACKWARD.name);
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.PROVENANCE_FIELD_PAIRING_CONFIG, "CHECK_ID");
    harness.injectSchemaRegistryProperties(props);
  }

  @BeforeEach
  public void setUpTest(TestInfo testInfo) throws Exception {
    harness.setUpTest(testInfo);
  }

  @AfterEach
  public void tearDown() throws Exception {
    harness.tearDown();
  }

  @Test
  public void testARenameByAliasIsCompatibleWhenFieldsPairByCheckId() throws Exception {
    RestApp restApp = harness.getRestApp();
    String subject = "avro_rename";
    restApp.restClient.registerSchema(BEFORE, AvroSchema.TYPE, Collections.emptyList(), subject);
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setCompatibilityLevel(CompatibilityLevel.BACKWARD.name);
    config.setCompatibilityPolicy("LOGICAL");
    restApp.restClient.updateConfig(config, subject);

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(RENAMED);
    request.setSchemaType(AvroSchema.TYPE);
    List<String> messages =
        restApp.restClient.testCompatibility(request, subject, null, false, true);
    assertEquals(List.of(), messages);
    restApp.restClient.registerSchema(RENAMED, AvroSchema.TYPE, Collections.emptyList(), subject);
  }
}
