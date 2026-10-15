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
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@code schema.providers.json.fetch.remote.schemas=false} actually reaches the
 * registry's registration path (not just the {@code JsonSchema} unit level), in both READWRITE
 * and IMPORT mode.
 */
public class JsonSchemaFetchRemoteRefsConfigTest extends ClusterTestHarness {

  private static final String SCHEMA_WITH_UNREGISTERED_HTTP_REF =
      "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
      + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}";

  public JsonSchemaFetchRemoteRefsConfigTest() {
    super(1, true);
  }

  @Override
  public Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty(
        SchemaRegistryConfig.SCHEMA_PROVIDERS_JSON_FETCH_REMOTE_REFS_CONFIG, "false");
    return props;
  }

  @Test
  public void readWriteRegistrationRejectedWhenRemoteRefFetchingDisabled() throws Exception {
    try {
      restApp.restClient.registerSchema(
          SCHEMA_WITH_UNREGISTERED_HTTP_REF, "JSON", Collections.emptyList(), "testSubject");
      fail("Expected registration to be rejected because remote ref fetching is disabled");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }
  }

  @Test
  public void importModeRegistrationRejectedWhenRemoteRefFetchingDisabled() throws Exception {
    restApp.restClient.setMode("IMPORT");
    try {
      restApp.restClient.registerSchema(SCHEMA_WITH_UNREGISTERED_HTTP_REF, "JSON",
          Collections.emptyList(), "importSubject", 1, 1);
      fail("Expected registration to be rejected because remote ref fetching is disabled");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }
  }
}
