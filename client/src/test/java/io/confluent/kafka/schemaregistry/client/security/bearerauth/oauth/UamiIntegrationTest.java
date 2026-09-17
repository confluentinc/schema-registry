/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Manual integration test for UAMI credential provider.
 *
 * <p>Run this on an Azure VM with a User Assigned Managed Identity attached.
 * Set the required system properties before running:
 *
 * <pre>
 *   mvn test -pl client \
 *     -Dtest=UamiIntegrationTest \
 *     -Dschema.registry.url=https://psrc-123456.us-east-1.aws.confluent.cloud \
 *     -Duami.client.id=YOUR_UAMI_CLIENT_ID \
 *     -Duami.resource=api://YOUR_BOOTSTRAP_ID \
 *     -Dbearer.auth.logical.cluster=lsrc-XXXXX \
 *     -Dbearer.auth.identity.pool.id=pool-XXXXX
 * </pre>
 *
 * <p>Or edit the constants below and run:
 * <pre>
 *   mvn test -pl client -Dtest=UamiIntegrationTest
 * </pre>
 */
public class UamiIntegrationTest {

  // ---- Edit these values before running on your Azure VM ----
  private static final String SCHEMA_REGISTRY_URL =
      "https://psrc-123456.us-east-1.aws.confluent.cloud";
  private static final String UAMI_CLIENT_ID = "YOUR_UAMI_CLIENT_ID";
  private static final String RESOURCE = "api://YOUR_BOOTSTRAP_ID";
  private static final String LOGICAL_CLUSTER = "lsrc-XXXXX";
  private static final String IDENTITY_POOL_ID = "pool-XXXXX";
  // -----------------------------------------------------------

  private static String prop(String key, String fallback) {
    String value = System.getProperty(key);
    return value != null ? value : fallback;
  }

  @org.junit.Test
  public void testListSubjectsWithUami() throws Exception {
    // Skip when not running on an Azure VM (i.e. no -Dschema.registry.url passed)
    org.junit.Assume.assumeTrue(
        "Skipped: set -Dschema.registry.url to run this test",
        System.getProperty("schema.registry.url") != null);

    String srUrl = prop("schema.registry.url", SCHEMA_REGISTRY_URL);
    String clientId = prop("uami.client.id", UAMI_CLIENT_ID);
    String resource = prop("uami.resource", RESOURCE);
    String logicalCluster = prop("bearer.auth.logical.cluster", LOGICAL_CLUSTER);
    String identityPoolId = prop("bearer.auth.identity.pool.id", IDENTITY_POOL_ID);

    String endpointQuery = String.format(
        "api-version=2025-04-07&resource=%s&client_id=%s",
        resource, clientId);

    Map<String, String> config = new HashMap<>();
    config.put(SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE, "UAMI");
    config.put(SchemaRegistryClientConfig.BEARER_AUTH_UAMI_ENDPOINT_QUERY, endpointQuery);
    config.put(SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, logicalCluster);
    config.put(SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, identityPoolId);

    try (CachedSchemaRegistryClient client =
             new CachedSchemaRegistryClient(srUrl, 100, config)) {

      System.out.println("Fetching subjects from " + srUrl + " ...");
      Collection<String> subjects = client.getAllSubjects();
      System.out.println("Subjects: " + subjects);
    }
  }
}
