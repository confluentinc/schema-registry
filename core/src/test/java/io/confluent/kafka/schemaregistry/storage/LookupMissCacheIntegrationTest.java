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

package io.confluent.kafka.schemaregistry.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class LookupMissCacheIntegrationTest extends ClusterTestHarness {

  private static final String AVRO_1 =
      "{\"type\":\"record\",\"name\":\"r1\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
  private static final String AVRO_2 =
      "{\"type\":\"record\",\"name\":\"r2\",\"fields\":[{\"name\":\"f2\",\"type\":\"int\"}]}";

  private RestApp other;

  public LookupMissCacheIntegrationTest() {
    super(1, true, CompatibilityLevel.NONE.name);
  }

  @AfterEach
  public void tearDownOther() throws Exception {
    if (other != null) {
      other.stop();
    }
  }

  @Test
  public void testAvroMissIsCachedUntilRegistered() throws Exception {
    testMissIsCachedUntilRegistered(AvroSchema.TYPE, AVRO_1, AVRO_2);
  }

  @Test
  public void testJsonMissIsCachedUntilRegistered() throws Exception {
    testMissIsCachedUntilRegistered(JsonSchema.TYPE,
        "{\"type\":\"object\",\"properties\":{\"f1\":{\"type\":\"string\"}}}",
        "{\"type\":\"object\",\"properties\":{\"f2\":{\"type\":\"integer\"}}}");
  }

  @Test
  public void testProtobufMissIsCachedUntilRegistered() throws Exception {
    testMissIsCachedUntilRegistered(ProtobufSchema.TYPE,
        "syntax = \"proto3\";\nmessage M1 {\n  string f1 = 1;\n}\n",
        "syntax = \"proto3\";\nmessage M2 {\n  int32 f2 = 1;\n}\n");
  }

  private void testMissIsCachedUntilRegistered(String type, String looked, String other)
      throws Exception {
    String subject = "s";
    assertErrorCode(40401, () -> lookUp(type, looked, subject, false));
    assertErrorCode(40401, () -> lookUp(type, looked, subject, false));
    assertEquals(1, missCache(restApp).size());

    // Another schema of the subject: still a miss, now with its subject found
    restApp.restClient.registerSchema(other, type, Collections.emptyList(), subject);
    assertErrorCode(40403, () -> lookUp(type, looked, subject, false));

    restApp.restClient.registerSchema(looked, type, Collections.emptyList(), subject);
    assertEquals(2, lookUp(type, looked, subject, false).getVersion());
  }

  @Test
  public void testSoftDeleteAndReregister() throws Exception {
    String subject = "s";
    restApp.restClient.registerSchema(AVRO_1, subject);
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1");

    assertErrorCode(40401, () -> lookUp(AvroSchema.TYPE, AVRO_1, subject, false));
    // The cached miss is for lookups without deleted versions only
    assertEquals(1, lookUp(AvroSchema.TYPE, AVRO_1, subject, true).getVersion());

    restApp.restClient.registerSchema(AVRO_1, subject);
    assertNotNull(lookUp(AvroSchema.TYPE, AVRO_1, subject, false));
  }

  @Test
  public void testConfigChangeDropsMiss() throws Exception {
    String subject = "s";
    // Version 1 carries metadata; version 2, the latest, has none to pass on to a lookup
    RegisterSchemaRequest withMetadata = new RegisterSchemaRequest(new AvroSchema(AVRO_1));
    withMetadata.setMetadata(new Metadata(null, Collections.singletonMap("k", "v"), null));
    restApp.restClient.registerSchema(withMetadata, subject, false);
    RegisterSchemaRequest withoutMetadata = new RegisterSchemaRequest(new AvroSchema(AVRO_2));
    withoutMetadata.setMetadata(new Metadata(null, null, null));
    restApp.restClient.registerSchema(withoutMetadata, subject, false);

    assertErrorCode(40403, () -> lookUp(AvroSchema.TYPE, AVRO_1, subject, false));

    // The override gives the lookup the metadata of version 1
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setOverrideMetadata(new Metadata(null, Collections.singletonMap("k", "v"), null));
    restApp.restClient.updateConfig(config, subject);
    assertEquals(1, lookUp(AvroSchema.TYPE, AVRO_1, subject, false).getVersion());
  }

  @Test
  public void testNewContextDropsMiss() throws Exception {
    // A context without the subject, which the lookup skips
    restApp.restClient.registerSchema(AVRO_2, ":.other:bar");
    assertErrorCode(40401, () -> lookUp(AvroSchema.TYPE, AVRO_1, "foo", false));

    restApp.restClient.registerSchema(AVRO_1, ":.ctx:foo");
    Schema found = lookUp(AvroSchema.TYPE, AVRO_1, "foo", false);
    assertEquals(":.ctx:foo", found.getSubject());
    assertEquals(1, found.getVersion());
  }

  @Test
  public void testFollowerDropsMissOnReadingTheWrite() throws Exception {
    other = new RestApp(choosePort(), null, brokerList, KAFKASTORE_TOPIC,
        CompatibilityLevel.NONE.name, false, null);
    other.start();
    String subject = "s";

    assertErrorCode(40401, () -> other.restClient.lookUpSubjectVersion(
        AVRO_1, AvroSchema.TYPE, Collections.emptyList(), subject, false));
    assertEquals(1, missCache(other).size());

    restApp.restClient.registerSchema(AVRO_1, subject);
    ((KafkaSchemaRegistry) other.schemaRegistry()).getKafkaStore()
        .waitUntilKafkaReaderReachesLastOffset(30000);
    assertEquals(1, other.restClient.lookUpSubjectVersion(
        AVRO_1, AvroSchema.TYPE, Collections.emptyList(), subject, false).getVersion());
  }

  @Test
  public void testDisabled() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.SCHEMA_LOOKUP_MISS_CACHE_SIZE_CONFIG, 0);
    other = new RestApp(choosePort(), null, brokerList, KAFKASTORE_TOPIC,
        CompatibilityLevel.NONE.name, false, props);
    other.start();
    assertNull(((AbstractSchemaRegistry) other.schemaRegistry()).lookupMissCache);

    assertErrorCode(40401, () -> other.restClient.lookUpSubjectVersion(
        AVRO_1, AvroSchema.TYPE, Collections.emptyList(), "s", false));
  }

  private Schema lookUp(String type, String schema, String subject, boolean deleted)
      throws Exception {
    return restApp.restClient.lookUpSubjectVersion(
        schema, type, Collections.emptyList(), subject, deleted);
  }

  private static LookupMissCache missCache(RestApp app) {
    return ((AbstractSchemaRegistry) app.schemaRegistry()).lookupMissCache;
  }

  private static void assertErrorCode(int errorCode, Lookup lookup) {
    RestClientException e = assertThrows(RestClientException.class, lookup::run);
    assertEquals(errorCode, e.getErrorCode());
  }

  private interface Lookup {
    void run() throws Exception;
  }
}
