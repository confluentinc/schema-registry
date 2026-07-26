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

package io.confluent.kafka.schemaregistry.client.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.schema.avro.SchemaRegistryRpcClient;
import org.apache.kafka.server.schema.SchemaNotFoundException;
import org.junit.Before;
import org.junit.Test;

/**
 * Drives the transport through {@link CachedSchemaRegistryClient} rather than calling
 * {@code httpRequest} directly, because the point of the {@code RestService} seam is that the
 * existing client stack works unchanged on top of it. A test that called the seam directly would
 * pass even if the response shapes did not match what the client expects to deserialise.
 */
public class KafkaRpcRestServiceTest {

  private static final String SCHEMA_TEXT =
      "{\"type\":\"record\",\"name\":\"Order\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
  private static final Uuid GUID = Uuid.randomUuid();

  private RecordingRpcClient rpc;
  private CachedSchemaRegistryClient client;

  @Before
  public void setUp() {
    rpc = new RecordingRpcClient();
    client = new CachedSchemaRegistryClient(new KafkaRpcRestService(rpc), 10);
  }

  @Test
  public void testRegisterGoesThroughTheRegisterRpc() throws Exception {
    int id = client.register("orders-value", new AvroSchema(SCHEMA_TEXT));

    assertEquals(7, id);
    assertEquals(List.of("register:orders-value:AVRO"), rpc.calls);
  }

  @Test
  public void testLookupByIdReturnsTheSchema() throws Exception {
    // Registering first so the cache is populated the way the serde would populate it, then reading
    // an id the cache does not hold.
    assertEquals(SCHEMA_TEXT, client.getSchemaById(99).canonicalString());
    assertEquals(List.of("lookupById:99"), rpc.calls);
  }

  @Test
  public void testLatestVersionCarriesSubjectAndVersion() throws Exception {
    SchemaMetadata metadata = client.getLatestSchemaMetadata("orders-value");

    assertEquals(7, metadata.getId());
    assertEquals(3, metadata.getVersion());
    assertEquals(SCHEMA_TEXT, metadata.getSchema());
    assertEquals(List.of("lookupLatest:orders-value"), rpc.calls);
  }

  @Test
  public void testGetIdLooksUpBySchemaText() throws Exception {
    int id = client.getId("orders-value", new AvroSchema(SCHEMA_TEXT));

    assertEquals(7, id);
    assertEquals(List.of("lookupBySchema:orders-value:AVRO"), rpc.calls);
  }

  @Test
  public void testMissingSubjectBecomesA404() {
    rpc.notFound = true;
    try {
      client.getLatestSchemaMetadata("absent-value");
      fail("expected a not-found error");
    } catch (IOException | RestClientException e) {
      RestClientException rest = (RestClientException) e;
      assertEquals(404, rest.getStatus());
      assertEquals(40401, rest.getErrorCode());
    }
  }

  @Test
  public void testLookupByExplicitVersionFailsRatherThanReturningLatest() throws Exception {
    // The RPC supports this lookup but the transport does not expose it yet. Silently answering
    // with the latest version would corrupt a reader that asked for a specific one.
    KafkaRpcRestService service = new KafkaRpcRestService(rpc);
    try {
      service.getVersion("orders-value", 2);
      fail("expected an explicit failure");
    } catch (IOException | RestClientException e) {
      RestClientException rest = (RestClientException) e;
      assertEquals(501, rest.getStatus());
      assertTrue(rest.getMessage(), rest.getMessage().contains("not yet implemented"));
    }
    assertEquals(List.of(), rpc.calls);
  }

  @Test
  public void testAnUnsupportedPathFailsLoudlyInsteadOfReachingHttp() throws Exception {
    // The whole reason for overriding httpRequest rather than the semantic methods: anything not
    // handled must fail here, not attempt a connection to an endpoint that does not exist.
    KafkaRpcRestService service = new KafkaRpcRestService(rpc);
    try {
      service.getAllSubjects();
      fail("expected an unsupported-path failure");
    } catch (IOException | RestClientException e) {
      RestClientException rest = (RestClientException) e;
      assertEquals(501, rest.getStatus());
      assertTrue(rest.getMessage(), rest.getMessage().contains("not supported over Kafka RPCs"));
    }
  }

  @Test
  public void testGuidIsRenderedAsAStandardUuid() throws Exception {
    // Kafka renders a Uuid in base64, but the serde parses a schema GUID with UUID.fromString.
    // Emitting Kafka's rendering makes every serialization fail on "Invalid UUID string".
    String guid = new KafkaRpcRestService(rpc).getId(99).getGuid();

    assertEquals(new java.util.UUID(GUID.getMostSignificantBits(), GUID.getLeastSignificantBits())
        .toString(), guid);
  }

  @Test
  public void testGuidLookupAcceptsTheStandardUuidRendering() throws Exception {
    KafkaRpcRestService service = new KafkaRpcRestService(rpc);
    String standard = new java.util.UUID(
        GUID.getMostSignificantBits(), GUID.getLeastSignificantBits()).toString();

    service.getByGuid(standard, null);

    assertEquals(List.of("lookupByGuid:" + GUID), rpc.calls);
  }

  @Test
  public void testAssociationLookupAnswersTheDefaultSubjectNameStrategy() throws Exception {
    // The default strategy asks for the topic's association before deriving a subject name, and
    // addresses the topic as a named resource within a cluster.
    rpc.topicSubject = "orders-from-association";
    KafkaRpcRestService service = new KafkaRpcRestService(rpc);

    List<Association> associations = service.getAssociationsByResourceName(
        Map.of(), "orders", "cluster-1", "topic", List.of("value"), null, 0, -1);

    assertEquals(1, associations.size());
    assertEquals("orders-from-association", associations.get(0).getSubject());
    assertEquals("value", associations.get(0).getAssociationType());
    // Only the requested binding is looked up, not both.
    assertEquals(List.of("subjectForTopic:orders:false"), rpc.calls);
  }

  @Test
  public void testAssociationLookupIsEmptyWhenTheTopicHasNone() throws Exception {
    // An empty list is what tells the strategy to fall back to conventional topic-value naming, so
    // this must not be an error.
    KafkaRpcRestService service = new KafkaRpcRestService(rpc);

    assertEquals(List.of(), service.getAssociationsByResourceName(
        Map.of(), "orders", "cluster-1", "topic", List.of("value"), null, 0, -1));
  }

  @Test
  public void testCloseClosesTheTransport() throws Exception {
    KafkaRpcRestService service = new KafkaRpcRestService(rpc);
    service.close();
    assertTrue(rpc.closed);
  }

  private static class RecordingRpcClient implements SchemaRegistryRpcClient {
    private final List<String> calls = new ArrayList<>();
    private boolean notFound;
    private boolean closed;
    private String topicSubject;

    private RegisteredSchema schema(String subject) {
      if (notFound) {
        throw new SchemaNotFoundException("Subject '" + subject + "' not found.");
      }
      return new RegisteredSchema(7, GUID, 3, subject, "AVRO", SCHEMA_TEXT);
    }

    @Override
    public RegisteredSchema register(String subject, String schemaType, String schemaText) {
      calls.add("register:" + subject + ":" + schemaType);
      return schema(subject);
    }

    @Override
    public RegisteredSchema lookupById(int id) {
      calls.add("lookupById:" + id);
      return schema("orders-value");
    }

    @Override
    public RegisteredSchema lookupByGuid(Uuid guid) {
      calls.add("lookupByGuid:" + guid);
      return schema("orders-value");
    }

    @Override
    public RegisteredSchema lookupBySchema(String subject, String schemaType, String schemaText) {
      calls.add("lookupBySchema:" + subject + ":" + schemaType);
      return schema(subject);
    }

    @Override
    public RegisteredSchema lookupLatest(String subject) {
      calls.add("lookupLatest:" + subject);
      return schema(subject);
    }

    @Override
    public Optional<String> subjectForTopic(String topic, boolean isKey) {
      calls.add("subjectForTopic:" + topic + ":" + isKey);
      return Optional.ofNullable(topicSubject);
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
