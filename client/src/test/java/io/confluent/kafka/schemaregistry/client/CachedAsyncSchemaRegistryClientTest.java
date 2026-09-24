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

package io.confluent.kafka.schemaregistry.client;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.AsyncRestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class CachedAsyncSchemaRegistryClientTest {

  private static final int CACHE_CAPACITY = 5;
  private static final String SUBJECT = "foo";
  private static final int ID = 25;
  private static final String RECORD =
      "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}";
  private static final String INNER =
      "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}";
  private static final String OUTER =
      "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":[{\"name\":\"inner\",\"type\":\"Inner\"}]}";
  private static final AvroSchema AVRO_SCHEMA = new AvroSchema(RECORD);

  private AsyncRestService restService;
  private CachedAsyncSchemaRegistryClient client;

  @Before
  public void setUp() {
    restService = createMock(AsyncRestService.class);
    client = newClient(Collections.emptyMap());
  }

  @Test
  public void testGetSchemaByIdIsCached() throws Exception {
    expect(restService.getId(ID, "")).andReturn(completedFuture(new SchemaString(RECORD))).once();
    replay(restService);

    assertEquals(AVRO_SCHEMA.canonicalString(), await(client.getSchemaById(ID)).canonicalString());
    assertEquals(AVRO_SCHEMA.canonicalString(), await(client.getSchemaById(ID)).canonicalString());

    verify(restService);
  }

  @Test
  public void testConcurrentRequestsShareOneRegistryCall() throws Exception {
    CompletableFuture<SchemaString> response = new CompletableFuture<>();
    expect(restService.getId(ID, "")).andReturn(response).once();
    replay(restService);

    CompletableFuture<ParsedSchema> first = client.getSchemaById(ID);
    CompletableFuture<ParsedSchema> second = client.getSchemaById(ID);
    assertFalse(first.isDone());
    response.complete(new SchemaString(RECORD));

    assertEquals(AVRO_SCHEMA.canonicalString(), await(first).canonicalString());
    assertEquals(AVRO_SCHEMA.canonicalString(), await(second).canonicalString());
    verify(restService);
  }

  @Test
  public void testCancellingOneCallerDoesNotAffectOthers() throws Exception {
    CompletableFuture<SchemaString> response = new CompletableFuture<>();
    expect(restService.getId(ID, "")).andReturn(response).once();
    replay(restService);

    CompletableFuture<Schema> first = client.getSchemaEntityBySubjectAndId(null, ID);
    first.cancel(true);
    CompletableFuture<Schema> second = client.getSchemaEntityBySubjectAndId(null, ID);
    response.complete(new SchemaString(RECORD));

    assertEquals(RECORD, await(second).getSchema());
    verify(restService);
  }

  @Test
  public void testFailedLoadIsNotCached() throws Exception {
    expect(restService.getId(ID, ""))
        .andReturn(failedFuture(new IOException("connection reset"))).once();
    expect(restService.getId(ID, "")).andReturn(completedFuture(new SchemaString(RECORD))).once();
    replay(restService);

    awaitFailure(client.getSchemaById(ID), IOException.class);
    assertEquals(AVRO_SCHEMA.canonicalString(), await(client.getSchemaById(ID)).canonicalString());

    verify(restService);
  }

  @Test
  public void testNotFoundIsRememberedForMissingIdTtl() throws Exception {
    client = newClient(Collections.singletonMap(
        SchemaRegistryClientConfig.MISSING_ID_CACHE_TTL_CONFIG, "60"));
    expect(restService.getId(ID, ""))
        .andReturn(failedFuture(new RestClientException("Schema not found", 404, 40403))).once();
    replay(restService);

    awaitFailure(client.getSchemaById(ID), RestClientException.class);
    // Answered from the missing cache, without calling the registry again
    RestClientException e = awaitFailure(client.getSchemaById(ID), RestClientException.class);
    assertEquals(40403, e.getErrorCode());

    verify(restService);
  }

  @Test
  public void testRegisterIsCachedAndInvalidatesLatestVersion() throws Exception {
    Schema latest = new Schema(SUBJECT, 1, ID, AvroSchema.TYPE, Collections.emptyList(), RECORD);
    expect(restService.getLatestVersion(SUBJECT)).andReturn(completedFuture(latest)).times(2);
    expect(restService.registerSchema(
        anyObject(RegisterSchemaRequest.class), eq(SUBJECT), eq(false)))
        .andReturn(completedFuture(new RegisterSchemaResponse(ID))).once();
    replay(restService);

    await(client.getLatestSchemaMetadata(SUBJECT));
    await(client.getLatestSchemaMetadata(SUBJECT));
    assertEquals(ID, (int) await(client.register(SUBJECT, AVRO_SCHEMA)));
    assertEquals(ID, (int) await(client.register(SUBJECT, AVRO_SCHEMA)));
    // Registering made the cached latest version stale
    await(client.getLatestSchemaMetadata(SUBJECT));

    verify(restService);
  }

  @Test
  public void testGetIdLooksUpAgainWhenRegisteredResponseHasNoVersion() throws Exception {
    expect(restService.registerSchema(
        anyObject(RegisterSchemaRequest.class), eq(SUBJECT), eq(false)))
        .andReturn(completedFuture(new RegisterSchemaResponse(ID))).once();
    expect(restService.lookUpSubjectVersion(
        anyObject(RegisterSchemaRequest.class), eq(SUBJECT), eq(false), eq(false)))
        .andReturn(completedFuture(
            new Schema(SUBJECT, 3, ID, AvroSchema.TYPE, Collections.emptyList(), RECORD)))
        .once();
    replay(restService);

    await(client.register(SUBJECT, AVRO_SCHEMA));
    assertEquals(3, (int) await(client.getIdWithResponse(SUBJECT, AVRO_SCHEMA, false)).getVersion());
    assertEquals(3, (int) await(client.getIdWithResponse(SUBJECT, AVRO_SCHEMA, false)).getVersion());

    verify(restService);
  }

  @Test
  public void testGetIdCachesSchemaById() throws Exception {
    expect(restService.lookUpSubjectVersion(
        anyObject(RegisterSchemaRequest.class), eq(SUBJECT), eq(false), eq(false)))
        .andReturn(completedFuture(
            new Schema(SUBJECT, 3, ID, AvroSchema.TYPE, Collections.emptyList(), RECORD)))
        .once();
    replay(restService);

    assertEquals(ID, (int) await(client.getId(SUBJECT, AVRO_SCHEMA)));
    // Served from the id cache, which the lookup populated
    assertEquals(AVRO_SCHEMA.canonicalString(), await(client.getSchemaById(ID)).canonicalString());

    verify(restService);
  }

  @Test
  public void testParseSchemaPrefetchesReferencesWithoutBlocking() throws Exception {
    CompletableFuture<Schema> reference = new CompletableFuture<>();
    expect(restService.getVersion("inner-value", 1, true)).andReturn(reference).once();
    replay(restService);

    CompletableFuture<Optional<ParsedSchema>> parsed = client.parseSchema(outerSchema());
    // Returned while the reference is still being fetched
    assertFalse(parsed.isDone());
    reference.complete(
        new Schema("inner-value", 1, 10, AvroSchema.TYPE, Collections.emptyList(), INNER));

    assertEquals("Outer", await(parsed).get().name());
    verify(restService);
  }

  @Test
  public void testParseSchemaFailsWhenReferenceCannotBeFetched() throws Exception {
    expect(restService.getVersion("inner-value", 1, true))
        .andReturn(failedFuture(new RestClientException("Version not found", 404, 40402)))
        .once();
    replay(restService);

    awaitFailure(client.parseSchema(outerSchema()), RestClientException.class);

    verify(restService);
  }

  @Test
  public void testInvalidSchemaParsesToEmpty() throws Exception {
    replay(restService);

    Schema invalid =
        new Schema(null, null, null, AvroSchema.TYPE, Collections.emptyList(), "not a schema");
    assertFalse(await(client.parseSchema(invalid)).isPresent());
  }

  @Test
  public void testGetSchemaByIdFailsForInvalidSchema() throws Exception {
    expect(restService.getId(ID, ""))
        .andReturn(completedFuture(new SchemaString("not a schema"))).once();
    replay(restService);

    awaitFailure(client.getSchemaById(ID), IOException.class);

    verify(restService);
  }

  @Test
  public void testResetClearsCaches() throws Exception {
    expect(restService.getId(ID, "")).andReturn(completedFuture(new SchemaString(RECORD))).times(2);
    replay(restService);

    await(client.getSchemaById(ID));
    client.reset();
    await(client.getSchemaById(ID));

    verify(restService);
  }

  private CachedAsyncSchemaRegistryClient newClient(Map<String, ?> configs) {
    return new CachedAsyncSchemaRegistryClient(restService, CACHE_CAPACITY, null, configs);
  }

  private static Schema outerSchema() {
    return new Schema(SUBJECT, null, null, AvroSchema.TYPE,
        Collections.singletonList(new SchemaReference("Inner", "inner-value", 1)), OUTER);
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    return future.get(10, TimeUnit.SECONDS);
  }

  private static <E extends Throwable> E awaitFailure(
      CompletableFuture<?> future, Class<E> type) throws Exception {
    try {
      future.get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      assertTrue("Expected " + type.getSimpleName() + " but got " + e.getCause(),
          type.isInstance(e.getCause()));
      return type.cast(e.getCause());
    }
    throw new AssertionError("Expected the future to fail with " + type.getSimpleName());
  }
}
