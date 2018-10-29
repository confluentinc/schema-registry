/*
 * Copyright 2018 Confluent Inc.
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

import org.apache.avro.Schema;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.HashMap;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeGetResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.RestService;

import static org.easymock.EasyMock.anyBoolean;
import org.apache.avro.Schema;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.HashMap;
import java.util.Arrays;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.IntStream;
import org.junit.Before;

public class CachedSchemaRegistryClientTest {

  private static final int IDENTITY_MAP_CAPACITY = 5;
  private static final String SCHEMA_STR_0 = avroSchemaString(0);
  private static final Schema AVRO_SCHEMA_0 = avroSchema(0);
  private static final String SUBJECT_0 = "foo";
  private static final int VERSION_1 = 1;
  private static final int ID_25 = 25;
  private static final io.confluent.kafka.schemaregistry.client.rest.entities.Schema SCHEMA_DETAILS
      = new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
          SUBJECT_0, 7, ID_25, AvroSchema.AVRO, SCHEMA_STR_0);

  private RestService restService;
  private CachedSchemaRegistryClient client;

  @Before
  public void setUp() {
    restService = createNiceMock(RestService.class);

    client = new CachedSchemaRegistryClient(restService, IDENTITY_MAP_CAPACITY, new HashMap<>());
  }

  @Test
  public void testHttpHeaderConfiguration() {
    Map<String, String> headers = Collections.singletonMap("Authorization", "Bearer RGFuIGlzIGEgZG93bmVy");
    restService.setHttpHeaders(eq(headers));
    expectLastCall();

    replay(restService);
    // Headers should be configured regardless of the value of the "configs" parameter
    new CachedSchemaRegistryClient(
        restService,
        IDENTITY_MAP_CAPACITY,
        null,
        headers
    );

    verify(restService);
  }

  @Test
  public void testRegisterSchemaCache() throws Exception {
    // Expect one call to register schema
    expect(restService.registerSchema(anyString(), eq(SUBJECT_0)))
        .andReturn(ID_25)
        .once();

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0)); // hit the cache

    verify(restService);
  }

  @Test
  public void testRegisterSchemaCacheWithVersionAndId() throws Exception {
    // Expect one call to register schema
    expect(restService.registerSchema(anyString(), eq(SUBJECT_0), eq(VERSION_1), eq(ID_25)))
        .andReturn(ID_25)
        .once();

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0, VERSION_1, ID_25));
    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0, VERSION_1, ID_25)); // hit the cache

    verify(restService);
  }

  @Test
  public void testRegisterOverCapacity() throws Exception {
    expect(restService.registerSchema(anyString(), anyString()))
        .andReturn(ID_25)
        .andReturn(26)
        .andReturn(27)
        .andReturn(28)
        .andReturn(29);

    replay(restService);

    for (int i = 0; i != IDENTITY_MAP_CAPACITY; ++i) {
      client.register(SUBJECT_0, avroSchema(i));  // Each one results in new id.
    }

    try {
      // This call should exceed the identityMapCapacity
      client.register(SUBJECT_0, avroSchema(IDENTITY_MAP_CAPACITY));
      fail();
    } catch (IllegalStateException e) {
      //
    }

    verify(restService);
  }

  @Test
  public void testIdCache() throws Exception {
    expect(restService.registerSchema(anyString(), eq(SUBJECT_0)))
        .andReturn(ID_25);

    // Expect only one call to getId (the rest should hit the cache)
    expect(restService.getId(ID_25))
        .andReturn(new SchemaString(SCHEMA_STR_0));

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(AVRO_SCHEMA_0, client.getBySubjectAndId(SUBJECT_0, ID_25));
    assertEquals(AVRO_SCHEMA_0, client.getBySubjectAndId(SUBJECT_0, ID_25)); // hit the cache

    verify(restService);
  }

  @Test
  public void testVersionCache() throws Exception {
    int version = 7;

    expect(restService.registerSchema(anyString(), eq(SUBJECT_0)))
        .andReturn(ID_25);

    // Expect only one call to lookup the subject (the rest should hit the cache)
    expect(restService.lookUpSubjectVersion(anyString(), eq(SUBJECT_0), eq(true)))
        .andReturn(
            new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(SUBJECT_0, version,
                ID_25, AvroSchema.AVRO, SCHEMA_STR_0));

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(version, client.getVersion(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(version, client.getVersion(SUBJECT_0, AVRO_SCHEMA_0)); // hit the cache

    verify(restService);
  }

  @Test
  public void testIdenticalSchemas() throws Exception {
    SchemaString schemaStringOne = new SchemaString(SCHEMA_STR_0);
    SchemaString schemaStringTwo = new SchemaString(SCHEMA_STR_0);

    String subjectOne = "subjectOne";
    String subjectTwo = "subjectTwo";

    expect(restService.registerSchema(anyString(), eq(subjectOne)))
        .andReturn(ID_25);
    expect(restService.registerSchema(anyString(), eq(subjectTwo)))
        .andReturn(ID_25);

    expect(restService.getId(eq(ID_25)))
            .andReturn(schemaStringOne);
    expect(restService.getId(eq(ID_25)))
            .andReturn(schemaStringTwo);

    replay(restService);

    // Make sure they still get the same ID
    assertEquals(ID_25, client.register(subjectOne, AVRO_SCHEMA_0));
    assertEquals(ID_25, client.register(subjectTwo, AVRO_SCHEMA_0));
    // Neither of these two schemas should be cached yet
    assertEquals(AVRO_SCHEMA_0, client.getBySubjectAndId(subjectOne, ID_25));
    assertEquals(AVRO_SCHEMA_0, client.getBySubjectAndId(subjectTwo, ID_25));
    // These two should be cached now
    assertEquals(AVRO_SCHEMA_0, client.getBySubjectAndId(subjectOne, ID_25));
    assertEquals(AVRO_SCHEMA_0, client.getBySubjectAndId(subjectTwo, ID_25));

    verify(restService);
  }

  @Test
  public void testDeleteSchemaCache() throws Exception {
    expect(restService.registerSchema(anyString(), eq(SUBJECT_0)))
        .andReturn(ID_25)
        .once();

    expect(restService.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT_0))
        .andReturn(Arrays.asList(0));

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0)); // hit the cache

    assertEquals(Arrays.asList(0), client.deleteSubject(SUBJECT_0));

    verify(restService);
  }

  @Test
  public void testDeleteVersionCache() throws Exception {
    int version = 7;

    expect(restService.registerSchema(anyString(), eq(SUBJECT_0)))
        .andReturn(ID_25);

    // Expect only one call to lookup the subject (the rest should hit the cache)
    expect(restService.lookUpSubjectVersion(anyString(), eq(SUBJECT_0), eq(true)))
        .andReturn(new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(SUBJECT_0,
                                                                                     version,
            ID_25,
            AvroSchema.AVRO,
            SCHEMA_STR_0));

    expect(restService.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
        SUBJECT_0,
                                           String.valueOf(version)))
        .andReturn(0);

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(version, client.getVersion(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(version, client.getVersion(SUBJECT_0, AVRO_SCHEMA_0)); // hit the cache

    assertEquals(Integer.valueOf(0),
        client.deleteSchemaVersion(SUBJECT_0, String.valueOf(version)));

    verify(restService);
  }

  @Test
  public void testSetMode() throws Exception {
    final String mode = "READONLY";

    EasyMock.reset(restService);

    ModeUpdateRequest modeUpdateRequest = new ModeUpdateRequest();
    modeUpdateRequest.setMode(mode);
    expect(restService.setMode(eq(mode))).andReturn(modeUpdateRequest);

    replay(restService);

    assertEquals(mode, client.setMode(mode));

    verify(restService);
  }

  @Test
  public void testGetMode() throws Exception {
    final String mode = "READONLY";

    EasyMock.reset(restService);

    ModeGetResponse modeGetResponse = new ModeGetResponse(mode);
    expect(restService.getMode()).andReturn(modeGetResponse);

    replay(restService);

    assertEquals(mode, client.getMode());

    verify(restService);
  }

  public void testDeleteVersionNotInVersionCache() throws Exception {
    expect(client.deleteSchemaVersion(Collections.emptyMap(), SUBJECT_0, "0"))
        .andReturn(10);

    replay(restService);

    final Integer result = client.deleteSchemaVersion(Collections.emptyMap(), SUBJECT_0, "0");

    assertEquals((Integer)10, result);
    verify(restService);
  }

  @Test(expected = NullPointerException.class)
  public void testDeleteNullSubjectThrows() throws Exception {
    client.deleteSubject(null);
  }

  @Test
  public void testThreadSafe() throws Exception {
    expect(restService.registerSchema(anyString(), eq(SUBJECT_0)))
        .andReturn(ID_25)
        .anyTimes();

    expect(restService.getId(ID_25))
        .andReturn(new SchemaString(SCHEMA_STR_0))
        .anyTimes();

    expect(restService.lookUpSubjectVersion(eq(AVRO_SCHEMA_0.toString()), eq(SUBJECT_0), anyBoolean()))
        .andReturn(SCHEMA_DETAILS)
        .anyTimes();

    replay(restService);

    IntStream.range(0, 1_000)
        .parallel()
        .forEach(idx -> {
          try {
            final int id = client.register(SUBJECT_0, AVRO_SCHEMA_0);
            final int version = client.getVersion(SUBJECT_0, AVRO_SCHEMA_0);
            client.getId(SUBJECT_0, AVRO_SCHEMA_0);
            client.getBySubjectAndId(SUBJECT_0, id);
            client.deleteSchemaVersion(SUBJECT_0, String.valueOf(version));
            client.deleteSubject(SUBJECT_0);
          } catch (final IOException | RestClientException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test(expected = ConfigException.class)
  public void testMultipleCredentialProvider() throws Exception {
    Map<String, String> config = new HashMap<>();
    // Default credential provider
    config.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "URL");
    config.put(SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE, "STATIC_TOKEN");
    config.put(SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG, "auth-token");

    // Throws ConfigException if both credential providers are fully configured.
    new CachedSchemaRegistryClient(
            // Sets initial credential set for URL provider
            new RestService("http://user:password@sr.com:8020"),
            IDENTITY_MAP_CAPACITY,
            config,
            null
    );
  }

  private static Schema avroSchema(final int i) {
    return new Schema.Parser().parse(avroSchemaString(i));
  }

  private static String avroSchemaString(final int i) {
    return "{\"type\": \"record\", \"name\": \"Blah" + i + "\", "
        + "\"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
  }
}
