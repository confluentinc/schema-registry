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

import com.google.common.testing.FakeTicker;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.ConfigException;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.Mode;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.ParsedSchema;

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class CachedSchemaRegistryClientTest {

  private static final int CACHE_CAPACITY = 5;
  private static final String SCHEMA_STR_0 = avroSchemaString(0);
  private static final AvroSchema AVRO_SCHEMA_0 = avroSchema(0);
  private static final AvroSchema SCHEMA_WITH_DECIMAL = new AvroSchema(
          "{\n"
              + "    \"type\": \"record\",\n"
              + "    \"name\": \"MyRecord\",\n"
              + "    \"fields\": [\n"
              + "        {\n"
              + "            \"name\": \"field1\",\n"
              + "            \"type\": [\n"
              + "                \"null\",\n"
              + "                {\n"
              + "                    \"type\": \"bytes\",\n"
              + "                    \"scale\": 4,\n"
              + "                    \"precision\": 17,\n"
              + "                    \"logicalType\": \"decimal\"\n"
              + "                }\n"
              + "            ],\n"
              + "            \"default\": null\n"
              + "        }\n"
              + "    ]\n"
              + "}");
  private static final AvroSchema SCHEMA_WITH_DECIMAL2 = new AvroSchema(
          "{\n"
              + "    \"type\": \"record\",\n"
              + "    \"name\": \"MyRecord\",\n"
              + "    \"fields\": [\n"
              + "        {\n"
              + "            \"name\": \"field1\",\n"
              + "            \"type\": [\n"
              + "                \"null\",\n"
              + "                {\n"
              + "                    \"type\": \"bytes\",\n"
              + "                    \"logicalType\": \"decimal\",\n"
              + "                    \"precision\": 17,\n"
              + "                    \"scale\": 4\n"
              + "                }\n"
              + "            ],\n"
              + "            \"default\": null\n"
              + "        }\n"
              + "    ]\n"
              + "}");
  private static final String SUBJECT_0 = "foo";
  private static final int VERSION_1 = 1;
  private static final int VERSION_2 = 2;
  private static final int ID_25 = 25;
  private static final int ID_50 = 50;
  private static final io.confluent.kafka.schemaregistry.client.rest.entities.Schema SCHEMA_DETAILS
      = new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
          SUBJECT_0, 7, ID_25, AvroSchema.TYPE, Collections.emptyList(), SCHEMA_STR_0);

  private RestService restService;
  private CachedSchemaRegistryClient client;

  @Before
  public void setUp() {
    restService = createNiceMock(RestService.class);

    client = new CachedSchemaRegistryClient(restService, CACHE_CAPACITY, new HashMap<>());
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
        CACHE_CAPACITY,
        null,
        headers
    );

    verify(restService);
  }

  @Test
  public void testDuplicateClientNamespaceConfiguration() {
    Map<String, String> configs = Collections.singletonMap("key", "value");
    restService.configure(configs);
    expectLastCall();

    replay(restService);

    Map<String, String> duplicateConfigs = new HashMap<>();
    duplicateConfigs.put("key", "value");
    duplicateConfigs.put("schema.registry.key", "value");
    new CachedSchemaRegistryClient(
        restService,
        CACHE_CAPACITY,
        duplicateConfigs,
        null
    );

    verify(restService);
  }

  @Test
  public void testRegisterSchemaCache() throws Exception {
    // Expect one call to register schema
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25))
        .once();

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0)); // hit the cache

    verify(restService);
  }

  @Test
  public void testRegisterSchemaCacheWithVersionAndId() throws Exception {
    // Expect one call to register schema
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25))
        .once();

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0, VERSION_1, ID_25));
    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0, VERSION_1, ID_25)); // hit the cache

    verify(restService);
  }

  @Test
  public void testRegisterEquivalentSchemaDifferentid() throws Exception {
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25))
        .once();
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_50))
        .once();

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, SCHEMA_WITH_DECIMAL, VERSION_1, ID_25));
    assertEquals(ID_50, client.register(SUBJECT_0, SCHEMA_WITH_DECIMAL2, VERSION_2, ID_50));

    verify(restService);
  }

  @Test
  public void testRegisterOverCapacity() throws Exception {
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        anyString(), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25))
        .andReturn(new RegisterSchemaResponse(26))
        .andReturn(new RegisterSchemaResponse(27))
        .andReturn(new RegisterSchemaResponse(28))
        .andReturn(new RegisterSchemaResponse(29))
        .andReturn(new RegisterSchemaResponse(30));

    replay(restService);

    for (int i = 0; i != CACHE_CAPACITY; ++i) {
      client.register(SUBJECT_0, avroSchema(i));  // Each one results in new id.
    }

    // This call should exceed the cache capacity
    // Due to BoundedConcurrencyHashMap it should succeed
    client.register(SUBJECT_0, avroSchema(CACHE_CAPACITY));

    verify(restService);
  }

  @Test
  public void testIdCache() throws Exception {
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25));

    // Expect only one call to getId (the rest should hit the cache)
    expect(restService.getId(ID_25, SUBJECT_0))
        .andReturn(new SchemaString(SCHEMA_STR_0));

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(
        AVRO_SCHEMA_0.rawSchema(),
        ((AvroSchema) client.getSchemaBySubjectAndId(SUBJECT_0, ID_25)).rawSchema()
    );
    assertEquals(
        AVRO_SCHEMA_0.rawSchema(),
        ((AvroSchema) client.getSchemaBySubjectAndId(SUBJECT_0, ID_25)).rawSchema()
    ); // hit the cache

    verify(restService);
  }

  @Test
  public void testVersionCache() throws Exception {
    int version = 7;

    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25));

    // Expect only one call to lookup the subject (the rest should hit the cache)
    expect(restService.lookUpSubjectVersion(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean(),
        eq(true)))
        .andReturn(
            new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(SUBJECT_0, version,
                ID_25, AvroSchema.TYPE, Collections.emptyList(), SCHEMA_STR_0));

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

    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(subjectOne), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25));
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(subjectTwo), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25));

    expect(restService.getId(ID_25, subjectOne))
            .andReturn(schemaStringOne);
    expect(restService.getId(ID_25, subjectTwo))
            .andReturn(schemaStringTwo);

    replay(restService);

    // Make sure they still get the same ID
    assertEquals(ID_25, client.register(subjectOne, AVRO_SCHEMA_0));
    assertEquals(ID_25, client.register(subjectTwo, AVRO_SCHEMA_0));
    // Neither of these two schemas should be cached yet
    assertEquals(
        AVRO_SCHEMA_0.rawSchema(),
        ((AvroSchema) client.getSchemaBySubjectAndId(subjectOne, ID_25)).rawSchema()
    );
    assertEquals(
        AVRO_SCHEMA_0.rawSchema(),
        ((AvroSchema) client.getSchemaBySubjectAndId(subjectTwo, ID_25)).rawSchema()
    );
    // These two should be cached now
    assertEquals(
        AVRO_SCHEMA_0.rawSchema(),
        ((AvroSchema) client.getSchemaBySubjectAndId(subjectOne, ID_25)).rawSchema()
    );
    assertEquals(
        AVRO_SCHEMA_0.rawSchema(),
        ((AvroSchema) client.getSchemaBySubjectAndId(subjectTwo, ID_25)).rawSchema()
    );

    verify(restService);
  }

  @Test
  public void testLookUpEmptyRuleSetMetadataSchema() throws Exception {
    Metadata emptyMetadata = new Metadata(new HashMap<>(), new HashMap<>(), Collections.emptySet());
    RuleSet emptyRuleset = new RuleSet(Collections.emptyList(), Collections.emptyList());

    AvroSchema schemaWithEmptyFields = new AvroSchema(
        AVRO_SCHEMA_0.canonicalString(),
        new ArrayList<>(),
        new HashMap<>(),
        emptyMetadata,
        emptyRuleset,
        1,
        true
    );

    String subjectOne = "subjectOne";
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema emptySchemaDetails
        = new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
        SUBJECT_0, 1, ID_25, AvroSchema.TYPE, Collections.emptyList(), schemaWithEmptyFields.canonicalString());

    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(subjectOne), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25));
    expect(restService.lookUpSubjectVersion(anyObject(RegisterSchemaRequest.class), eq(subjectOne), anyBoolean(), anyBoolean()))
        .andReturn(emptySchemaDetails);
    expect(restService.getId(ID_25, subjectOne))
        .andReturn(new SchemaString(schemaWithEmptyFields.canonicalString()));

    replay(restService);

    assertEquals(ID_25, client.register(subjectOne, schemaWithEmptyFields));
    assertEquals(
        AVRO_SCHEMA_0.rawSchema(),
        ((AvroSchema) client.getSchemaBySubjectAndId(subjectOne, ID_25)).rawSchema()
    );
    assertEquals(ID_25, client.getId(subjectOne, schemaWithEmptyFields, false));

    verify(restService);
  }

  @Test
  public void testDeleteSchemaCache() throws Exception {
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25))
        .once();

    expect(restService.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT_0, false))
        .andReturn(Arrays.asList(0));

    expect(restService.deleteSubject(RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT_0, true))
            .andReturn(Arrays.asList(1));

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0)); // hit the cache

    assertEquals(Arrays.asList(0), client.deleteSubject(SUBJECT_0));
    assertEquals(Arrays.asList(1), client.deleteSubject(SUBJECT_0, true));

    verify(restService);
  }

  @Test
  public void testDeleteVersionCache() throws Exception {
    int version = 7;

    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25));

    // Expect only one call to lookup the subject (the rest should hit the cache)
    expect(restService.lookUpSubjectVersion(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean(), eq(true)))
        .andReturn(new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(SUBJECT_0,
                                                                                     version,
            ID_25, AvroSchema.TYPE, Collections.emptyList(), SCHEMA_STR_0));

    expect(restService.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
        SUBJECT_0, String.valueOf(version), false))
        .andReturn(0);

    expect(restService.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
            SUBJECT_0, String.valueOf(version), true))
            .andReturn(1);

    replay(restService);

    assertEquals(ID_25, client.register(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(version, client.getVersion(SUBJECT_0, AVRO_SCHEMA_0));
    assertEquals(version, client.getVersion(SUBJECT_0, AVRO_SCHEMA_0)); // hit the cache

    assertEquals(Integer.valueOf(0),
            client.deleteSchemaVersion(SUBJECT_0, String.valueOf(version)));

    assertEquals(Integer.valueOf(1),
            client.deleteSchemaVersion(SUBJECT_0, String.valueOf(version), true));

    verify(restService);
  }

  @Test
  public void testSetMode() throws Exception {
    final String mode = "READONLY";

    reset(restService);

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

    reset(restService);

    Mode modeGetResponse = new Mode(mode);
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

  @Test(expected = NullPointerException.class)
  public void testDeleteNullSubjectThrowsPermanent() throws Exception {
    client.deleteSubject(null, true);
  }

  @Test
  public void testThreadSafe() throws Exception {
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean()))
        .andReturn(new RegisterSchemaResponse(ID_25))
        .anyTimes();

    expect(restService.getId(ID_25, SUBJECT_0))
        .andReturn(new SchemaString(SCHEMA_STR_0))
        .anyTimes();

    expect(restService.lookUpSubjectVersion(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean(), anyBoolean()))
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
        CACHE_CAPACITY,
            config,
            null
    );
  }

  @Test
  public void testGetSchemas() throws Exception {
    expect(restService.registerSchema(anyObject(RegisterSchemaRequest.class),
        anyString(), anyBoolean()))
            .andReturn(new RegisterSchemaResponse(ID_25))
            .andReturn(new RegisterSchemaResponse(26))
            .andReturn(new RegisterSchemaResponse(27))
            .andReturn(new RegisterSchemaResponse(28))
            .andReturn(new RegisterSchemaResponse(29));

    List<Schema> schemas = IntStream.range(0, 5)
        .mapToObj( idx -> new Schema(
                SUBJECT_0,
                7,
                idx + 25,
                AvroSchema.TYPE,
                Collections.emptyList(),
                avroSchemaString(idx)
          )
        ).collect(Collectors.toList());


    expect(restService.getSchemas(anyString(), anyBoolean(), anyBoolean())).andReturn(schemas);

    replay(restService);

    for (int i = 0; i != CACHE_CAPACITY; ++i) {
      client.register(SUBJECT_0, avroSchema(i));  // Each one results in new id.
    }

    List<ParsedSchema> parsedSchemas = client.getSchemas(SUBJECT_0, false, true);
    assertEquals(5, parsedSchemas.size());
    IntStream.range(0, 5).forEach(idx -> {
            assertEquals(new AvroSchema(avroSchemaString(idx)), parsedSchemas.get(idx));
            assertEquals(AvroSchema.TYPE, parsedSchemas.get(idx).schemaType());
    });
  }

  @Test
  public void testGetSchemasEmptyReturn() throws Exception {
    List<Schema> emptyList = Collections.emptyList();
    expect(restService.getSchemas(anyString(), anyBoolean(), anyBoolean())).andReturn(emptyList);

    replay(restService);

    List<ParsedSchema> parsedSchemas = client.getSchemas(SUBJECT_0, false, true);
    assertEquals(0, parsedSchemas.size());
  }

  @Test
  public void testMissingIdCache() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.MISSING_ID_CACHE_TTL_CONFIG, 60L);
    configs.put(SchemaRegistryClientConfig.MISSING_SCHEMA_CACHE_TTL_CONFIG, 60L);

    FakeTicker fakeTicker = new FakeTicker();
    client = new CachedSchemaRegistryClient(
        restService,
        CACHE_CAPACITY,
        null,
        configs,
        null,
        fakeTicker
    );

    expect(restService.getId(ID_25, SUBJECT_0))
        .andThrow(new RestClientException("Schema 25 not found", 404, 40403))
        .andReturn(new SchemaString(SCHEMA_STR_0));

    replay(restService);

    try {
      client.getSchemaBySubjectAndId(SUBJECT_0, ID_25);
      fail();
    } catch (RestClientException rce) {
      assertEquals("Schema 25 not found; error code: 40403", rce.getMessage());
    }

    fakeTicker.advance(59, TimeUnit.SECONDS);

    // Should hit the cache
    try {
      client.getSchemaBySubjectAndId(SUBJECT_0, ID_25);
      fail();
    } catch (RestClientException rce) {
      assertEquals("Schema 25 not found; error code: 40403", rce.getMessage());
    }

    fakeTicker.advance(2, TimeUnit.SECONDS);
    Thread.sleep(100);
    assertNotNull(client.getSchemaBySubjectAndId(SUBJECT_0, ID_25));
  }

  @Test
  public void testMissingSubjectCache() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.MISSING_ID_CACHE_TTL_CONFIG, 60L);
    configs.put(SchemaRegistryClientConfig.MISSING_SCHEMA_CACHE_TTL_CONFIG, 60L);

    FakeTicker fakeTicker = new FakeTicker();
    client = new CachedSchemaRegistryClient(
            restService,
            CACHE_CAPACITY,
            null,
            configs,
            null,
            fakeTicker
    );

    int version = 7;
    expect(restService.lookUpSubjectVersion(anyObject(RegisterSchemaRequest.class),
            eq(SUBJECT_0), anyBoolean(),
            eq(false)))
            .andThrow(new RestClientException("Subject not found",
                    404, 40401))
            .andReturn(
                    new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(SUBJECT_0, version,
                            ID_25, AvroSchema.TYPE, Collections.emptyList(), SCHEMA_STR_0));

    replay(restService);

    try {
      client.getId(SUBJECT_0, AVRO_SCHEMA_0);
      fail();
    } catch (RestClientException rce) {
      assertEquals("Subject not found; error code: 40401", rce.getMessage());
    }

    fakeTicker.advance(59, TimeUnit.SECONDS);

    try {
      client.getId(SUBJECT_0, AVRO_SCHEMA_0);
      fail();
    } catch (RestClientException rce) {
      assertEquals("Schema not found; error code: 40403", rce.getMessage());
    }

    fakeTicker.advance(2, TimeUnit.SECONDS);
    Thread.sleep(100);
    client.getId(SUBJECT_0, AVRO_SCHEMA_0);
  }

  @Test
  public void testMissingSchemaCache() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.MISSING_ID_CACHE_TTL_CONFIG, 60L);
    configs.put(SchemaRegistryClientConfig.MISSING_SCHEMA_CACHE_TTL_CONFIG, 60L);

    FakeTicker fakeTicker = new FakeTicker();
    client = new CachedSchemaRegistryClient(
        restService,
        CACHE_CAPACITY,
        null,
        configs,
        null,
        fakeTicker
    );

    int version = 7;
    expect(restService.lookUpSubjectVersion(anyObject(RegisterSchemaRequest.class),
        eq(SUBJECT_0), anyBoolean(),
        eq(false)))
        .andThrow(new RestClientException("Schema not found",
            404, 40403))
        .andReturn(
            new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(SUBJECT_0, version,
                ID_25, AvroSchema.TYPE, Collections.emptyList(), SCHEMA_STR_0));

    replay(restService);

    try {
      client.getId(SUBJECT_0, AVRO_SCHEMA_0);
      fail();
    } catch (RestClientException rce) {
      assertEquals("Schema not found; error code: 40403", rce.getMessage());
    }

    fakeTicker.advance(59, TimeUnit.SECONDS);

    try {
      client.getId(SUBJECT_0, AVRO_SCHEMA_0);
      fail();
    } catch (RestClientException rce) {
      assertEquals("Schema not found; error code: 40403", rce.getMessage());
    }

    fakeTicker.advance(2, TimeUnit.SECONDS);
    Thread.sleep(100);
    client.getId(SUBJECT_0, AVRO_SCHEMA_0);
  }


  private static AvroSchema avroSchema(final int i) {
    return new AvroSchema(avroSchemaString(i));
  }

  private static String avroSchemaString(final int i) {
    return "{\"type\": \"record\", \"name\": \"Blah" + i + "\", "
        + "\"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
  }
}
