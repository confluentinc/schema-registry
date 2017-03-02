/**
 * Copyright 2015 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.apache.avro.Schema;
import org.junit.Test;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CachedSchemaRegistryClientTest {

  @Test
  public void testRegisterSchemaCache() throws Exception {
    RestService restService = createMock(RestService.class);
    CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(restService, 20);

    String schema = "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
    Schema avroSchema = new Schema.Parser().parse(schema);

    String subject = "foo";
    int id = 25;

    // Expect one call to register schema
    expect(restService.registerSchema(anyString(), eq(subject)))
            .andReturn(id);

    replay(restService);

    assertEquals(id, client.register(subject, avroSchema));
    assertEquals(id, client.register(subject, avroSchema)); // hit the cache

    verify(restService);
  }

  @Test
  public void testRegisterOverCapacity() throws Exception {
    RestService restService = createMock(RestService.class);
    CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(restService, 1); // capacity is just one

    String schema1 = "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
    Schema avroSchema1 = new Schema.Parser().parse(schema1);

    String schema2 = "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [" +
            "{ \"name\": \"name\", \"type\": \"string\" }," +
            "{ \"name\": \"blah\", \"type\": \"string\" }" +
            "]}";
    Schema avroSchema2 = new Schema.Parser().parse(schema2);


    String subject = "foo";
    int id = 25;

    // Expect one call to register schema (the second one will fail)
    expect(restService.registerSchema(anyString(), eq(subject)))
            .andReturn(id);

    replay(restService);

    assertEquals(id, client.register(subject, avroSchema1));
    try {
      // This call should exceed the identityMapCapacity
      assertEquals(id, client.register(subject, avroSchema2));
      fail();
    } catch (IllegalStateException e) {
    }

    verify(restService);
  }

  @Test
  public void testIdCache() throws Exception {
    RestService restService = createMock(RestService.class);
    CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(restService, 20);

    String schema = "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
    Schema avroSchema = new Schema.Parser().parse(schema);

    String subject = "foo";
    int id = 25;

    expect(restService.registerSchema(anyString(), eq(subject)))
            .andReturn(id);

    // Expect only one call to getId (the rest should hit the cache)
    expect(restService.getId(id))
            .andReturn(new SchemaString(schema));

    replay(restService);

    assertEquals(id, client.register(subject, avroSchema));
    assertEquals(avroSchema, client.getBySubjectAndId(subject, id));
    assertEquals(avroSchema, client.getBySubjectAndId(subject, id)); // hit the cache

    verify(restService);
  }

  @Test
  public void testVersionCache() throws Exception {
    RestService restService = createMock(RestService.class);
    CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(restService, 20);

    String schema = "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
    Schema avroSchema = new Schema.Parser().parse(schema);

    String subject = "foo";
    int id = 25;
    int version = 7;

    expect(restService.registerSchema(anyString(), eq(subject)))
            .andReturn(id);

    // Expect only one call to lookup the subject (the rest should hit the cache)
    expect(restService.lookUpSubjectVersion(anyString(), eq(subject)))
            .andReturn(new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, version, id, schema));

    replay(restService);

    assertEquals(id, client.register(subject, avroSchema));
    assertEquals(version, client.getVersion(subject, avroSchema));
    assertEquals(version, client.getVersion(subject, avroSchema)); // hit the cache

    verify(restService);
  }

  @Test
  public void testIdenticalSchemas() throws Exception {
    RestService restService = createMock(RestService.class);
    CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(restService, 20);

    String schema = "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
    Schema avroSchema = new Schema.Parser().parse(schema);
    SchemaString schemaStringOne = new SchemaString(schema);
    SchemaString schemaStringTwo = new SchemaString(schema);

    String subjectOne = "subjectOne";
    String subjectTwo = "subjectTwo";
    int id = 25;

    expect(restService.registerSchema(anyString(), eq(subjectOne)))
            .andReturn(id);
    expect(restService.registerSchema(anyString(), eq(subjectTwo)))
            .andReturn(id);

    expect(restService.getId(eq(id)))
            .andReturn(schemaStringOne);
    expect(restService.getId(eq(id)))
            .andReturn(schemaStringTwo);

    replay(restService);

    // Make sure they still get the same ID
    assertEquals(id, client.register(subjectOne, avroSchema));
    assertEquals(id, client.register(subjectTwo, avroSchema));
    // Neither of these two schemas should be cached yet
    assertEquals(avroSchema, client.getBySubjectAndId(subjectOne, id));
    assertEquals(avroSchema, client.getBySubjectAndId(subjectTwo, id));
    // These two should be cached now
    assertEquals(avroSchema, client.getBySubjectAndId(subjectOne, id));
    assertEquals(avroSchema, client.getBySubjectAndId(subjectTwo, id));

    verify(restService);
  }

}
