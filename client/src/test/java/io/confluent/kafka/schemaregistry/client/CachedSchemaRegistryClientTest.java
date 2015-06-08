package io.confluent.kafka.schemaregistry.client;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import org.apache.avro.Schema;
import org.junit.Test;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

public class CachedSchemaRegistryClientTest {

  @Test
  public void testIdCache() throws Exception {
    RestService restService = createMock(RestService.class);
    CachedSchemaRegistryClient client = new CachedSchemaRegistryClient("localhost:8080", 20, restService);

    String schema = "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
    Schema avroSchema = new Schema.Parser().parse(schema);

    String subject = "foo";
    int id = 25;

    expect(restService.registerSchema(anyObject(UrlList.class), anyObject(RegisterSchemaRequest.class), anyString()))
            .andReturn(id);
    expect(restService.getId(anyObject(UrlList.class), eq(id)))
            .andReturn(new SchemaString(schema));

    replay(restService);

    assertEquals(id, client.register(subject, avroSchema));
    assertEquals(avroSchema, client.getByID(id));
    assertEquals(avroSchema, client.getByID(id)); // hit the cache

    verify(restService);
  }

  @Test
  public void testVersionCache() throws Exception {
    RestService restService = createMock(RestService.class);
    CachedSchemaRegistryClient client = new CachedSchemaRegistryClient("localhost:8080", 20, restService);

    String schema = "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
    Schema avroSchema = new Schema.Parser().parse(schema);

    String subject = "foo";
    int id = 25;
    int version = 7;

    expect(restService.registerSchema(anyObject(UrlList.class), anyObject(RegisterSchemaRequest.class), anyString()))
            .andReturn(id);
    expect(restService.lookUpSubjectVersion(anyObject(UrlList.class), anyObject(RegisterSchemaRequest.class), eq(subject)))
            .andReturn(new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, version, id, schema));

    replay(restService);

    assertEquals(id, client.register(subject, avroSchema));
    assertEquals(version, client.getVersion(subject, avroSchema));
    assertEquals(version, client.getVersion(subject, avroSchema)); // hit the cache

    verify(restService);
  }



}
