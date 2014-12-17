package io.confluent.kafka.schemaregistry.storage.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;

public class SchemaSerializer implements Serializer<Schema> {

  public SchemaSerializer() {

  }

  @Override
  public byte[] toBytes(Schema data) throws SerializationException {
    try {
      return new ObjectMapper().writeValueAsBytes(data);
    } catch (IOException e) {
      throw new SerializationException("Error while serializing schema " + data.toString(),
                                       e);
    }
  }

  @Override
  public Schema fromBytes(byte[] data) throws SerializationException {
    Schema schema = null;
    try {
      schema = new ObjectMapper().readValue(data, Schema.class);
    } catch (IOException e) {
      throw new SerializationException("Error while deserializing schema", e);
    }
    return schema;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> stringMap) {

  }
}
