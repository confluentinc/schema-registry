package io.confluent.kafka.schemaregistry.storage.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.rest.entities.Schema;

import java.io.IOException;
import java.util.Map;

public class SchemaSerializer implements Serializer<Schema> {

    public SchemaSerializer() {

    }

    @Override public byte[] toBytes(Schema data) {
        try {
            return new ObjectMapper().writeValueAsBytes(data);
        } catch (IOException e) {
            // TODO: throw a SerializationException
            e.printStackTrace();
            return null;
        }
    }

    @Override public Schema fromBytes(byte[] data) {
        Schema schema = null;
        try {
            schema = new ObjectMapper().readValue(data, Schema.class);
        } catch (IOException e) {
            // TODO: throw a SerializationException
            e.printStackTrace();
        }
        return schema;
    }

    @Override public void close() {

    }

    @Override public void configure(Map<String, ?> stringMap) {

    }
}
