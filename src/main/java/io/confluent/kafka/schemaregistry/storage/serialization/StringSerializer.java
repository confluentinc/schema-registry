package io.confluent.kafka.schemaregistry.storage.serialization;

import java.util.Map;

public class StringSerializer implements Serializer<String> {

    @Override public byte[] toBytes(String data) {
        return data.getBytes();
    }

    @Override public String fromBytes(byte[] data) {
        return new String(data);
    }

    @Override public void close() {
        // do nothing
    }

    @Override public void configure(Map<String, ?> stringMap) {
        // do nothing
    }
}
