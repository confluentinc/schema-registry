package io.confluent.kafka.schemaregistry.storage.serialization;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class ZkStringSerializer implements ZkSerializer {

    @Override public byte[] serialize(Object o) throws ZkMarshallingError {
        return ((String) o).getBytes();
    }

    @Override public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        if (bytes == null)
            return null;
        return new String(bytes);
    }
}
