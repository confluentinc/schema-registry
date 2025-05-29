package io.confluent.kafka.schemaregistry.storage.encoder;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.kcache.Cache;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

public interface MetadataEncoderServiceInterface {

    void encodeMetadata(SchemaValue schema) throws SchemaRegistryStoreException;

    void decodeMetadata(SchemaValue schema) throws SchemaRegistryStoreException;

    void init();

    void close();

    void transformMetadata(SchemaValue schema, boolean rotationNeeded, boolean isEncode,
                           BiFunction<Aead, String, String> func)
            throws SchemaRegistryStoreException;

    KeysetHandle getOrCreateEncoder(String tenant, Boolean rotationNeeded);
}
