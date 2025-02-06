/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.storage.serialization;

import io.confluent.dekregistry.storage.EncryptionKeyId;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class EncryptionKeyIdSerde implements Serde<EncryptionKeyId> {

  private final Serde<EncryptionKeyId> inner;

  public EncryptionKeyIdSerde() {
    inner = Serdes
        .serdeFrom(new EncryptionKeyIdSerializer(), new EncryptionKeyIdDeserializer());
  }

  @Override
  public Serializer<EncryptionKeyId> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<EncryptionKeyId> deserializer() {
    return inner.deserializer();
  }

  @Override
  public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
    inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
    inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }
}