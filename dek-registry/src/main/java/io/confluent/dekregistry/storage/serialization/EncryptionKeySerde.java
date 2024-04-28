/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.storage.serialization;

import io.confluent.dekregistry.storage.EncryptionKey;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class EncryptionKeySerde implements Serde<EncryptionKey> {

  private final Serde<EncryptionKey> inner;

  public EncryptionKeySerde() {
    inner = Serdes
        .serdeFrom(new EncryptionKeySerializer(), new EncryptionKeyDeserializer());
  }

  @Override
  public Serializer<EncryptionKey> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<EncryptionKey> deserializer() {
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