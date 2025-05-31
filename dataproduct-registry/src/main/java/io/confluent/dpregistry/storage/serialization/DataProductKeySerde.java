/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dpregistry.storage.serialization;

import io.confluent.dpregistry.storage.DataProductKey;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class DataProductKeySerde implements Serde<DataProductKey> {

  private final Serde<DataProductKey> inner;

  public DataProductKeySerde() {
    inner = Serdes
        .serdeFrom(new DataProductKeySerializer(), new DataProductKeyDeserializer());
  }

  @Override
  public Serializer<DataProductKey> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<DataProductKey> deserializer() {
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