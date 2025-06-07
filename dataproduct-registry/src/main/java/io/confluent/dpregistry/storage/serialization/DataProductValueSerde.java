/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dpregistry.storage.serialization;

import io.confluent.dpregistry.storage.DataProductValue;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class DataProductValueSerde implements Serde<DataProductValue> {

  private final Serde<DataProductValue> inner;

  public DataProductValueSerde() {
    inner = Serdes
        .serdeFrom(new DataProductValueSerializer(), new DataProductValueDeserializer());
  }

  @Override
  public Serializer<DataProductValue> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<DataProductValue> deserializer() {
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