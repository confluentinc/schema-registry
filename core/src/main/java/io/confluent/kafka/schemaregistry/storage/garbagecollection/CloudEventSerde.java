/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.storage.garbagecollection;

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CloudEventSerde implements Serde<CloudEvent> {

  private final Serializer<CloudEvent> serializer;
  private final Deserializer<CloudEvent> deserializer;

  public CloudEventSerde() {
    this.serializer = new CloudEventSerializer();
    this.deserializer = new CloudEventDeserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serializer.configure(configs, isKey);
    deserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    serializer.close();
    deserializer.close();
  }

  @Override
  public Serializer<CloudEvent> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<CloudEvent> deserializer() {
    return deserializer;
  }
}
