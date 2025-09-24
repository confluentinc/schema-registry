/*
 * Copyright 2024 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.serializers.wrapper;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.schema.id.DualSchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.serializers.schema.id.SchemaIdDeserializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeDeserializer implements Deserializer<Object> {

  protected Logger log = LoggerFactory.getLogger(CompositeDeserializer.class);

  private boolean isKey;
  private SchemaIdDeserializer schemaIdDeserializer;
  private Deserializer<?> oldDeserializer;
  private Deserializer<?> confluentDeserializer;
  private SchemaRegistryClient schemaRegistryClient;

  /**
   * Constructor used by Kafka consumer.
   */
  public CompositeDeserializer() {
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(new CompositeDeserializerConfig(configs), isKey);
  }

  protected void configure(CompositeDeserializerConfig config, boolean isKey) {
    this.isKey = isKey;
    this.schemaIdDeserializer = new DualSchemaIdDeserializer();

    Map<String, Object> oldParams = new HashMap<>();
    oldParams.putAll(config.originals());
    oldParams.putAll(config.originalsWithPrefix(
        CompositeDeserializerConfig.COMPOSITE_OLD_DESERIALIZER + "."));
    this.oldDeserializer = config.getConfiguredInstance(
        CompositeDeserializerConfig.COMPOSITE_OLD_DESERIALIZER, Deserializer.class);
    this.oldDeserializer.configure(oldParams, isKey);

    Map<String, Object> cfltParams = new HashMap<>();
    cfltParams.putAll(config.originals());
    cfltParams.putAll(config.originalsWithPrefix(
        CompositeDeserializerConfig.COMPOSITE_CONFLUENT_DESERIALIZER + "."));
    this.confluentDeserializer = config.getConfiguredInstance(
        CompositeDeserializerConfig.COMPOSITE_CONFLUENT_DESERIALIZER, Deserializer.class);
    this.confluentDeserializer.configure(cfltParams, isKey);

    this.schemaRegistryClient = getSchemaRegistryClient();
  }

  public Deserializer<?> getOldDeserializer() {
    return oldDeserializer;
  }

  public Deserializer<?> getConfluentDeserializer() {
    return confluentDeserializer;
  }

  @Override
  public Object deserialize(String topic, byte[] bytes) {
    return deserialize(topic, null, bytes);
  }

  @Override
  public Object deserialize(String topic, Headers headers, byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    SchemaId schemaId = new SchemaId("");
    try {
      schemaIdDeserializer.deserialize(topic, isKey, headers, bytes, schemaId);
      if (isValidSchemaId(topic, schemaId)) {
        return confluentDeserializer.deserialize(topic, headers, bytes);
      }
    } catch (SerializationException e) {
      // ignore, fall through
    }
    return oldDeserializer.deserialize(topic, bytes);
  }

  private boolean isValidSchemaId(String topic, SchemaId schemaId) {
    return schemaId.getGuid() != null || hasValidId(topic, schemaId.getId());
  }

  private boolean hasValidId(String topic, Integer id) {
    if (id == null || id <= 0) {
      return false;
    }

    // We assume TopicNameStrategy
    String subject = isKey ? topic + "-key" : topic + "-value";

    try {
      // Obtain the schema.
      ParsedSchema schema = schemaRegistryClient.getSchemaBySubjectAndId(subject, id);

      // Get the id of the schema that was saved in the registry.
      int savedId = schemaRegistryClient.getId(subject, schema);

      return id == savedId;
    } catch (Exception e) {
      log.warn("Error while validating schema id", e);
      return false;
    }
  }

  private SchemaRegistryClient getSchemaRegistryClient() {
    if (!(confluentDeserializer instanceof AbstractKafkaSchemaSerDe)) {
      throw new IllegalArgumentException("Value of 'confluent.deserializer' "
          + "property must be an instance of "
          + "'io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe'");
    }
    return ((AbstractKafkaSchemaSerDe) confluentDeserializer).getSchemaRegistryClient();
  }

  @Override
  public void close() {
    oldDeserializer.close();
    confluentDeserializer.close();
  }
}
