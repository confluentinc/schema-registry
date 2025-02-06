/*
 * Copyright 2024 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package io.confluent.kafka.serializers.wrapper;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeDeserializer implements Deserializer<Object> {

  protected Logger log = LoggerFactory.getLogger(CompositeDeserializer.class);

  protected static final byte MAGIC_BYTE = 0x0;

  private boolean isKey;
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
    Map<String, Object> originals = config.originals();
    this.oldDeserializer = config.getConfiguredInstance(
        CompositeDeserializerConfig.COMPOSITE_OLD_DESERIALIZER, Deserializer.class);
    this.oldDeserializer.configure(originals, isKey);
    this.confluentDeserializer = config.getConfiguredInstance(
        CompositeDeserializerConfig.COMPOSITE_CONFLUENT_DESERIALIZER, Deserializer.class);
    this.confluentDeserializer.configure(originals, isKey);
    this.schemaRegistryClient = getSchemaRegistryClient();
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

    // We assume TopicNameStrategy
    String subject = isKey ? topic + "-key" : topic + "-value";

    int schemaId = getSchemaId(ByteBuffer.wrap(bytes));

    if (isValidSchemaId(subject, schemaId)) {
      return confluentDeserializer.deserialize(topic, headers, bytes);
    } else {
      return oldDeserializer.deserialize(topic, bytes);
    }
  }

  private int getSchemaId(ByteBuffer payload) {
    if (payload == null || payload.get() != MAGIC_BYTE) {
      return -1;
    }
    return payload.getInt();
  }

  protected boolean isValidSchemaId(String subject, int id) {
    if (id == -1) {
      return false;
    }

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
