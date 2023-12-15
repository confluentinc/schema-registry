/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DlqAction implements RuleAction {

  private static final Logger log = LoggerFactory.getLogger(DlqAction.class);

  public static final String TYPE = "DLQ";

  public static final String DLQ_TOPIC = "dlq.topic";
  public static final String DLQ_AUTO_FLUSH = "dlq.auto.flush";
  public static final String PRODUCER = "producer";  // for testing

  public static final String HEADER_PREFIX = "__rule.";
  public static final String RULE_NAME = HEADER_PREFIX + "name";
  public static final String RULE_MODE = HEADER_PREFIX + "mode";
  public static final String RULE_SUBJECT = HEADER_PREFIX + "subject";
  public static final String RULE_TOPIC = HEADER_PREFIX + "topic";
  public static final String RULE_EXCEPTION = HEADER_PREFIX + "exception";

  private static final LongSerializer LONG_SERIALIZER = new LongSerializer();
  private static final IntegerSerializer INT_SERIALIZER = new IntegerSerializer();
  private static final ShortSerializer SHORT_SERIALIZER = new ShortSerializer();
  private static final DoubleSerializer DOUBLE_SERIALIZER = new DoubleSerializer();
  private static final FloatSerializer FLOAT_SERIALIZER = new FloatSerializer();

  private Map<String, ?> configs;
  private String topic;
  private boolean autoFlush;
  private volatile KafkaProducer<byte[], byte[]> producer;

  @Override
  public boolean addOriginalConfigs() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    this.configs = configs;
    this.topic = (String) configs.get(DLQ_TOPIC);
    Object autoFlushConfig = configs.get(DLQ_AUTO_FLUSH);
    if (autoFlushConfig != null) {
      this.autoFlush = Boolean.parseBoolean(autoFlushConfig.toString());
    }
    // used by tests
    this.producer = (KafkaProducer<byte[], byte[]>) configs.get(PRODUCER);
  }

  public String type() {
    return TYPE;
  }

  public String topic() {
    return topic;
  }

  private KafkaProducer<byte[], byte[]> producer() {
    if (producer == null) {
      Map<String, Object> producerConfigs = baseProducerConfigs();
      producerConfigs.putAll(configs);
      synchronized (this) {
        if (producer == null) {
          producer = new KafkaProducer<>(producerConfigs);
        }
      }
    }
    return producer;
  }

  public void run(RuleContext ctx, Object message, RuleException ex) throws RuleException {
    String topic = topic();
    if (topic == null || topic.isEmpty()) {
      topic = ctx.getParameter(DLQ_TOPIC);
    }
    if (topic == null || topic.isEmpty()) {
      throw new SerializationException("Could not send to DLQ as no topic is configured");
    }
    final String dlqTopic = topic;
    try {
      byte[] keyBytes = convertToBytes(ctx, ctx.originalKey());
      byte[] valueBytes = convertToBytes(ctx, ctx.originalValue());
      ProducerRecord<byte[], byte[]> producerRecord =
          new ProducerRecord<>(dlqTopic, null, keyBytes, valueBytes, ctx.headers());
      populateHeaders(ctx, producerRecord, ex);
      producer().send(producerRecord, (metadata, exception) -> {
        if (exception != null) {
          log.error("Could not produce message to DLQ topic {}", dlqTopic, exception);
        } else {
          log.info("Sent message to DLQ topic {}", dlqTopic);
        }
      });
      if (autoFlush) {
        producer.flush();
      }
    } catch (Exception e) {
      log.error("Could not produce message to DLQ topic {}", dlqTopic, e);
    }

    String msg = "Rule failed: " + ctx.rule().getName();
    // throw a RuntimeException
    throw ex != null ? new SerializationException(msg, ex) : new SerializationException(msg);
  }

  private byte[] convertToBytes(RuleContext ctx, Object message) throws IOException {
    if (message == null) {
      return null;
    } else if (message instanceof byte[]) {
      return (byte[]) message;
    } else if (message instanceof ByteBuffer) {
      ByteBuffer buffer = (ByteBuffer) message;
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return bytes;
    } else if (message instanceof Bytes) {
      return ((Bytes) message).get();
    } else if (message instanceof String || message instanceof UUID) {
      return message.toString().getBytes(StandardCharsets.UTF_8);
    } else if (message instanceof Long) {
      return LONG_SERIALIZER.serialize(ctx.topic(), (Long)message);
    } else if (message instanceof Integer) {
      return INT_SERIALIZER.serialize(ctx.topic(), (Integer) message);
    } else if (message instanceof Short) {
      return SHORT_SERIALIZER.serialize(ctx.topic(), (Short) message);
    } else if (message instanceof Double) {
      return DOUBLE_SERIALIZER.serialize(ctx.topic(), (Double) message);
    } else if (message instanceof Float) {
      return FLOAT_SERIALIZER.serialize(ctx.topic(), (Float) message);
    } else {
      return convertToJsonBytes(ctx, message);
    }
  }

  private byte[] convertToJsonBytes(RuleContext ctx, Object message) throws IOException {
    JsonNode json = ctx.target().toJson(message);
    return JacksonMapper.INSTANCE.writeValueAsBytes(json);
  }

  private void populateHeaders(
      RuleContext ctx, ProducerRecord<byte[], byte[]> producerRecord, RuleException ex) {
    Headers headers = producerRecord.headers();
    headers.add(RULE_NAME, toBytes(ctx.rule().getName()));
    headers.add(RULE_MODE, toBytes(ctx.ruleMode().name()));
    headers.add(RULE_SUBJECT, toBytes(ctx.subject()));
    headers.add(RULE_TOPIC, toBytes(ctx.topic()));
    if (ex != null) {
      headers.add(RULE_EXCEPTION, toBytes(ex.getMessage()));
    }
  }

  private byte[] toBytes(String value) {
    if (value != null) {
      return value.getBytes(StandardCharsets.UTF_8);
    } else {
      return null;
    }
  }

  static Map<String, Object> baseProducerConfigs() {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    // These settings will execute infinite retries on retriable exceptions.
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
        Integer.toString(Integer.MAX_VALUE));
    return producerProps;
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }
  }
}
