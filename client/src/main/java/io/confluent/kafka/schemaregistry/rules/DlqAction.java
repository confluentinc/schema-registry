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
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DlqAction implements RuleAction {

  private static final Logger log = LoggerFactory.getLogger(DlqAction.class);

  public static final String TYPE = "DLQ";

  public static final String TOPIC = "topic";
  public static final String PRODUCER = "producer";  // for testing

  private String topic;
  private KafkaProducer<byte[], byte[]> producer;

  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    topic = (String) configs.get(TOPIC);
    if (topic == null || topic.isEmpty()) {
      log.warn("DLQ topic is missing");
    }
    KafkaProducer<byte[], byte[]> producer = (KafkaProducer<byte[], byte[]>) configs.get(PRODUCER);
    if (producer == null) {
      producer = new KafkaProducer<>((Map<String, Object>) configs);
    }
    this.producer = producer;
  }

  public String type() {
    return TYPE;
  }

  public void run(RuleContext ctx, Object message, RuleException ex) throws RuleException {
    if (topic == null || topic.isEmpty()) {
      return;
    }

    try {
      byte[] keyBytes = convertToBytes(ctx, ctx.originalKey());
      byte[] valueBytes = convertToBytes(ctx, ctx.originalValue());
      ProducerRecord<byte[], byte[]> producerRecord =
          new ProducerRecord<>(topic, null, keyBytes, valueBytes, ctx.headers());
      producer.send(producerRecord, (metadata, exception) -> {
        if (exception != null) {
          log.error("Could not produce message to dlq topic " + topic, exception);
        }
      });
    } catch (IOException e) {
      log.error("Could not produce message to dlq topic " + topic, e);
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
    } else {
      JsonNode json = ctx.target().toJson(message);
      return JacksonMapper.INSTANCE.writeValueAsBytes(json);
    }
  }
}
