/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for {@link CelValidator} fired through {@link KafkaAvroSerializer}.
 * Covers the wiring (validation actually runs from the serializer pipeline) and the
 * {@code validation.rules.execution} config; per-rule CEL semantics are exercised at the
 * unit level by {@code CelExecutorTest}.
 *
 * <p>Schema layout: {@code Person} record with one message-level rule (sanity) and two
 * field-level rules (age and name).
 */
public class CelValidatorAvroSerializerTest {

  private static final String TOPIC = "person";
  private static final String SCHEMA_STR =
      "{"
      + "\"type\":\"record\","
      + "\"name\":\"Person\","
      + "\"namespace\":\"io.confluent.kafka.schemaregistry.rules.cel\","
      + "\"confluent:rules\":["
      + "  {\"name\":\"ageNotInsane\",\"expr\":\"this.age <= 150\"}"
      + "],"
      + "\"fields\":["
      + "  {\"name\":\"age\",\"type\":\"int\","
      + "   \"confluent:rules\":[{\"name\":\"agePositive\",\"expr\":\"this >= 0\"}]},"
      + "  {\"name\":\"name\",\"type\":\"string\","
      + "   \"confluent:rules\":[{\"name\":\"nameNotEmpty\",\"expr\":\"size(this) > 0\"}]}"
      + "]"
      + "}";

  private SchemaRegistryClient client;
  private Schema avroSchema;

  @BeforeEach
  void setUp() throws Exception {
    client = new MockSchemaRegistryClient(ImmutableList.of(new AvroSchemaProvider()));
    AvroSchema schema = new AvroSchema(SCHEMA_STR);
    client.register(TOPIC + "-value", schema);
    avroSchema = schema.rawSchema();
  }

  private KafkaAvroSerializer serializer(String validationMode) {
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_SIZE, "0");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, "false");
    props.put("validation.rules.execution", validationMode);
    return new KafkaAvroSerializer(client, props);
  }

  private GenericRecord record(int age, String name) {
    GenericRecord r = new GenericData.Record(avroSchema);
    r.put("age", age);
    r.put("name", name);
    return r;
  }

  @Test
  void serializationPasses_whenAllRulesPass() {
    byte[] payload = serializer("AFTER_DOMAIN_RULES").serialize(TOPIC, record(30, "Alice"));
    assertNotNull(payload);
  }

  @Test
  void serializationFails_whenFieldRuleFails() {
    SerializationException ex = assertThrows(SerializationException.class,
        () -> serializer("AFTER_DOMAIN_RULES").serialize(TOPIC, record(-5, "Alice")));
    String msg = causeMessage(ex);
    assertTrue(msg.contains("agePositive"),
        "Expected message to mention failed rule, got: " + msg);
  }

  @Test
  void serializationPasses_whenValidationDisabled() {
    // age=-5 would fail agePositive, but validation is disabled → succeeds.
    byte[] payload = serializer("DISABLED").serialize(TOPIC, record(-5, "Alice"));
    assertNotNull(payload);
  }

  @Test
  void serializationFails_whenMultipleRulesFail() {
    // age=-5 fails agePositive; name="" fails nameNotEmpty. Both should be reported.
    SerializationException ex = assertThrows(SerializationException.class,
        () -> serializer("AFTER_DOMAIN_RULES").serialize(TOPIC, record(-5, "")));
    String msg = causeMessage(ex);
    assertTrue(msg.contains("agePositive"),
        "Expected agePositive in message, got: " + msg);
    assertTrue(msg.contains("nameNotEmpty"),
        "Expected nameNotEmpty in message, got: " + msg);
    assertTrue(msg.contains("2 violations"),
        "Expected violation count in message, got: " + msg);
  }

  /**
   * Inline rules travel with a referenced schema. References are resolved at parse time, so the
   * referenced record's {@code confluent:rules} become part of the parsed referencing schema and
   * the validation walk reaches them exactly as it would a locally-declared nested record. This is
   * the property that distinguishes inline rules from an external rule set, which applies only to
   * the root schema.
   */
  @Test
  void serializationFails_whenInlineRuleOnReferencedSchemaFails() throws Exception {
    String ns = "io.confluent.kafka.schemaregistry.rules.cel";
    String productStr =
        "{"
        + "\"type\":\"record\",\"name\":\"Product\",\"namespace\":\"" + ns + "\","
        + "\"fields\":["
        + "  {\"name\":\"sku\",\"type\":\"string\","
        + "   \"confluent:rules\":[{\"name\":\"skuNotEmpty\",\"expr\":\"size(this) > 0\"}]}"
        + "]"
        + "}";
    String orderStr =
        "{"
        + "\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"" + ns + "\","
        + "\"fields\":["
        + "  {\"name\":\"id\",\"type\":\"string\"},"
        + "  {\"name\":\"product\",\"type\":\"" + ns + ".Product\"}"
        + "]"
        + "}";

    client.register("product-value", new AvroSchema(productStr));
    AvroSchema order = new AvroSchema(
        orderStr,
        ImmutableList.of(new SchemaReference(ns + ".Product", "product-value", 1)),
        ImmutableMap.of(ns + ".Product", productStr),
        null);
    client.register("order-value", order);

    Schema orderSchema = order.rawSchema();
    GenericRecord product = new GenericData.Record(orderSchema.getField("product").schema());
    product.put("sku", "");
    GenericRecord rec = new GenericData.Record(orderSchema);
    rec.put("id", "ord-1");
    rec.put("product", product);

    SerializationException ex = assertThrows(SerializationException.class,
        () -> serializer("AFTER_DOMAIN_RULES").serialize("order", rec));
    String msg = causeMessage(ex);
    assertTrue(msg.contains("skuNotEmpty"),
        "Expected the referenced schema's inline rule to fire, got: " + msg);
  }

  /** The serializer wraps our SerializationException(violations) as the cause. */
  private static String causeMessage(Throwable t) {
    Throwable cause = t.getCause();
    return cause != null ? cause.getMessage() : t.getMessage();
  }
}
