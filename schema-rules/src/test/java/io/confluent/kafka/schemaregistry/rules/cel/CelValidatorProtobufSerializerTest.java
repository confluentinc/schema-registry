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
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for {@link CelValidator} fired through {@link KafkaProtobufSerializer}.
 * Covers the wiring (validation actually runs from the serializer pipeline) and the
 * {@code validation.rules.execution} config; per-rule CEL semantics are exercised at the
 * unit level by {@code CelExecutorTest}.
 *
 * <p>Schema layout: {@code ValidationPerson} message (defined in {@code ValidationWidget.proto})
 * with one message-level rule (sanity) and two field-level rules (age and name).
 */
public class CelValidatorProtobufSerializerTest {

  private static final String TOPIC = "person";

  private SchemaRegistryClient client;

  @BeforeEach
  void setUp() throws Exception {
    client = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
    ProtobufSchema schema = new ProtobufSchema(ValidationPerson.getDescriptor());
    client.register(TOPIC + "-value", schema);
  }

  private KafkaProtobufSerializer<ValidationPerson> serializer(String validationMode) {
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_SIZE, "0");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, "false");
    props.put("validation.rules.execution", validationMode);
    return new KafkaProtobufSerializer<>(client, props);
  }

  private static ValidationPerson record(int age, String name) {
    return ValidationPerson.newBuilder().setAge(age).setName(name).build();
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

  /** The serializer wraps our SerializationException(violations) as the cause. */
  private static String causeMessage(Throwable t) {
    Throwable cause = t.getCause();
    return cause != null ? cause.getMessage() : t.getMessage();
  }
}
