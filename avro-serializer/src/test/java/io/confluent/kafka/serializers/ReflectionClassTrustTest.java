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

package io.confluent.kafka.serializers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.example.User;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.reflecttrust.LatestNested;
import io.confluent.kafka.serializers.reflecttrust.LatestRoot;
import io.confluent.kafka.serializers.reflecttrust.ReflectTrustFixtures;
import io.confluent.kafka.serializers.reflecttrust.RejectedElement;
import io.confluent.kafka.serializers.reflecttrust.RejectedRoot;
import io.confluent.kafka.serializers.reflecttrust.sub.TrustedPojo;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

// DGS-25191: the classes a reflection deserializer loads because a schema names them
public class ReflectionClassTrustTest {

  private static final String FIXTURES = "io.confluent.kafka.serializers.reflecttrust";
  private static final String NO_CLASS_NAMESPACE = "io.confluent.kafka.serializers.noclass";
  private static final String OTHER_PACKAGE = "com.example.other";

  private final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
  private final KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry,
      Collections.singletonMap(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus"));

  @Test
  public void testUnconfiguredTrustOnlyWarns() {
    byte[] bytes = serializer.serialize("unconfigured", pojoValue(namedAs(TrustedPojo.class)));
    assertPojo(deserializer(null, null).deserialize("unconfigured", bytes));
  }

  @Test
  public void testUntrustedClass() {
    byte[] bytes = serializer.serialize("untrusted", pojoValue(namedAs(RejectedRoot.class)));
    KafkaAvroDeserializer deserializer = deserializer(null, OTHER_PACKAGE);
    assertRejected(() -> deserializer.deserialize("untrusted", bytes), RejectedRoot.class);
  }

  @Test
  public void testUntrustedArrayElement() {
    Schema element = namedAs(RejectedElement.class);
    Object array = new GenericData.Array<>(Schema.createArray(element),
        Collections.singletonList(pojoValue(element)));
    byte[] bytes = serializer.serialize("untrusted-array", array);
    KafkaAvroDeserializer deserializer = deserializer(null, OTHER_PACKAGE);
    assertRejected(() -> deserializer.deserialize("untrusted-array", bytes),
        RejectedElement.class);
  }

  @Test
  public void testTrustedClass() {
    byte[] bytes = serializer.serialize("trusted-class", pojoValue(namedAs(TrustedPojo.class)));
    assertPojo(deserializer(TrustedPojo.class.getName(), null)
        .deserialize("trusted-class", bytes));
  }

  @Test
  public void testTrustedParentPackage() {
    byte[] bytes = serializer.serialize("trusted-package", pojoValue(namedAs(TrustedPojo.class)));
    assertPojo(deserializer(null, FIXTURES).deserialize("trusted-package", bytes));
  }

  // The registry's reader schema names the nested class, which the writer's class does not
  @Test
  public void testUntrustedClassInLatestVersion() throws Exception {
    String topic = "latest-untrusted";
    Schema writerNested = record(NO_CLASS_NAMESPACE, "Nested", stringField());
    Schema writer = record(NO_CLASS_NAMESPACE, "Record", stringField(),
        new Schema.Field("nested", writerNested));
    GenericRecord value = pojoValue(writer);
    value.put("nested", pojoValue(writerNested));
    byte[] bytes = serializer.serialize(topic, value);

    Schema latestNested = namedAs(LatestNested.class);
    latestNested.addAlias(writerNested.getFullName());
    Schema latest = record(FIXTURES, "LatestRoot", stringField(),
        new Schema.Field("nested", latestNested));
    latest.addAlias(writer.getFullName());
    schemaRegistry.register(topic + "-value", new AvroSchema(latest));

    KafkaAvroDeserializer deserializer = latestDeserializer(LatestRoot.class.getName());
    assertRejected(() -> deserializer.deserialize(topic, bytes), LatestNested.class);
    assertFalse(ReflectTrustFixtures.initialized.contains("LatestRoot"));
  }

  @Test
  public void testTrustedClassInLatestVersion() throws Exception {
    String topic = "latest-trusted";
    Schema writer = record(NO_CLASS_NAMESPACE, "Pojo", stringField());
    byte[] bytes = serializer.serialize(topic, pojoValue(writer));
    Schema latest = namedAs(TrustedPojo.class);
    latest.addAlias(writer.getFullName());
    schemaRegistry.register(topic + "-value", new AvroSchema(latest));

    assertPojo(latestDeserializer(TrustedPojo.class.getName()).deserialize(topic, bytes));
  }

  // A reader schema that the application passes in is not checked
  @Test
  public void testSuppliedReaderSchema() {
    byte[] bytes = serializer.serialize("supplied", pojoValue(namedAs(TrustedPojo.class)));
    KafkaAvroDeserializer deserializer = deserializer(null, OTHER_PACKAGE);
    assertPojo(deserializer.deserialize("supplied", (Headers) null, bytes,
        ReflectData.get().getSchema(TrustedPojo.class)));
  }

  @Test
  public void testIsTrusted() {
    assertTrue(trust(TrustedPojo.class.getName(), null).isTrusted(TrustedPojo.class));
    assertTrue(trust(null, FIXTURES + ".sub").isTrusted(TrustedPojo.class));
    assertTrue(trust(null, " " + FIXTURES + ". ").isTrusted(TrustedPojo.class));
    assertTrue(trust(null, "*").isTrusted(TrustedPojo.class));
    // A package trusts its subpackages, not packages that share its prefix
    assertFalse(trust(null, "io.confluent.kafka.serializers.reflect")
        .isTrusted(TrustedPojo.class));
    assertFalse(trust(null, FIXTURES + ".sub.more").isTrusted(TrustedPojo.class));
    assertFalse(trust(TrustedPojo.class.getSimpleName(), null).isTrusted(TrustedPojo.class));
    // Avro-generated classes are always trusted
    assertTrue(trust(null, OTHER_PACKAGE).isTrusted(User.class));
  }

  private KafkaAvroDeserializer deserializer(String classes, String packages) {
    return new KafkaAvroDeserializer(schemaRegistry, props(classes, packages));
  }

  private KafkaAvroDeserializer latestDeserializer(String classes) {
    Map<String, Object> props = props(classes, null);
    props.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, true);
    return new KafkaAvroDeserializer(schemaRegistry, props);
  }

  private static Map<String, Object> props(String classes, String packages) {
    Map<String, Object> props = new HashMap<>();
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, true);
    if (classes != null) {
      props.put(KafkaAvroDeserializerConfig.AVRO_TRUSTED_CLASSES_CONFIG, classes);
    }
    if (packages != null) {
      props.put(KafkaAvroDeserializerConfig.AVRO_TRUSTED_PACKAGES_CONFIG, packages);
    }
    return props;
  }

  private static ReflectionClassTrust trust(String classes, String packages) {
    return new ReflectionClassTrust(asList(classes), asList(packages));
  }

  private static List<String> asList(String value) {
    return value != null ? Collections.singletonList(value) : Collections.emptyList();
  }

  // Rejected by the trust check, and the class was never initialized
  private static void assertRejected(ThrowingRunnable deserialize, Class<?> clazz) {
    Throwable cause = assertThrows(SerializationException.class, deserialize);
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    assertTrue(cause.getMessage(), cause.getMessage().contains(clazz.getName() + " named by"));
    assertFalse(ReflectTrustFixtures.initialized.contains(clazz.getSimpleName()));
  }

  private static void assertPojo(Object value) {
    assertEquals("value", ((TrustedPojo) value).f);
  }

  // The writer's schema for a class, built by hand so that the class is not loaded
  private static Schema namedAs(Class<?> clazz) {
    return record(clazz.getPackage().getName(), clazz.getSimpleName(), stringField());
  }

  private static Schema record(String namespace, String name, Schema.Field... fields) {
    return Schema.createRecord(name, null, namespace, false, Arrays.asList(fields));
  }

  private static Schema.Field stringField() {
    return new Schema.Field("f", Schema.create(Schema.Type.STRING));
  }

  private static GenericRecord pojoValue(Schema schema) {
    GenericRecord value = new GenericData.Record(schema);
    value.put("f", "value");
    return value;
  }
}
