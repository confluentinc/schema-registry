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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import io.confluent.kafka.example.User;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.reflecttrust.ElementClassTarget;
import io.confluent.kafka.serializers.reflecttrust.JavaClassArrayTarget;
import io.confluent.kafka.serializers.reflecttrust.JavaClassTarget;
import io.confluent.kafka.serializers.reflecttrust.LatestNested;
import io.confluent.kafka.serializers.reflecttrust.LatestRoot;
import io.confluent.kafka.serializers.reflecttrust.ReflectTrustFixtures;
import io.confluent.kafka.serializers.reflecttrust.RejectedElement;
import io.confluent.kafka.serializers.reflecttrust.RejectedRoot;
import io.confluent.kafka.serializers.reflecttrust.sub.TrustedPojo;
import java.io.File;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.rules.TemporaryFolder;

// DGS-25191: the classes a reflection deserializer loads because a schema names them
public class ReflectionClassTrustTest {

  private static final String FIXTURES = "io.confluent.kafka.serializers.reflecttrust";
  private static final String NO_CLASS_NAMESPACE = "io.confluent.kafka.serializers.noclass";
  private static final String OTHER_PACKAGE = "com.example.other";
  private static final String CONTEXT_ONLY_INITIALIZED = "dgs25191.reflectcontextonly.initialized";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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

  // ReflectData.getClass loads the class that a java-class property names
  @Test
  public void testUntrustedJavaClassOnString() {
    Schema schema = Schema.create(Schema.Type.STRING);
    schema.addProp(SpecificData.CLASS_PROP, JavaClassTarget.class.getName());
    byte[] bytes = serializer.serialize("java-class", new NonRecordContainer(schema, "value"));
    KafkaAvroDeserializer deserializer = deserializer(null, OTHER_PACKAGE);
    assertRejected(() -> deserializer.deserialize("java-class", bytes), JavaClassTarget.class);
  }

  @Test
  public void testUntrustedJavaClassOnArray() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    schema.addProp(SpecificData.CLASS_PROP, JavaClassArrayTarget.class.getName());
    byte[] bytes = serializer.serialize("java-class-array",
        new NonRecordContainer(schema, Collections.singletonList(1)));
    KafkaAvroDeserializer deserializer = deserializer(null, OTHER_PACKAGE);
    assertRejected(() -> deserializer.deserialize("java-class-array", bytes),
        JavaClassArrayTarget.class);
  }

  // A reflect reader loads the class that a java-element-class property names
  @Test
  public void testUntrustedJavaElementClassInLatestVersion() throws Exception {
    String topic = "latest-element-class";
    Schema writer = record(NO_CLASS_NAMESPACE, "Record", stringField(),
        new Schema.Field("items", Schema.createArray(Schema.create(Schema.Type.INT))));
    GenericRecord value = pojoValue(writer);
    value.put("items", Collections.singletonList(1));
    byte[] bytes = serializer.serialize(topic, value);
    Schema items = Schema.createArray(Schema.create(Schema.Type.INT));
    items.addProp(SpecificData.ELEMENT_PROP, ElementClassTarget.class.getName());
    schemaRegistry.register(topic + "-value", new AvroSchema(trustedLatest(writer, items)));

    Map<String, Object> props = props(TrustedPojo.class.getName(), null);
    props.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, true);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistry, props);
    assertRejected(() -> deserializer.deserialize(topic, bytes), ElementClassTarget.class);
  }

  // A registry schema is checked under each context class loader: under the default one the class
  // can't be found, and under the other it is found and rejected, without being initialized
  @Test
  public void testLatestVersionCheckedPerContextClassLoader() throws Exception {
    File dir = compileClass("ReflectContextOnly", "static { System.setProperty(\""
        + CONTEXT_ONLY_INITIALIZED + "\", \"true\"); }");
    String topic = "latest-context-loader";
    Schema writer = record(NO_CLASS_NAMESPACE, "Record", stringField(),
        new Schema.Field("items", Schema.createArray(Schema.create(Schema.Type.INT))));
    GenericRecord value = pojoValue(writer);
    value.put("items", Collections.singletonList(1));
    byte[] bytes = serializer.serialize(topic, value);
    Schema items = Schema.createArray(Schema.create(Schema.Type.INT));
    items.addProp(SpecificData.ELEMENT_PROP, "ReflectContextOnly");
    schemaRegistry.register(topic + "-value", new AvroSchema(trustedLatest(writer, items)));
    Map<String, Object> props = props(TrustedPojo.class.getName(), null);
    props.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, true);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistry, props);

    assertTrue(rejection(() -> deserializer.deserialize(topic, bytes))
        .contains("ReflectContextOnly named by the schema cannot be found"));
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {dir.toURI().toURL()}, null)) {
      thread.setContextClassLoader(loader);
      assertTrue(rejection(() -> deserializer.deserialize(topic, bytes))
          .contains("ReflectContextOnly named by the schema is not trusted"));
    } finally {
      thread.setContextClassLoader(original);
    }
    assertNull(System.getProperty(CONTEXT_ONLY_INITIALIZED));
  }

  // Avro shares the classes it loads across the JVM, so with trust configured, a class that
  // another context class loader exposed is rejected rather than passed as missing
  @Test
  public void testClassAvroLoadedUnderAnotherContextClassLoader() throws Exception {
    File dir = compileClass("SharedCacheOnly", "public String f;");
    byte[] bytes = serializer.serialize("shared-cache",
        pojoValue(record(null, "SharedCacheOnly", stringField())));
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {dir.toURI().toURL()}, null)) {
      thread.setContextClassLoader(loader);
      // Unconfigured trust only warns, so Avro loads the class and keeps it
      Object value = deserializer(null, null).deserialize("shared-cache", bytes);
      assertEquals("SharedCacheOnly", value.getClass().getName());
    } finally {
      thread.setContextClassLoader(original);
    }
    KafkaAvroDeserializer deserializer = deserializer(null, OTHER_PACKAGE);
    assertTrue(rejection(() -> deserializer.deserialize("shared-cache", bytes))
        .contains("SharedCacheOnly named by the schema cannot be found"));
  }

  // Trusting every package checks nothing, so a record without a class still reads generically
  @Test
  public void testAllPackagesTrustedWithRecordWithoutClass() throws Exception {
    String topic = "all-packages";
    Schema nested = record(NO_CLASS_NAMESPACE, "Nested", stringField());
    Schema writer = record(NO_CLASS_NAMESPACE, "Record", stringField(),
        new Schema.Field("nested", nested));
    GenericRecord value = pojoValue(writer);
    value.put("nested", pojoValue(nested));
    byte[] bytes = serializer.serialize(topic, value);
    schemaRegistry.register(topic + "-value", new AvroSchema(writer));

    Map<String, Object> props = props(null, "*");
    props.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, true);
    GenericRecord result = (GenericRecord)
        new KafkaAvroDeserializer(schemaRegistry, props).deserialize(topic, bytes);
    assertEquals("value", ((GenericRecord) result.get("nested")).get("f").toString());
  }

  // Readers cached under an earlier configuration are not reused once trust is tightened
  @Test
  public void testReconfiguredTrust() {
    byte[] bytes = serializer.serialize("reconfigured", pojoValue(namedAs(TrustedPojo.class)));
    KafkaAvroDeserializer deserializer = deserializer(null, null);
    assertPojo(deserializer.deserialize("reconfigured", bytes));
    deserializer.configure(props(null, OTHER_PACKAGE), false);
    assertRejected(() -> deserializer.deserialize("reconfigured", bytes), TrustedPojo.class);
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
    // Avro-generated classes, Avro 1.12.2's defaults and collections are always trusted
    ReflectionClassTrust other = trust(null, OTHER_PACKAGE);
    assertTrue(other.isTrusted(User.class));
    assertTrue(other.isTrusted(BigDecimal.class));
    assertTrue(other.isTrusted(ArrayList.class));
    // An array class by its component type, as Avro checks it
    assertTrue(other.isTrusted(int[].class));
    assertTrue(other.isTrusted(String[][].class));
    assertFalse(other.isTrusted(URI.class));
    assertFalse(other.isTrusted(TrustedPojo[].class));
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
    String message = rejection(deserialize);
    assertTrue(message, message.contains(clazz.getName() + " named by"));
    assertFalse(ReflectTrustFixtures.initialized.contains(clazz.getSimpleName()));
  }

  // The message of the exception that caused the rejection
  private static String rejection(ThrowingRunnable deserialize) {
    Throwable cause = assertThrows(SerializationException.class, deserialize);
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause.getMessage();
  }

  // A registry schema for TrustedPojo, which reads the writer's record, with the given items
  private static Schema trustedLatest(Schema writer, Schema items) {
    Schema latest = record(TrustedPojo.class.getPackage().getName(),
        TrustedPojo.class.getSimpleName(), stringField(), new Schema.Field("items", items));
    latest.addAlias(writer.getFullName());
    return latest;
  }

  // Compiles a public class in the default package that only a loader of the returned directory
  // can see
  private File compileClass(String className, String body) throws Exception {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assumeNotNull(compiler);
    File dir = temporaryFolder.newFolder();
    File source = new File(dir, className + ".java");
    Files.write(source.toPath(), ("public class " + className + " { " + body + " }")
        .getBytes(StandardCharsets.UTF_8));
    assertEquals(0, compiler.run(null, null, null, source.getPath()));
    return dir;
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
