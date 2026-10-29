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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import io.confluent.kafka.example.KindTest;
import io.confluent.kafka.example.KindTestEnum;
import io.confluent.kafka.example.KindTestHash;
import io.confluent.kafka.example.User;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.File;
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
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.common.errors.SerializationException;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.function.ThrowingRunnable;

// DGS-25191: a class named by a schema is neither initialized nor instantiated unless it is an
// Avro record, enum or fixed class
public class SchemaNamedClassTest {

  // Resolved as nested classes of SchemaNamedClasses
  private static final String NAMESPACE = SchemaNamedClasses.class.getName();
  private static final String NO_CLASS_NAMESPACE = "io.confluent.kafka.serializers.noclass";
  private static final String CONTEXT_ONLY_INITIALIZED = "dgs25191.contextonly.initialized";
  private static final String READER_CONTEXT_ONLY_INITIALIZED =
      "dgs25191.readercontextonly.initialized";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
  private final KafkaAvroSerializer serializer;
  private final KafkaAvroDeserializer specificDeserializer;
  private final KafkaAvroDeserializer latestSpecificDeserializer;

  public SchemaNamedClassTest() {
    Map<String, Object> props = new HashMap<>();
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    serializer = new KafkaAvroSerializer(schemaRegistry, props);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    specificDeserializer = new KafkaAvroDeserializer(schemaRegistry, props);
    props.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, true);
    latestSpecificDeserializer = new KafkaAvroDeserializer(schemaRegistry, props);
  }

  @Test
  public void testTopLevelRecord() {
    Schema schema = record(NAMESPACE, "TopLevelRecord", stringField());
    assertNotLoaded("top-level", value(schema), "TopLevelRecord");
  }

  @Test
  public void testUnionBranch() {
    // No null branch: a primitive branch fails the specific reader on its own
    Schema branch = record(NAMESPACE, "UnionBranch", stringField());
    Schema union = Schema.createUnion(record(NO_CLASS_NAMESPACE, "Other", stringField()), branch);
    assertNotLoaded("union", new NonRecordContainer(union, value(branch)), "UnionBranch");
  }

  @Test
  public void testArrayElement() {
    Schema element = record(NAMESPACE, "ArrayElement", stringField());
    assertNotLoaded("array", array(element, value(element)), "ArrayElement");
  }

  @Test
  public void testMapValue() {
    Schema valueSchema = record(NAMESPACE, "MapValue", stringField());
    Object map = new NonRecordContainer(Schema.createMap(valueSchema),
        Collections.singletonMap("k", value(valueSchema)));
    assertNotLoaded("map", map, "MapValue");
  }

  // A record without a class is read from the writer schema, so its fields' types are looked up
  @Test
  public void testNestedRecord() {
    Schema nested = record(NAMESPACE, "NestedRecord", stringField());
    Schema outer = record(NO_CLASS_NAMESPACE, "Outer", new Schema.Field("nested", nested));
    GenericRecord value = new GenericData.Record(outer);
    value.put("nested", value(nested));
    assertNotLoaded("nested-record", array(outer, value), "NestedRecord");
  }

  @Test
  public void testNestedEnum() {
    Schema nested = Schema.createEnum("NestedEnum", null, NAMESPACE, Arrays.asList("A", "B"));
    Schema outer = record(NO_CLASS_NAMESPACE, "Outer", new Schema.Field("nested", nested));
    GenericRecord value = new GenericData.Record(outer);
    value.put("nested", new GenericData.EnumSymbol(nested, "A"));
    assertNotLoaded("nested-enum", array(outer, value), "NestedEnum");
  }

  @Test
  public void testNestedFixed() {
    Schema nested = Schema.createFixed("NestedFixed", null, NAMESPACE, 1);
    Schema outer = record(NO_CLASS_NAMESPACE, "Outer", new Schema.Field("nested", nested));
    GenericRecord value = new GenericData.Record(outer);
    value.put("nested", new GenericData.Fixed(nested, new byte[] {1}));
    assertNotLoaded("nested-fixed", array(outer, value), "NestedFixed");
  }

  // The reader schema comes from the registry, without a lookup of the writer's class
  @Test
  public void testLatestVersionReaderSchema() throws Exception {
    String topic = "latest";
    Schema writer = record(NO_CLASS_NAMESPACE, "Record", stringField());
    byte[] bytes = serializer.serialize(topic, value(writer));
    Schema latest = record(NAMESPACE, "LatestVersion", stringField());
    latest.addAlias(writer.getFullName());
    schemaRegistry.register(topic + "-value", new AvroSchema(latest));

    assertRejected(() -> latestSpecificDeserializer.deserialize(topic, bytes), "LatestVersion");
  }

  @Test
  public void testGeneratedRecord() {
    User user = new User("name");
    byte[] bytes = serializer.serialize("generated", user);
    assertEquals(user, specificDeserializer.deserialize("generated", bytes));
  }

  @Test
  public void testGeneratedArrayElement() {
    List<User> users = array(User.getClassSchema(), new User("name"));
    byte[] bytes = serializer.serialize("generated-array", users);
    assertEquals(Collections.singletonList(new User("name")),
        new ArrayList<>((List<?>) specificDeserializer.deserialize("generated-array", bytes)));
  }

  // Generated enum and fixed classes pass the check, and are still read into
  @Test
  public void testGeneratedEnumAndFixed() {
    KindTest value = new KindTest(KindTestEnum.B, new KindTestHash(new byte[] {1, 2}));
    byte[] bytes = serializer.serialize("generated-kinds", value);
    assertEquals(value, specificDeserializer.deserialize("generated-kinds", bytes));
    AvroSchemaUtils.checkSpecificClasses(KindTest.getClassSchema());
  }

  // The reader schema is the registry's, not the class's own
  @Test
  public void testGeneratedEnumAndFixedInLatestVersion() {
    KindTest value = new KindTest(KindTestEnum.A, new KindTestHash(new byte[] {3, 4}));
    byte[] bytes = serializer.serialize("generated-kinds-latest", value);
    assertEquals(value, latestSpecificDeserializer.deserialize("generated-kinds-latest", bytes));
  }

  // An array of enums is read from the writer's schema, into the generated enum
  @Test
  public void testGeneratedEnumArrayElement() {
    Schema array = Schema.createArray(KindTestEnum.getClassSchema());
    byte[] bytes = serializer.serialize("generated-enum-array",
        new NonRecordContainer(array, Arrays.asList(KindTestEnum.A, KindTestEnum.B)));
    assertEquals(Arrays.asList(KindTestEnum.A, KindTestEnum.B), new ArrayList<>(
        (List<?>) specificDeserializer.deserialize("generated-enum-array", bytes)));
  }

  @Test
  public void testFindSpecificClass() {
    Schema topLevel = record("io.confluent.kafka.serializers", "SchemaNamedTopLevel",
        stringField());
    assertSame(SchemaNamedTopLevel.class, AvroSchemaUtils.findSpecificClass(topLevel));
    assertFalse(SchemaNamedClasses.initialized.contains("SchemaNamedTopLevel"));

    Schema nested = record(NAMESPACE, "TopLevelRecord", stringField());
    assertSame(SchemaNamedClasses.TopLevelRecord.class,
        AvroSchemaUtils.findSpecificClass(nested));
    assertSame(SchemaNamedClasses.TopLevelRecord.class, AvroSchemaUtils.findSpecificClass(
        Schema.createUnion(Schema.create(Schema.Type.NULL), nested)));
    assertFalse(SchemaNamedClasses.initialized.contains("TopLevelRecord"));

    assertNull(AvroSchemaUtils.findSpecificClass(
        record(NO_CLASS_NAMESPACE, "Missing", stringField())));
    assertSame(User.class, AvroSchemaUtils.findSpecificClass(User.getClassSchema()));
  }

  // Avro also tries the context class loader, so a cached lookup holds only for the one it used
  @Test
  public void testFindSpecificClassPerContextClassLoader() throws Exception {
    File dir = compileContextOnlyClass("ContextOnly", CONTEXT_ONLY_INITIALIZED);
    Schema schema = record(null, "ContextOnly", stringField());
    assertNull(AvroSchemaUtils.findSpecificClass(schema));
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {dir.toURI().toURL()}, null)) {
      thread.setContextClassLoader(loader);
      Class<?> found = AvroSchemaUtils.findSpecificClass(schema);
      assertSame(loader, found.getClassLoader());
      assertThrows(SerializationException.class,
          () -> AvroSchemaUtils.checkSpecificClasses(schema));
    } finally {
      thread.setContextClassLoader(original);
    }
    assertNull(AvroSchemaUtils.findSpecificClass(schema));
    assertNull(System.getProperty(CONTEXT_ONLY_INITIALIZED));
  }

  // A cached reader is checked again when it is used under another context class loader. An empty
  // array makes Avro build the reader without looking up its element class.
  @Test
  public void testCachedReaderCheckedPerContextClassLoader() throws Exception {
    File dir = compileContextOnlyClass("ReaderContextOnly", READER_CONTEXT_ONLY_INITIALIZED);
    String topic = "cached-reader";
    Schema writerItem = record(NO_CLASS_NAMESPACE, "Item", stringField());
    Schema writer = record(NO_CLASS_NAMESPACE, "Record", stringField(),
        new Schema.Field("items", Schema.createArray(writerItem)));
    GenericRecord empty = value(writer);
    empty.put("items", Collections.emptyList());
    byte[] emptyBytes = serializer.serialize(topic, empty);
    GenericRecord nonEmpty = value(writer);
    nonEmpty.put("items", Collections.singletonList(value(writerItem)));
    byte[] nonEmptyBytes = serializer.serialize(topic, nonEmpty);
    Schema latestItem = record(null, "ReaderContextOnly", stringField());
    latestItem.addAlias(writerItem.getFullName());
    Schema latest = record(NO_CLASS_NAMESPACE, "Record", stringField(),
        new Schema.Field("items", Schema.createArray(latestItem)));
    schemaRegistry.register(topic + "-value", new AvroSchema(latest));

    latestSpecificDeserializer.deserialize(topic, emptyBytes);
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {dir.toURI().toURL()}, null)) {
      thread.setContextClassLoader(loader);
      assertRejected(() -> latestSpecificDeserializer.deserialize(topic, nonEmptyBytes),
          "ReaderContextOnly");
    } finally {
      thread.setContextClassLoader(original);
    }
    assertNull(System.getProperty(READER_CONTEXT_ONLY_INITIALIZED));
  }

  @Test
  public void testFindSpecificClassUnnamedTypes() {
    for (Schema schema : Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT),
        Schema.createArray(Schema.create(Schema.Type.LONG)),
        Schema.createMap(Schema.create(Schema.Type.LONG)),
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)),
        Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.INT)))) {
      assertEquals(schema.toString(), SpecificData.get().getClass(schema),
          AvroSchemaUtils.findSpecificClass(schema));
    }
  }

  @Test
  public void testSpecificDataForSchema() {
    Schema schema = record(NAMESPACE, "SpecificDataChoice", stringField());
    assertSame(AvroSchemaUtils.getSpecificData(),
        AvroSchemaUtils.getSpecificDataForSchema(schema));
    assertSame(SpecificData.get(), AvroSchemaUtils.getSpecificDataForSchema(schema, false));
    assertFalse(SchemaNamedClasses.initialized.contains("SpecificDataChoice"));

    assertSame(SpecificData.getForClass(User.class),
        AvroSchemaUtils.getSpecificDataForSchema(User.getClassSchema()));
    AvroSchemaUtils.checkSpecificClasses(User.getClassSchema());
  }

  private void assertNotLoaded(String topic, Object value, String className) {
    byte[] bytes = serializer.serialize(topic, value);
    assertRejected(() -> specificDeserializer.deserialize(topic, bytes), className);
  }

  // Rejected by the class check, and the class was never initialized
  private static void assertRejected(ThrowingRunnable deserialize, String className) {
    Throwable cause = assertThrows(SerializationException.class, deserialize);
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    assertTrue(cause.getMessage(), cause.getMessage().contains(className));
    assertFalse(SchemaNamedClasses.initialized.contains(className));
  }

  // Compiles a class in the default package that only a loader of the returned directory can
  // see; its static initializer sets the given system property
  private File compileContextOnlyClass(String className, String initializedProperty)
      throws Exception {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assumeNotNull(compiler);
    File dir = temporaryFolder.newFolder();
    File source = new File(dir, className + ".java");
    Files.write(source.toPath(), ("public class " + className + " { static { System.setProperty("
        + "\"" + initializedProperty + "\", \"true\"); } }").getBytes(StandardCharsets.UTF_8));
    assertEquals(0, compiler.run(null, null, null, source.getPath()));
    return dir;
  }

  private static Schema record(String namespace, String name, Schema.Field... fields) {
    return Schema.createRecord(name, null, namespace, false, Arrays.asList(fields));
  }

  private static Schema.Field stringField() {
    return new Schema.Field("f", Schema.create(Schema.Type.STRING));
  }

  private static GenericRecord value(Schema schema) {
    GenericRecord value = new GenericData.Record(schema);
    value.put("f", "value");
    return value;
  }

  @SafeVarargs
  private static <T> GenericData.Array<T> array(Schema element, T... values) {
    return new GenericData.Array<>(Schema.createArray(element), Arrays.asList(values));
  }
}
