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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import io.confluent.kafka.example.ExtendedUser;
import io.confluent.kafka.example.Grant;
import io.confluent.kafka.example.User;
import io.confluent.kafka.example.reserved.record$;
import io.confluent.kafka.example.stringable.StringableLink;
import io.confluent.kafka.example.stringable.StringableUser;
import io.confluent.kafka.example.uniontest.UnionTestUser;
import io.confluent.kafka.schemaregistry.avro.AvroTrustedClasses;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

// Runs in the avro-untrusted-classes surefire execution, where no package is trusted. Avro caches
// the classes it loads, so each case uses its own class, and messages a consumer must reject are
// produced as generic records, which never load a class.
public class AvroTrustedSerdeTest {

  public static class Widget {
    public String name = "w";
  }

  public static class Gadget {
    public String name = "g";
  }

  public static class Doohickey {
    public String name = "d";
  }

  public static class Whatsit {
    public String name = "w";
  }

  public static class Sprocket {
    public String name = "s";
  }

  // Hand-written specific records whose own schema names a java-class type
  public abstract static class Labeled extends SpecificRecordBase {
    public Object label;

    static Schema schema(String name) {
      return SchemaBuilder.record(name).namespace(AvroTrustedSerdeTest.class.getName()).fields()
          .name("label").type(SchemaBuilder.builder().stringBuilder()
              .prop(SpecificData.CLASS_PROP, StringBuilder.class.getName()).endString())
          .noDefault().endRecord();
    }

    @Override
    public Object get(int field) {
      return label;
    }

    @Override
    public void put(int field, Object value) {
      label = value;
    }
  }

  public static class Bracket extends Labeled {
    public static final Schema SCHEMA$ = schema("Bracket");

    @Override
    public Schema getSchema() {
      return SCHEMA$;
    }
  }

  public static class Clamp extends Labeled {
    public static final Schema SCHEMA$ = schema("Clamp");

    @Override
    public Schema getSchema() {
      return SCHEMA$;
    }
  }

  // A hand-written specific record, which passes the class kind check but is not generated
  public static class Cog extends SpecificRecordBase {
    public static final Schema SCHEMA$ = SchemaBuilder.record("Cog")
        .namespace(AvroTrustedSerdeTest.class.getName()).fields().requiredString("name")
        .endRecord();

    public Object name;

    @Override
    public Schema getSchema() {
      return SCHEMA$;
    }

    @Override
    public Object get(int field) {
      return name;
    }

    @Override
    public void put(int field, Object value) {
      name = value;
    }
  }

  // Hand-written nested specific records, a parent and the child its schema names
  public static class Panel extends SpecificRecordBase {
    public static final Schema SCHEMA$ = SchemaBuilder.record("Panel")
        .namespace(AvroTrustedSerdeTest.class.getName()).fields().requiredInt("x").endRecord();

    public Object x;

    @Override
    public Schema getSchema() {
      return SCHEMA$;
    }

    @Override
    public Object get(int field) {
      return x;
    }

    @Override
    public void put(int field, Object value) {
      x = value;
    }
  }

  public static class Frame extends SpecificRecordBase {
    public static final Schema SCHEMA$ = SchemaBuilder.record("Frame")
        .namespace(AvroTrustedSerdeTest.class.getName()).fields()
        .name("panel").type(Panel.SCHEMA$).noDefault().endRecord();

    public Object panel;

    @Override
    public Schema getSchema() {
      return SCHEMA$;
    }

    @Override
    public Object get(int field) {
      return panel;
    }

    @Override
    public void put(int field, Object value) {
      panel = value;
    }
  }

  private SchemaRegistryClient schemaRegistry;

  @BeforeClass
  public static void requireUntrustedFork() {
    assumeTrue(Boolean.getBoolean("avro.untrusted.classes"));
  }

  @Before
  public void setUp() {
    schemaRegistry = new MockSchemaRegistryClient();
    AvroTrustedClasses.resetToDefaults();
  }

  @After
  public void tearDown() {
    AvroTrustedClasses.resetToDefaults();
  }

  private static Map<String, Object> config(String... keyValues) {
    Map<String, Object> config = new HashMap<>();
    config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    for (int i = 0; i < keyValues.length; i += 2) {
      config.put(keyValues[i], keyValues[i + 1]);
    }
    return config;
  }

  // Not closed, since closing a serde also closes the shared registry client
  private byte[] serialize(String topic, Object value, String... keyValues) {
    return new KafkaAvroSerializer(schemaRegistry, config(keyValues)).serialize(topic, value);
  }

  private Object deserialize(String topic, byte[] bytes, String... keyValues) {
    return new KafkaAvroDeserializer(schemaRegistry, config(keyValues)).deserialize(topic, bytes);
  }

  private static GenericRecord record(Schema schema, String field, String value) {
    GenericRecord record = new GenericData.Record(schema);
    record.put(field, value);
    return record;
  }

  private static void assertForbidden(Runnable read) {
    SerializationException e = assertThrows(SerializationException.class, read::run);
    Throwable cause = e;
    while (cause != null && !(cause instanceof SecurityException)) {
      cause = cause.getCause();
    }
    assertTrue("expected a SecurityException cause: " + e, cause instanceof SecurityException);
  }

  @Test
  public void specificRecordSerializesWithoutConfig() {
    Grant grant = Grant.newBuilder().setGrant("all").build();
    byte[] bytes = serialize("grants", grant);
    Object result = deserialize("grants", bytes,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true",
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_VALUE_TYPE_CONFIG, Grant.class.getName());
    assertEquals(grant, result);
  }

  @Test
  public void configuredSpecificTypeDeserializesWithoutConfig() {
    GenericRecord user = record(ExtendedUser.SCHEMA$, "name", "alice");
    user.put("age", 30);
    byte[] bytes = serialize("users", user);
    Object result = deserialize("users", bytes,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true",
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_VALUE_TYPE_CONFIG,
        ExtendedUser.class.getName());
    assertEquals("alice", ((ExtendedUser) result).getName().toString());
  }

  @Test
  public void typelessSpecificReaderNeedsNoConfig() {
    // The class is named only by the writer's schema, but generated by Avro
    byte[] bytes = serialize("union", record(UnionTestUser.SCHEMA$, "name", "bob"));
    Object result = deserialize("union", bytes,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    assertEquals("bob", ((UnionTestUser) result).getName().toString());
  }

  @Test
  public void reflectPojoSerializesWithoutConfig() {
    byte[] bytes = serialize("widgets", new Widget(),
        KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
    Object result = deserialize("widgets", bytes);
    assertEquals("w", ((GenericRecord) result).get("name").toString());
  }

  @Test
  public void reflectReaderNeedsTrustedClasses() {
    Schema schema = ReflectData.get().getSchema(Gadget.class);
    byte[] bytes = serialize("gadgets", record(schema, "name", "g"));
    assertForbidden(() -> deserialize("gadgets", bytes,
        KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true"));
    // A nested class by its binary name
    Object result = deserialize("gadgets", bytes,
        KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, "true",
        KafkaAvroDeserializerConfig.AVRO_TRUSTED_CLASSES_CONFIG, Gadget.class.getName());
    assertEquals("g", ((Gadget) result).name);
  }

  @Test
  public void projectionOntoGeneratedClassNeedsNoConfig() {
    byte[] bytes = serialize("projected", record(User.SCHEMA$, "name", "carol"));
    Object result = new KafkaAvroDeserializer(schemaRegistry,
        config(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true"))
        .deserialize("projected", bytes, User.getClassSchema());
    assertEquals("carol", ((User) result).getName().toString());
  }

  @Test
  public void readerSchemaDoesNotTrustOtherClasses() {
    // A reader schema may come from anywhere, so a class it names is trusted only if generated
    Schema schema = Cog.SCHEMA$;
    byte[] bytes = serialize("cogs", record(schema, "name", "z"));
    assertForbidden(() -> new KafkaAvroDeserializer(schemaRegistry,
        config(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true"))
        .deserialize("cogs", bytes, schema));
  }

  @Test
  public void containerOfPojosSerializesWithoutConfig() {
    // The serializer unwraps the container, so the POJOs inside are what Avro loads
    Schema mapSchema = Schema.createMap(ReflectData.get().getSchema(Doohickey.class));
    Map<String, Object> map = Collections.singletonMap("k", new Doohickey());
    byte[] bytes = serialize("doohickeys", new NonRecordContainer(mapSchema, map));
    Object value = ((Map<?, ?>) deserialize("doohickeys", bytes)).values().iterator().next();
    assertEquals("d", ((GenericRecord) value).get("name").toString());
  }

  @Test
  public void containerOfPojoListSerializesWithoutConfig() {
    Schema arraySchema = Schema.createArray(ReflectData.get().getSchema(Whatsit.class));
    byte[] bytes = serialize("whatsits",
        new NonRecordContainer(arraySchema, Collections.singletonList(new Whatsit())));
    Object value = ((List<?>) deserialize("whatsits", bytes)).get(0);
    assertEquals("w", ((GenericRecord) value).get("name").toString());
  }

  @Test
  public void typelessSpecificReaderTrustsJavaClassTypesOfGeneratedClasses() {
    // The generated class declares a java.net.URI field through java-class
    byte[] bytes = serialize("sites",
        record(StringableUser.SCHEMA$, "site", "https://example.com"));
    Object result = deserialize("sites", bytes,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    assertEquals(URI.create("https://example.com"), ((StringableUser) result).getSite());
  }

  @Test
  public void latestVersionSpecificReaderTrustsJavaClassTypesOfGeneratedClasses() {
    // The reader schema comes from the registry, so only the generated class's own schema adds
    // trust for its java.net.URL field
    byte[] bytes = serialize("links",
        record(StringableLink.SCHEMA$, "url", "https://example.com"));
    Object result = deserialize("links", bytes,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true",
        KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "true");
    assertEquals("https://example.com", ((StringableLink) result).getUrl().toString());
  }

  @Test
  public void typelessSpecificReaderTrustsOnlyTheConfiguredClass() {
    // The writer's schema selects the class, so the types its schema names need their own config
    byte[] bytes = serialize("brackets", record(Bracket.SCHEMA$, "label", "x"));
    assertForbidden(() -> deserialize("brackets", bytes,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true",
        KafkaAvroDeserializerConfig.AVRO_TRUSTED_CLASSES_CONFIG, Bracket.class.getName()));
  }

  @Test
  public void typelessSpecificReaderReadsOnceTheJavaClassTypeIsConfigured() {
    byte[] bytes = serialize("clamps", record(Clamp.SCHEMA$, "label", "x"));
    Object result = deserialize("clamps", bytes,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true",
        KafkaAvroDeserializerConfig.AVRO_TRUSTED_CLASSES_CONFIG,
        Clamp.class.getName() + "," + StringBuilder.class.getName());
    assertEquals("x", ((StringBuilder) ((Clamp) result).label).toString());
  }

  @Test
  public void configuredNestedSpecificTypeTrustsItsNestedChild() {
    GenericRecord panel = new GenericData.Record(Panel.SCHEMA$);
    panel.put("x", 5);
    GenericRecord frame = new GenericData.Record(Frame.SCHEMA$);
    frame.put("panel", panel);
    byte[] bytes = serialize("frames", frame);
    Object result = deserialize("frames", bytes,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true",
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_VALUE_TYPE_CONFIG, Frame.class.getName());
    assertEquals(5, ((Panel) ((Frame) result).panel).x);
  }

  @Test
  public void containerOfPojosAfterANullSerializesWithoutConfig() {
    Schema element = Schema.createUnion(Schema.create(Schema.Type.NULL),
        ReflectData.get().getSchema(Sprocket.class));
    byte[] bytes = serialize("sprockets", new NonRecordContainer(Schema.createArray(element),
        Arrays.asList(null, new Sprocket())));
    Object value = ((List<?>) deserialize("sprockets", bytes)).get(1);
    assertEquals("s", ((GenericRecord) value).get("name").toString());
  }

  @Test
  public void latestVersionSpecificReaderResolvesMangledClassNames() {
    // The record is named "record", so its generated class is record$
    byte[] bytes = serialize("reserved", record(record$.SCHEMA$, "path", "/tmp/x"));
    Object result = deserialize("reserved", bytes,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true",
        KafkaAvroDeserializerConfig.USE_LATEST_VERSION, "true");
    assertEquals("/tmp/x", ((record$) result).getPath().getPath());
  }
}
