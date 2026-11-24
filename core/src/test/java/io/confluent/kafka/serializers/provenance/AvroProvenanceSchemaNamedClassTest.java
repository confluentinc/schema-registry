/*
 * Copyright 2026 Confluent Inc.
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
 */

package io.confluent.kafka.serializers.provenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// DGS-25191: a reader a provenance projection caches is checked again under another context
// class loader, as one in the datum reader cache is
class AvroProvenanceSchemaNamedClassTest {

  private static final String TOPIC = "provenance-cached-reader";
  private static final String CLASS_NAME = "ProvenanceReaderContextOnly";
  private static final String INITIALIZED = "dgs25191.provenancecontextonly.initialized";

  @TempDir
  Path temporaryFolder;

  // An empty array makes Avro build the reader without looking up its element class
  @Test
  void cachedProjectionReaderCheckedPerContextClassLoader() throws Exception {
    File dir = compileContextOnlyClass();
    Schema item = record(null, CLASS_NAME);
    Schema schema = record("io.confluent.kafka.serializers.noclass", "Record",
        new Schema.Field("items", Schema.createArray(item)));
    ProvenanceMockSchemaRegistryClient client = new ProvenanceMockSchemaRegistryClient();
    client.register(TOPIC + "-value", new AvroSchema(schema));
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(client, config(false));
    GenericRecord empty = value(schema);
    empty.put("items", Collections.emptyList());
    byte[] emptyBytes = serializer.serialize(TOPIC, empty);
    GenericRecord nonEmpty = value(schema);
    nonEmpty.put("items", Collections.singletonList(value(item)));
    byte[] nonEmptyBytes = serializer.serialize(TOPIC, nonEmpty);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config(true));

    deserializer.deserialize(TOPIC, emptyBytes);
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {dir.toURI().toURL()}, null)) {
      thread.setContextClassLoader(loader);
      Throwable cause = assertThrows(SerializationException.class,
          () -> deserializer.deserialize(TOPIC, nonEmptyBytes));
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(cause.getMessage().contains(CLASS_NAME), cause.getMessage());
    } finally {
      thread.setContextClassLoader(original);
      assertNull(System.getProperty(INITIALIZED));
      System.clearProperty(INITIALIZED);
    }
  }

  private static Map<String, Object> config(boolean deserializer) {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", "bogus");
    config.put("auto.register.schemas", false);
    if (deserializer) {
      config.put("specific.avro.reader", true);
      config.put("use.latest.version", true);
      config.put("provenance.algorithm", "enabled");
    }
    return config;
  }

  // A class in the default package that only a loader of the returned directory can see; its
  // static initializer sets a system property
  private File compileContextOnlyClass() throws Exception {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assumeTrue(compiler != null);
    File dir = temporaryFolder.toFile();
    File source = new File(dir, CLASS_NAME + ".java");
    Files.write(source.toPath(), ("public class " + CLASS_NAME + " { static { System.setProperty("
        + "\"" + INITIALIZED + "\", \"true\"); } }").getBytes(StandardCharsets.UTF_8));
    assertEquals(0, compiler.run(null, null, null, source.getPath()));
    return dir;
  }

  private static Schema record(String namespace, String name, Schema.Field... fields) {
    Schema.Field f = new Schema.Field("f", Schema.create(Schema.Type.STRING));
    Schema.Field[] all = Arrays.copyOf(new Schema.Field[] {f}, fields.length + 1);
    System.arraycopy(fields, 0, all, 1, fields.length);
    return Schema.createRecord(name, null, namespace, false, Arrays.asList(all));
  }

  private static GenericRecord value(Schema schema) {
    GenericRecord value = new GenericData.Record(schema);
    value.put("f", "value");
    return value;
  }
}
