/**
 * Copyright 2014 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.SchemaRegistryClient;

public abstract class AbstracKafkaAvroSerializer {

  private static final byte MAGIC_BYTE = 0x0;
  private static final Schema.Parser parser = new Schema.Parser();
  private static final Map<String, Schema> primitiveSchemas;
  private final EncoderFactory encoderFactory = EncoderFactory.get();

  protected final String propertyName = "schema.registry.url";
  protected SchemaRegistryClient schemaRegistry;

  // TODO: null and bytes
  static {
    primitiveSchemas = new HashMap<String, Schema>();
    primitiveSchemas.put("Boolean", parser.parse("boolean"));
    primitiveSchemas.put("Integer", parser.parse("int"));
    primitiveSchemas.put("Long", parser.parse("long"));
    primitiveSchemas.put("Float", parser.parse("float"));
    primitiveSchemas.put("Double", parser.parse("double"));
    primitiveSchemas.put("String", parser.parse("string"));

  }

  private Schema getSchema(Object object) {
    if (object instanceof Boolean) {
      return primitiveSchemas.get("Boolean");
    } else if (object instanceof Integer) {
      return primitiveSchemas.get("Integer");
    } else if (object instanceof Long) {
      return primitiveSchemas.get("Long");
    } else if (object instanceof Float) {
      return primitiveSchemas.get("Float");
    } else if (object instanceof Double) {
      return primitiveSchemas.get("Double");
    } else if (object instanceof String) {
      return primitiveSchemas.get("String");
    } else if (object instanceof IndexedRecord) {
      return ((IndexedRecord) object).getSchema();
    } else {
      throw new IllegalArgumentException("Not supported Avro type!");
    }
  }

  public byte[] serializeImpl(String topic, Object record) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Schema schema = getSchema(record);
      int id = schemaRegistry.register(schema, topic);
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(4).putInt(id).array());
      BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);

      DatumWriter<Object> writer;

      if (record instanceof SpecificRecord) {
        writer = new SpecificDatumWriter<Object>(schema);
      } else {
        writer = new GenericDatumWriter<Object>(schema);
      }

      writer.write(record, encoder);
      return out.toByteArray();

    } catch (IOException e) {

    }
    return null;
  }
}
