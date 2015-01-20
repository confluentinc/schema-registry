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
package io.confluent.kafka.schemaregistryclient.serializer;

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

import io.confluent.kafka.schemaregistryclient.SchemaRegistryClient;

public abstract class AbstracKafkaAvroSerializer {

  private static final byte MAGIC_BYTE = 0x0;
  private static final int idSize = 8;
  private static final Schema.Parser parser = new Schema.Parser();
  private static final Map<String, Schema> primitiveSchemas;
  private final EncoderFactory encoderFactory = EncoderFactory.get();

  protected final String SCHEMA_REGISTRY_URL = "schema.registry.url";
  protected SchemaRegistryClient schemaRegistry;

  // TODO: null and bytes
  static {
    primitiveSchemas = new HashMap<String, Schema>();
    primitiveSchemas.put("Null", createPrimitiveSchema("null"));
    primitiveSchemas.put("Boolean", createPrimitiveSchema("boolean"));
    primitiveSchemas.put("Integer", createPrimitiveSchema("int"));
    primitiveSchemas.put("Long", createPrimitiveSchema("long"));
    primitiveSchemas.put("Float", createPrimitiveSchema("float"));
    primitiveSchemas.put("Double", createPrimitiveSchema("double"));
    primitiveSchemas.put("String", createPrimitiveSchema("string"));
    primitiveSchemas.put("Bytes", createPrimitiveSchema("bytes"));
  }

  private static Schema createPrimitiveSchema(String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return parser.parse(schemaString);
  }

  private Schema getSchema(Object object) {
    if (object == null) {
      return primitiveSchemas.get("Null");
    } else if (object instanceof Boolean) {
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
    } else if (object instanceof byte[]) {
      return primitiveSchemas.get("Bytes");
    } else if (object instanceof IndexedRecord) {
      return ((IndexedRecord) object).getSchema();
    } else {
        throw new IllegalArgumentException("Invalid Avro type!");
    }
  }

  protected byte[] serializeImpl(String topic, Object record) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Schema schema = getSchema(record);
      long id = schemaRegistry.register(schema, topic);
      out.write(MAGIC_BYTE);
      out.write(ByteBuffer.allocate(idSize).putLong(id).array());
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
      e.printStackTrace();
    }
    return null;
  }
}
