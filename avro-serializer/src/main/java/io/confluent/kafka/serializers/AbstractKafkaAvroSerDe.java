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
package io.confluent.kafka.serializers;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */
public abstract class AbstractKafkaAvroSerDe {
  protected static final byte MAGIC_BYTE = 0x0;
  protected static final int idSize = 4;

  protected static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
  protected static final String MAX_SCHEMAS_PER_SUBJECT = "max.schemas.per.subject";
  protected static final String SPECIFIC_AVRO_READER = "specific.avro.reader";
  protected final int DEFAULT_MAX_SCHEMAS_PER_SUBJECT = 1000;
  private static final Map<String, Schema> primitiveSchemas;
  protected SchemaRegistryClient schemaRegistry;

  static {
    Schema.Parser parser = new Schema.Parser();
    primitiveSchemas = new HashMap<String, Schema>();
    primitiveSchemas.put("Null", createPrimitiveSchema(parser, "null"));
    primitiveSchemas.put("Boolean", createPrimitiveSchema(parser, "boolean"));
    primitiveSchemas.put("Integer", createPrimitiveSchema(parser, "int"));
    primitiveSchemas.put("Long", createPrimitiveSchema(parser, "long"));
    primitiveSchemas.put("Float", createPrimitiveSchema(parser, "float"));
    primitiveSchemas.put("Double", createPrimitiveSchema(parser, "double"));
    primitiveSchemas.put("String", createPrimitiveSchema(parser, "string"));
    primitiveSchemas.put("Bytes", createPrimitiveSchema(parser, "bytes"));
  }

  private static Schema createPrimitiveSchema(Schema.Parser parser, String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return parser.parse(schemaString);
  }

  protected Schema getSchema(Object object) {
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
      throw new IllegalArgumentException(
          "Unsupported Avro type. Supported types are null, Boolean, Integer, Long, " +
          "Float, Double, String, byte[] and IndexedRecord");
    }
  }

  public int register(String subject, Schema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public Schema getByID(int id) throws IOException, RestClientException {
    return schemaRegistry.getByID(id);
  }
}
