/*
 * Copyright 2018 Confluent Inc.
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
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AvroSchemaUtils {

  private static final Map<String, Schema> primitiveSchemas;

  static {
    Schema.Parser parser = new Schema.Parser();
    primitiveSchemas = new HashMap<>();
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

  protected static Schema copyOf(Schema schema) {
    return new Schema.Parser().parse(schema.toString());
  }

  protected static Map<String, Schema> getPrimitiveSchemas() {
    return Collections.unmodifiableMap(primitiveSchemas);
  }

  public static Schema getSchema(Object object) {
    return getSchema(object, false);
  }

  public static Schema getSchema(Object object, boolean useReflection) {
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
    } else if (object instanceof CharSequence) {
      return primitiveSchemas.get("String");
    } else if (object instanceof byte[]) {
      return primitiveSchemas.get("Bytes");
    } else if (useReflection) {
      Schema schema = ReflectData.get().getSchema(object.getClass());
      if (schema == null) {
        throw new SerializationException("Schema is null for object of class "
            + object.getClass().getCanonicalName());
      } else {
        return schema;
      }
    } else if (object instanceof GenericContainer) {
      return ((GenericContainer) object).getSchema();
    } else {
      throw new IllegalArgumentException(
          "Unsupported Avro type. Supported types are null, Boolean, Integer, Long, "
              + "Float, Double, String, byte[] and IndexedRecord");
    }
  }
}
