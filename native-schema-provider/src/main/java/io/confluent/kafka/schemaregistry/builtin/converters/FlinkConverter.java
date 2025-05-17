/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.builtin.converters;

import io.confluent.kafka.schemaregistry.builtin.NativeSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema;
import io.confluent.kafka.schemaregistry.builtin.Schema.Field;
import io.confluent.kafka.schemaregistry.builtin.SchemaRuntimeException;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for converting between our runtime data format and Avro, and (de)serializing that data.
 */
public class FlinkConverter {

  private static final Logger log = LoggerFactory.getLogger(FlinkConverter.class);

  private Map<NativeSchema, String> fromNativeSchemaCache;
  private Map<String, NativeSchema> toNativeSchemaCache;

  public FlinkConverter(int cacheSize) {
    fromNativeSchemaCache = new BoundedConcurrentHashMap<>(cacheSize);
    toNativeSchemaCache = new BoundedConcurrentHashMap<>(cacheSize);
  }


  public String fromNativeSchema(NativeSchema schema) {
    if (schema == null) {
      return null;
    }

    String cached = fromNativeSchemaCache.get(schema);
    if (cached != null) {
      return cached;
    }

    FromNativeContext fromNativeContext = new FromNativeContext();
    String resultSchema = fromNativeSchema(schema.rawSchema(), fromNativeContext);
    fromNativeSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  public String fromNativeSchema(Schema schema,
                                                 FromNativeContext fromNativeContext) {
    if (schema == null) {
      return null;
    }

    // Extra type annotation information for otherwise lossy conversions
    String cfltType = null;

    final org.apache.avro.Schema baseSchema;
    int size = -1;
    switch (schema.getType()) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case CHAR:
        size = schema.getFixedSize();
        return "CHAR(" + size + ")";
      case STRING:
        return "STRING";
      case BINARY:
        size = schema.getFixedSize();
        return "BINARY(" + size + ")";
      case BYTES:
        return "BYTES";
      case ARRAY:
        // TODO
        break;
      case MAP:
        // TODO
        break;
      case STRUCT:
        StringBuilder sb = new StringBuilder("CREATE TABLE '" + schema.getName() + "' (\n");
        //String namespace = schema.getNamespace();
        //String name = schema.getName();
        //String doc = schema.getDoc();
        for (int i = 0; i < schema.getFields().size(); i++) {
          Field field = schema.getFields().get(i);
          String fieldName = field.name();
          //String fieldDoc = field.doc();
          sb.append("    '" + fieldName + "' "
              + fromNativeSchema(field.schema(), fromNativeContext));
          if (i < schema.getFields().size() - 1) {
            sb.append(",\n");
          } else {
            sb.append("\n");
          }
        }
        sb.append(");");
        return sb.toString();
      case UNION:
        // TODO
        break;
      default:
        throw new SchemaRuntimeException("Unknown schema type: " + schema.getType());
    }
    throw new SchemaRuntimeException("Unknown schema type: " + schema.getType());
  }

  /**
   * Class that holds the context for performing {@code toNativeSchema}
   */
  private static class ToNativeContext {
    private final Set<org.apache.avro.Schema> detectedCycles;

    /**
     * cycleReferences - map that holds connect Schema references to resolve cycles
     * detectedCycles - avro schemas that have been detected to have cycles
     */
    private ToNativeContext() {
      this.detectedCycles = new HashSet<>();
    }
  }

  /**
   * Class that holds the context for performing {@code fromNativeSchema}
   */
  private static class FromNativeContext {
    //SchemaMap is used to resolve references that need to mapped as types
    //private Map<Schema, AvroSchema> schemaMap;
    //schema name to Schema reference to resolve cycles
    private int defaultSchemaNameIndex = 0;

    private FromNativeContext() {
    }

    public int incrementAndGetNameIndex() {
      return ++defaultSchemaNameIndex;
    }
  }
}
