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

package io.confluent.connect.schema;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantJsonConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

public class ConnectVariant {

  public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.Variant";

  public static final String METADATA_FIELD = "metadata";
  public static final String VALUE_FIELD = "value";

  /**
   * Returns a SchemaBuilder for a Variant.
   *
   * @return a SchemaBuilder
   */
  public static SchemaBuilder builder() {
    return SchemaBuilder.struct()
        .name(LOGICAL_NAME)
        .field(METADATA_FIELD, Schema.BYTES_SCHEMA)
        .field(VALUE_FIELD, Schema.BYTES_SCHEMA);
  }

  /**
   * Returns whether a schema represents a Variant.
   *
   * @param schema the schema
   * @return whether the schema represents a Variant
   */
  public static boolean isVariant(Schema schema) {
    return schema != null && LOGICAL_NAME.equals(schema.name());
  }

  /**
   * Convert a Variant Connect Struct to a JsonNode.
   * Used by JSON Schema converter to serialize variant values.
   *
   * @param schema the schema
   * @param value the Variant Struct with metadata and value byte arrays
   * @return the JSON representation of the variant value
   */
  public static JsonNode fromLogical(Schema schema, Struct value) {
    if (!isVariant(schema)) {
      throw new DataException(
          "Requested conversion of Variant object but the schema does not match.");
    }
    Variant variant = new Variant(
        (byte[]) value.get(VALUE_FIELD),
        (byte[]) value.get(METADATA_FIELD));
    return VariantJsonConverter.toJsonNode(variant);
  }

  /**
   * Convert a JsonNode to a Variant Connect Struct.
   * Used by JSON Schema converter to deserialize variant values.
   *
   * @param schema the schema
   * @param value the JSON value to convert
   * @return a Struct with metadata and value byte arrays
   */
  public static Struct toLogical(Schema schema, JsonNode value) {
    if (!isVariant(schema)) {
      throw new DataException(
          "Requested conversion of Variant object but the schema does not match.");
    }
    Variant variant = VariantJsonConverter.toVariant(value);
    Struct struct = new Struct(schema);
    struct.put(METADATA_FIELD, toBytes(variant.getMetadataBuffer()));
    struct.put(VALUE_FIELD, toBytes(variant.getValueBuffer()));
    return struct;
  }

  private static byte[] toBytes(java.nio.ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }
}
