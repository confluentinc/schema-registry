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

package io.confluent.avro.type;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class VariantLogicalType extends LogicalType {
  public static final String NAME = "variant";
  private static final VariantLogicalType INSTANCE = new VariantLogicalType();

  public static VariantLogicalType get() {
    return INSTANCE;
  }

  private VariantLogicalType() {
    super(NAME);
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    if (!isVariantSchema(schema)) {
      throw new IllegalArgumentException("Invalid variant record: " + schema);
    }
  }

  static boolean isVariantSchema(Schema schema) {
    if (schema.getType() != Schema.Type.RECORD || schema.getFields().size() != 2) {
      return false;
    }

    Schema.Field metadataField = schema.getField("metadata");
    Schema.Field valueField = schema.getField("value");

    return metadataField != null
        && metadataField.schema().getType() == Schema.Type.BYTES
        && valueField != null
        && valueField.schema().getType() == Schema.Type.BYTES;
  }
}
