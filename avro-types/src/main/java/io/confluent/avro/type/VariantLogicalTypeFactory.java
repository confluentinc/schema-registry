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
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/**
 * Registers {@link VariantLogicalType} with Avro's global logical type registry.
 *
 */
public class VariantLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {

  @Override
  public LogicalType fromSchema(Schema schema) {
    return VariantLogicalType.get();
  }

  /**
   * Must be overridden: the interface default throws, and Avro calls this to key the registry.
   */
  @Override
  public String getTypeName() {
    return VariantLogicalType.NAME;
  }
}
