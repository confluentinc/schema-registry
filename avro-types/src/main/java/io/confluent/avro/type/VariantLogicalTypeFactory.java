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
 * <p>Avro's parser silently ignores a {@code logicalType} property it does not recognize, leaving
 * {@link Schema#getLogicalType()} null -- and since a conversion is looked up by its
 * {@code LogicalType}, an unregistered variant also gets no conversion applied. Registration is
 * therefore a precondition for reading or writing variants, not just a convenience.
 *
 * <p>This exists as a named public class with a public no-arg constructor so it can be used
 * wherever Avro accepts a factory by class name rather than instance -- notably
 * avro-maven-plugin's {@code customLogicalTypeFactories}, which runs codegen in its own JVM where
 * no Confluent registration code has executed.
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
