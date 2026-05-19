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

package io.confluent.kafka.schemaregistry.type.logical.common;

/**
 * Selects the wire-format edition emitted by the LogicalType-to-format
 * converters.
 *
 * <p>{@link #V2} is the canonical Logical Types emission. {@link #V1} produces
 * schemas wire-compatible with the Flink converters in
 * {@code cc-flink-schema-converters} — a strict subset of LT capabilities.
 *
 * <p>V1 throws {@code ValidationException} for LT-only constructs that have no
 * Flink equivalent (UNION, VARIANT, TIME/TIMESTAMP precision &gt; 6 in Avro,
 * ENUM in JSON). It auto-promotes anonymous proto ENUMs to named enums.
 * It silently skips LT-only metadata that Flink wouldn't carry.
 *
 * <p>The selected version is also recorded in the Schema Registry metadata
 * under the {@code confluent:edition} key, so consumers can identify which
 * edition produced a given schema.
 */
public enum LogicalTypeVersion {
  V1,
  V2,
}
