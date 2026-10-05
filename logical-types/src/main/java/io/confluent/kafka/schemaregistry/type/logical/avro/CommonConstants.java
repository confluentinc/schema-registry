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

package io.confluent.kafka.schemaregistry.type.logical.avro;

/** Common constants used for converting AVRO schema. */
public class CommonConstants {

  static final String CONNECT_TYPE_PROP = "connect.type";
  static final String KEY_FIELD = "key";
  static final String VALUE_FIELD = "value";
  static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
  static final String AVRO_LOGICAL_DECIMAL = "decimal";
  static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
  static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
  static final String AVRO_LOGICAL_DATE = "date";
  static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
  static final String AVRO_LOGICAL_TIME_MICROS = "time-micros";
  static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";
  static final String AVRO_LOGICAL_TIMESTAMP_MICROS = "timestamp-micros";
  static final String AVRO_LOGICAL_TIMESTAMP_NANOS = "timestamp-nanos";
  static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
  static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";
  static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_NANOS = "local-timestamp-nanos";

  static final String FLINK_PRECISION = "flink.precision";
  static final String FLINK_MIN_LENGTH = "flink.minLength";
  static final String FLINK_MAX_LENGTH = "flink.maxLength";
  static final String FLINK_PROPERTY_VERSION = "flink.version";
  static final String FLINK_PROPERTY_CURRENT_VERSION = "1";
  public static final String FLINK_TYPE = "flink.type";
  public static final String FLINK_MULTISET_TYPE = "multiset";

  // LT-internal markers — reserved namespace; not user-defined params.
  // Readers strip any key starting with "logical." from user-visible params,
  // so user-supplied SQL `WITH params` never collide with these.
  static final String LOGICAL_PREFIX = "logical.";
  static final String LOGICAL_ANONYMOUS_PROP = "logical.anonymous";
  static final String LOGICAL_KEY_LENGTH_PROP = "logical.key.length";
  static final String LOGICAL_KEY_TYPE_PROP = "logical.key.type";

  // Legacy MapEntry markers — used for wire-compat with Flink-Avro and
  // Connect AvroData (neither recognizes logicalType=map):
  //  - The writer emits CONNECT_INTERNAL_TYPE_PROP=MAP_ENTRY_TYPE_NAME on the
  //    entry record so legacy isMapEntry predicates match (LT uses per-map
  //    unique entry names, so the canonical-name predicate alone wouldn't fire).
  //  - The reader accepts both legacy forms as fallbacks:
  //      * Canonical name (CONNECT_AVRO_NAMESPACE + MAP_ENTRY_TYPE_NAME):
  //        Flink-Avro and AvroData (anonymous Connect schemas).
  //      * CONNECT_INTERNAL_TYPE_PROP=MAP_ENTRY_TYPE_NAME prop: AvroData
  //        (named Connect schemas).
  static final String CONNECT_AVRO_NAMESPACE = "io.confluent.connect.avro";
  static final String CONNECT_INTERNAL_TYPE_PROP = "connect.internal.type";
  static final String MAP_ENTRY_TYPE_NAME = "MapEntry";

  private CommonConstants() {}
}
