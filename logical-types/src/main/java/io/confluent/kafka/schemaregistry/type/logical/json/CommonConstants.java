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

package io.confluent.kafka.schemaregistry.type.logical.json;

/** Common constants used for converting JSON schema. */
public class CommonConstants {

  public static final String CONNECT_TYPE_PROP = "connect.type";
  public static final String CONNECT_TYPE_INT8 = "int8";
  public static final String CONNECT_TYPE_INT16 = "int16";
  public static final String CONNECT_TYPE_INT32 = "int32";
  public static final String CONNECT_TYPE_INT64 = "int64";
  public static final String CONNECT_TYPE_FLOAT32 = "float32";
  public static final String CONNECT_TYPE_FLOAT64 = "float64";
  public static final String CONNECT_TYPE_BYTES = "bytes";
  public static final String CONNECT_TYPE_MAP = "map";
  public static final String KEY_FIELD = "key";
  public static final String VALUE_FIELD = "value";
  public static final String CONNECT_INDEX_PROP = "connect.index";
  public static final String CONNECT_TYPE_TIME = "org.apache.kafka.connect.data.Time";
  public static final String CONNECT_TYPE_DATE = "org.apache.kafka.connect.data.Date";
  public static final String CONNECT_TYPE_TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";
  public static final String CONNECT_TYPE_DECIMAL = "org.apache.kafka.connect.data.Decimal";
  public static final String CONNECT_PARAMETERS = "connect.parameters";
  public static final String CONNECT_TYPE_DECIMAL_SCALE = "scale";
  public static final String CONNECT_TYPE_DECIMAL_PRECISION = "connect.decimal.precision";
  public static final String GENERALIZED_TYPE_UNION_PREFIX = "connect_union_field_";

  // LT-internal markers — reserved namespace; not user-defined params.
  public static final String LOGICAL_PREFIX = "logical.";
  public static final String LOGICAL_KEY_LENGTH_PROP = "logical.key.length";
  public static final String LOGICAL_KEY_TYPE_PROP = "logical.key.type";

  public static final String FLINK_TYPE_PROP = "flink.type";
  public static final String FLINK_PROPERTY_VERSION = "flink.version";
  public static final String FLINK_PROPERTY_CURRENT_VERSION = "1";
  public static final String FLINK_TYPE_TIMESTAMP = "timestamp";
  public static final String FLINK_TYPE_MULTISET = "multiset";
  public static final String FLINK_PRECISION = "flink.precision";
  static final String FLINK_MIN_LENGTH = "flink.minLength";
  static final String FLINK_MAX_LENGTH = "flink.maxLength";

  private CommonConstants() {}
}
