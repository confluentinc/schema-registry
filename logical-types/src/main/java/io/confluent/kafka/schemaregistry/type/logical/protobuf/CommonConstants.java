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

package io.confluent.kafka.schemaregistry.type.logical.protobuf;

/** Common constants used for converting Protobuf schema. */
public class CommonConstants {

  static final String CONNECT_TYPE_PROP = "connect.type";
  static final String CONNECT_TYPE_INT8 = "int8";
  static final String CONNECT_TYPE_INT16 = "int16";
  static final String PROTOBUF_DECIMAL_TYPE = "confluent.type.Decimal";
  static final String PROTOBUF_DECIMAL_LOCATION = "confluent/type/decimal.proto";
  static final String PROTOBUF_VARIANT_TYPE = "confluent.type.Variant";
  static final String PROTOBUF_VARIANT_LOCATION = "confluent/type/variant.proto";
  static final String PROTOBUF_DATE_TYPE = "google.type.Date";
  static final String PROTOBUF_DATE_LOCATION = "google/type/date.proto";
  static final String PROTOBUF_TIME_TYPE = "google.type.TimeOfDay";
  static final String PROTOBUF_TIME_LOCATION = "google/type/timeofday.proto";
  static final String PROTOBUF_PRECISION_PROP = "precision";
  static final String PROTOBUF_SCALE_PROP = "scale";
  static final String PROTOBUF_TIMESTAMP_TYPE = "google.protobuf.Timestamp";
  static final String PROTOBUF_TIMESTAMP_LOCATION = "google/protobuf/timestamp.proto";
  static final String MAP_ENTRY_SUFFIX = "Entry"; // Suffix used by protoc
  static final String KEY_FIELD = "key";
  static final String VALUE_FIELD = "value";

  static final String PROTOBUF_DOUBLE_WRAPPER_TYPE = "google.protobuf.DoubleValue";
  static final String PROTOBUF_FLOAT_WRAPPER_TYPE = "google.protobuf.FloatValue";
  static final String PROTOBUF_INT64_WRAPPER_TYPE = "google.protobuf.Int64Value";
  static final String PROTOBUF_UINT64_WRAPPER_TYPE = "google.protobuf.UInt64Value";
  static final String PROTOBUF_INT32_WRAPPER_TYPE = "google.protobuf.Int32Value";
  static final String PROTOBUF_UINT32_WRAPPER_TYPE = "google.protobuf.UInt32Value";
  static final String PROTOBUF_BOOL_WRAPPER_TYPE = "google.protobuf.BoolValue";
  static final String PROTOBUF_STRING_WRAPPER_TYPE = "google.protobuf.StringValue";
  static final String PROTOBUF_BYTES_WRAPPER_TYPE = "google.protobuf.BytesValue";

  static final String FLINK_PRECISION_PROP = "flink.precision";
  static final String FLINK_TYPE_PROP = "flink.type";
  static final String FLINK_TYPE_TIMESTAMP = "timestamp";
  static final String FLINK_TYPE_MULTISET = "multiset";
  static final String FLINK_MIN_LENGTH = "flink.minLength";
  static final String FLINK_MAX_LENGTH = "flink.maxLength";
  static final String FLINK_NOT_NULL = "flink.notNull";

  // Synthesized message/enum names.
  //
  // All synthesized type names follow a single rule:
  //
  //     <UpperCamelCase(fieldName)> + <Suffix>
  //
  // matching proto3's UpperCamelCase style guide for message/enum
  // identifiers. The transform is applied uniformly at every synthesis site
  // (see toMessageName in LogicalTypeToProtoConverter).
  //
  // Two kinds of synthesized names exist:
  //
  // (a) Anonymous nested type promotion — when an LT structure has no
  //     explicit name, we synthesize one from its containing field.
  //       Suffix          LT case
  //       --------        --------------------------------
  //       Row             anonymous STRUCT field
  //       Enum            anonymous ENUM field
  //       Entry           map-entry message (matches protoc convention)
  //
  // (b) Grammar wrappers — proto3 forbids six (outer × inner) combinations
  //     where an LT shape can't sit at a given position. We synthesize a
  //     one-field wrapper struct `{ value: <X> }`, emit a TYPE_MESSAGE field
  //     referring to it, and mark the field with `flink.wrapped=true`.
  //     Treat MAP/MULTISET as `repeated` (encoded as repeated map-entry
  //     messages), UNION as `oneof`, nullable as `optional`.
  //
  //       forbidden combination     LT case                                  suffix
  //       ----------------------    -------------------------------------    ----------------
  //       optional repeated         nullable ARRAY/MAP/MULTISET as field     RepeatedWrapper
  //       oneof    repeated         ARRAY/MAP/MULTISET as oneof branch       RepeatedWrapper
  //       oneof    oneof            UNION as oneof branch                    OneofWrapper
  //       repeated repeated         composite element of ARRAY               ElementWrapper
  //       repeated optional         nullable element of ARRAY                ElementWrapper
  //       repeated oneof            UNION element of ARRAY                   ElementWrapper
  //
  //     Wrapper suffix follows a precedence rule:
  //       - INSIDE a `repeated` (the bottom three rows): name by POSITION →
  //         ElementWrapper. The wrap-reason is uniformly "couldn't sit as a
  //         bare repeated element"; the reader treats all three identically.
  //       - OUTSIDE a `repeated` (the top three rows): name by CONTENT →
  //         RepeatedWrapper for repeated/map contents, OneofWrapper for
  //         oneof contents.
  static final String FLINK_REPEATED_WRAPPER_SUFFIX = "RepeatedWrapper";
  static final String FLINK_ELEMENT_WRAPPER_SUFFIX = "ElementWrapper";
  static final String FLINK_ONEOF_WRAPPER_SUFFIX = "OneofWrapper";
  static final String FLINK_ENUM_SUFFIX = "Enum";
  static final String FLINK_ROW_SUFFIX = "Row";
  static final String FLINK_PROPERTY_VERSION = "flink.version";
  static final String FLINK_PROPERTY_CURRENT_VERSION = "1";
  public static final String FLINK_WRAPPER = "flink.wrapped";
  public static final String FLINK_WRAPPER_FIELD_NAME = "value";

  // LT-internal markers — reserved namespace; not user-defined params.
  static final String LOGICAL_PREFIX = "logical.";
  static final String LOGICAL_DEFAULT_PROP = "logical.default";

  /**
   * Marker placed on a nested message's or enum's {@code Meta} params to
   * indicate that it corresponds to a user-declared named type — not a
   * writer-synthesized wrapper. The reader uses this to lift the nested type
   * into {@code localNamedTypes} (marked) rather than inlining it (unmarked).
   *
   * <p>Mirror-image of Avro/JSON's {@code logical.anonymous}: those formats
   * default to "lift as named" and use the marker to flag wrappers, because
   * Avro records and JSON {@code $defs} entries are inherently name-addressed.
   * Proto's wire format admits both nested-named and synthesized-anonymous
   * structures with no native distinction, so it defaults to "inline" and
   * uses this marker to flag the user-declared kind.
   *
   * <p>The marker lives in the {@link #LOGICAL_PREFIX logical.*} namespace,
   * so the reader's {@code isUserParam} filter strips it automatically and it
   * never leaks into the LT model's user-visible params.
   */
  public static final String LOGICAL_NAMED_PROP = "logical.named";

  private CommonConstants() {}
}
