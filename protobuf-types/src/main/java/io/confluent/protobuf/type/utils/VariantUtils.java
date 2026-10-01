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

package io.confluent.protobuf.type.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.confluent.protobuf.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantArrayBuilder;
import io.confluent.kafka.schemaregistry.type.VariantBuilder;
import io.confluent.kafka.schemaregistry.type.VariantObjectBuilder;
import java.nio.ByteBuffer;
import java.util.Map;

public class VariantUtils {

  public static io.confluent.kafka.schemaregistry.type.Variant toKafkaVariant(Variant variant) {
    ByteBuffer metadata = variant.getMetadata().asReadOnlyByteBuffer();
    ByteBuffer value = variant.getValue().asReadOnlyByteBuffer();
    return new io.confluent.kafka.schemaregistry.type.Variant(value, metadata);
  }

  public static io.confluent.kafka.schemaregistry.type.Variant toKafkaVariant(Message message) {
    ByteBuffer metadata = ByteBuffer.wrap(new byte[0]);
    ByteBuffer value = ByteBuffer.wrap(new byte[0]);
    for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      if (entry.getKey().getName().equals("metadata")) {
        metadata = ((ByteString) entry.getValue()).asReadOnlyByteBuffer();
      } else if (entry.getKey().getName().equals("value")) {
        value = ((ByteString) entry.getValue()).asReadOnlyByteBuffer();
      }
    }
    return new io.confluent.kafka.schemaregistry.type.Variant(value, metadata);
  }

  public static io.confluent.kafka.schemaregistry.type.Variant toKafkaVariant(Value value) {
    VariantBuilder builder = new VariantBuilder();
    buildFromValue(builder, value);
    return builder.build();
  }

  public static Variant fromKafkaVariant(io.confluent.kafka.schemaregistry.type.Variant variant) {
    return Variant.newBuilder()
        .setMetadata(ByteString.copyFrom(variant.getMetadataBuffer()))
        .setValue(ByteString.copyFrom(variant.getValueBuffer()))
        .build();
  }

  public static Variant fromValue(Value value) {
    return fromKafkaVariant(toKafkaVariant(value));
  }

  private static void buildFromValue(VariantBuilder builder, Value value) {
    switch (value.getKindCase()) {
      case NULL_VALUE:
        builder.appendNull();
        break;
      case NUMBER_VALUE:
        builder.appendDouble(value.getNumberValue());
        break;
      case STRING_VALUE:
        builder.appendString(value.getStringValue());
        break;
      case BOOL_VALUE:
        builder.appendBoolean(value.getBoolValue());
        break;
      case STRUCT_VALUE:
        buildFromStruct(builder, value.getStructValue());
        break;
      case LIST_VALUE:
        buildFromList(builder, value.getListValue());
        break;
      default:
        builder.appendNull();
        break;
    }
  }

  private static void buildFromStruct(VariantBuilder builder, Struct struct) {
    VariantObjectBuilder obj = builder.startObject();
    for (Map.Entry<String, Value> field : struct.getFieldsMap().entrySet()) {
      obj.appendKey(field.getKey());
      buildFromValue(obj, field.getValue());
    }
    builder.endObject();
  }

  private static void buildFromList(VariantBuilder builder, ListValue list) {
    VariantArrayBuilder arr = builder.startArray();
    for (Value elem : list.getValuesList()) {
      buildFromValue(arr, elem);
    }
    builder.endArray();
  }
}
