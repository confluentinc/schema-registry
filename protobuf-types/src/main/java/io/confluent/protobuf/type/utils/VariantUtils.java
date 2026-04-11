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
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.type.Variant;
import java.nio.ByteBuffer;
import java.util.Map;

public class VariantUtils {

  public static Variant toVariant(io.confluent.protobuf.type.Variant variant) {
    ByteBuffer metadata = variant.getMetadata().asReadOnlyByteBuffer();
    ByteBuffer value = variant.getValue().asReadOnlyByteBuffer();
    return new Variant(value, metadata);
  }

  public static Variant toVariant(Message message) {
    ByteBuffer metadata = ByteBuffer.wrap(new byte[0]);
    ByteBuffer value = ByteBuffer.wrap(new byte[0]);
    for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      if (entry.getKey().getName().equals("metadata")) {
        metadata = ((ByteString) entry.getValue()).asReadOnlyByteBuffer();
      } else if (entry.getKey().getName().equals("value")) {
        value = ((ByteString) entry.getValue()).asReadOnlyByteBuffer();
      }
    }
    return new Variant(value, metadata);
  }

  public static io.confluent.protobuf.type.Variant fromVariant(Variant variant) {
    return io.confluent.protobuf.type.Variant.newBuilder()
        .setMetadata(ByteString.copyFrom(variant.getMetadataBuffer()))
        .setValue(ByteString.copyFrom(variant.getValueBuffer()))
        .build();
  }
}
