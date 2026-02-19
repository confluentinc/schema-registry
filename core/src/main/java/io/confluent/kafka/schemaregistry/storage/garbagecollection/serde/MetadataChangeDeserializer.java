/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage.garbagecollection.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.confluent.protobuf.events.catalog.v1.MetadataChange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MetadataChangeDeserializer {
  private static final String PROTOBUF = "protobuf";
  private static final String JSON = "json";
  private static ObjectMapper MAPPER = new ObjectMapper();

  public MetadataChange deserialize(byte[] data, String format) {
    MetadataChange metadataChange;

    try {
      if (format.equals(PROTOBUF)) {
        metadataChange = MetadataChange.parseFrom(data);
      } else if (format.equals(JSON)) {
        MetadataChange.Builder builder = MetadataChange.newBuilder();
        String str = new String(data, StandardCharsets.UTF_8);
        JsonFormat.parser().merge(str, builder);
        metadataChange = builder.build();
      } else {
        throw new IllegalArgumentException(
                "Unsupported MetadataChange deserialization format: " + format);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Can't parse metadataChange with format " + format, e);
    }
    return metadataChange;
  }
}
