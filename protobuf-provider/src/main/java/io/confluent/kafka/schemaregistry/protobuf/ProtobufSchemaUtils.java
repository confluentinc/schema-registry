/*
 * Copyright 2020 Confluent Inc.
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
 *
 */

package io.confluent.kafka.schemaregistry.protobuf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import io.confluent.kafka.schemaregistry.ParsedSchema;

public class ProtobufSchemaUtils {

  private static final ObjectMapper jsonMapper = new ObjectMapper();

  public static ProtobufSchema copyOf(ProtobufSchema schema) {
    return new ProtobufSchema(schema.canonicalString(),
        schema.references(),
        schema.resolvedReferences(),
        schema.version()
    );
  }

  public static ProtobufSchema getSchema(Object object) {
    if (object == null) {
      return null;
    } else if (object instanceof Message) {
      Message message = (Message) object;
      Descriptors.Descriptor desc = message.getDescriptorForType();
      return new ProtobufSchema(desc);
    } else {
      throw new IllegalArgumentException("Unsupported type of class " + object.getClass()
          .getName());
    }
  }

  public static Object toObject(JsonNode value, ParsedSchema parsedSchema) throws IOException {
    ProtobufSchema schema = (ProtobufSchema) parsedSchema;
    StringWriter out = new StringWriter();
    jsonMapper.writeValue(out, value);
    String jsonString = out.toString();
    DynamicMessage.Builder message = schema.newMessageBuilder();
    JsonFormat.parser().merge(jsonString, message);
    return message.build();
  }

  public static byte[] toJson(Object value) throws IOException {
    if (value == null) {
      return null;
    } else if (value instanceof Message) {
      Message object = (Message) value;
      String jsonString = JsonFormat.printer().print(object);
      return jsonString.getBytes(StandardCharsets.UTF_8);
    } else {
      throw new IllegalArgumentException("Unsupported type of class " + value.getClass()
          .getName());
    }
  }
}