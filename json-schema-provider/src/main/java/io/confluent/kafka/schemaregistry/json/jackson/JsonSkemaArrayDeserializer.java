/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.util.ClassUtil;
import com.github.erosb.jsonsKema.JsonArray;
import com.github.erosb.jsonsKema.JsonBoolean;
import com.github.erosb.jsonsKema.JsonNull;
import com.github.erosb.jsonsKema.JsonNumber;
import com.github.erosb.jsonsKema.JsonString;
import com.github.erosb.jsonsKema.JsonValue;
import com.github.erosb.jsonsKema.UnknownSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JsonSkemaArrayDeserializer extends StdDeserializer<JsonArray> {
  private static final long serialVersionUID = 1L;

  public static final JsonSkemaArrayDeserializer instance = new JsonSkemaArrayDeserializer();

  public JsonSkemaArrayDeserializer() {
    super(JsonArray.class);
  }

  @Override
  public JsonArray deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    // 07-Jan-2019, tatu: As per [datatype-json-org#15], need to verify it's an Array
    if (!p.isExpectedStartArrayToken()) {
      final JsonToken t = p.currentToken();
      return (JsonArray) ctxt.handleUnexpectedToken(handledType(),
          t,
          p,
          "Unexpected token (%s), expected START_ARRAY for %s value",
          t,
          ClassUtil.nameOf(handledType())
      );
    }

    List<JsonValue> elements = new ArrayList<>();
    JsonToken t;
    while ((t = p.nextToken()) != JsonToken.END_ARRAY) {
      switch (t) {
        case START_ARRAY:
          elements.add(deserialize(p, ctxt));
          continue;
        case START_OBJECT:
          elements.add(JsonSkemaObjectDeserializer.instance.deserialize(p, ctxt));
          continue;
        case VALUE_STRING:
          elements.add(new JsonString(p.getText(), UnknownSource.INSTANCE));
          continue;
        case VALUE_NULL:
          elements.add(new JsonNull(UnknownSource.INSTANCE));
          continue;
        case VALUE_TRUE:
          elements.add(new JsonBoolean(true, UnknownSource.INSTANCE));
          continue;
        case VALUE_FALSE:
          elements.add(new JsonBoolean(false, UnknownSource.INSTANCE));
          continue;
        case VALUE_NUMBER_INT:
          // Note: added conversion of byte/short
          Number num = p.getNumberValue();
          if (num instanceof Byte || num instanceof Short) {
            num = num.intValue();
          }
          elements.add(new JsonNumber(num, UnknownSource.INSTANCE));
          continue;
        case VALUE_NUMBER_FLOAT:
          elements.add(new JsonNumber(p.getNumberValue(), UnknownSource.INSTANCE));
          continue;
        case VALUE_EMBEDDED_OBJECT:
          // Note: added conversion of byte[]
          Object o = p.getEmbeddedObject();
          if (o instanceof byte[]) {
            elements.add(new JsonString(p.getText(), UnknownSource.INSTANCE));
            continue;
          }
          return (JsonArray) ctxt.handleUnexpectedToken(handledType(), p);
        default:
          return (JsonArray) ctxt.handleUnexpectedToken(handledType(), p);
      }
    }
    return new JsonArray(elements, UnknownSource.INSTANCE);
  }
}
