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
 */

package io.confluent.kafka.schemaregistry.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.util.ClassUtil;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

public class JSONArrayDeserializer extends StdDeserializer<JSONArray> {
  private static final long serialVersionUID = 1L;

  public static final JSONArrayDeserializer instance = new JSONArrayDeserializer();

  public JSONArrayDeserializer() {
    super(JSONArray.class);
  }

  @Override
  public JSONArray deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    // 07-Jan-2019, tatu: As per [datatype-json-org#15], need to verify it's an Array
    if (!p.isExpectedStartArrayToken()) {
      final JsonToken t = p.currentToken();
      return (JSONArray) ctxt.handleUnexpectedToken(handledType(),
          t,
          p,
          "Unexpected token (%s), expected START_ARRAY for %s value",
          t,
          ClassUtil.nameOf(handledType())
      );
    }

    JSONArray array = new JSONArray();
    JsonToken t;
    while ((t = p.nextToken()) != JsonToken.END_ARRAY) {
      switch (t) {
        case START_ARRAY:
          array.put(deserialize(p, ctxt));
          continue;
        case START_OBJECT:
          array.put(JSONObjectDeserializer.instance.deserialize(p, ctxt));
          continue;
        case VALUE_STRING:
          array.put(p.getText());
          continue;
        case VALUE_NULL:
          array.put(JSONObject.NULL);
          continue;
        case VALUE_TRUE:
          array.put(Boolean.TRUE);
          continue;
        case VALUE_FALSE:
          array.put(Boolean.FALSE);
          continue;
        case VALUE_NUMBER_INT:
          // Note: added conversion of byte/short
          Number num = p.getNumberValue();
          if (num instanceof Byte || num instanceof Short) {
            num = num.intValue();
          }
          array.put(num);
          continue;
        case VALUE_NUMBER_FLOAT:
          array.put(p.getNumberValue());
          continue;
        case VALUE_EMBEDDED_OBJECT:
          // Note: added conversion of byte[]
          Object o = p.getEmbeddedObject();
          if (o instanceof byte[]) {
            o = p.getText();
          }
          array.put(o);
          continue;
        default:
          return (JSONArray) ctxt.handleUnexpectedToken(handledType(), p);
      }
    }
    return array;
  }
}
