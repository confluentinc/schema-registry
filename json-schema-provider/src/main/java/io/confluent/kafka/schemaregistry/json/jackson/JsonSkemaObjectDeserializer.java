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
import com.github.erosb.jsonsKema.JsonBoolean;
import com.github.erosb.jsonsKema.JsonNull;
import com.github.erosb.jsonsKema.JsonNumber;
import com.github.erosb.jsonsKema.JsonObject;
import com.github.erosb.jsonsKema.JsonString;
import com.github.erosb.jsonsKema.JsonValue;
import com.github.erosb.jsonsKema.UnknownSource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonSkemaObjectDeserializer extends StdDeserializer<JsonObject> {
  private static final long serialVersionUID = 1L;

  public static final JsonSkemaObjectDeserializer instance = new JsonSkemaObjectDeserializer();

  public JsonSkemaObjectDeserializer() {
    super(JsonObject.class);
  }

  @Override
  public JsonObject deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    Map<JsonString, JsonValue> ob = new HashMap<>();
    JsonToken t = p.getCurrentToken();
    if (t == JsonToken.START_OBJECT) {
      t = p.nextToken();
    }
    for (; t == JsonToken.FIELD_NAME; t = p.nextToken()) {
      JsonString fieldName = new JsonString(p.getCurrentName(), UnknownSource.INSTANCE);
      t = p.nextToken();
      switch (t) {
        case START_ARRAY:
          ob.put(fieldName, JsonSkemaArrayDeserializer.instance.deserialize(p, ctxt));
          continue;
        case START_OBJECT:
          ob.put(fieldName, deserialize(p, ctxt));
          continue;
        case VALUE_STRING:
          ob.put(fieldName, new JsonString(p.getText(), UnknownSource.INSTANCE));
          continue;
        case VALUE_NULL:
          ob.put(fieldName, new JsonNull(UnknownSource.INSTANCE));
          continue;
        case VALUE_TRUE:
          ob.put(fieldName, new JsonBoolean(true, UnknownSource.INSTANCE));
          continue;
        case VALUE_FALSE:
          ob.put(fieldName, new JsonBoolean(false, UnknownSource.INSTANCE));
          continue;
        case VALUE_NUMBER_INT:
          // Note: added conversion of byte/short
          Number num = p.getNumberValue();
          if (num instanceof Byte || num instanceof Short) {
            num = num.intValue();
          }
          ob.put(fieldName, new JsonNumber(num, UnknownSource.INSTANCE));
          continue;
        case VALUE_NUMBER_FLOAT:
          ob.put(fieldName, new JsonNumber(p.getNumberValue(), UnknownSource.INSTANCE));
          continue;
        case VALUE_EMBEDDED_OBJECT:
          // Note: added conversion of byte[]
          Object o = p.getEmbeddedObject();
          if (o instanceof byte[]) {
            ob.put(fieldName, new JsonString(p.getText(), UnknownSource.INSTANCE));
            continue;
          }
          return (JsonObject) ctxt.handleUnexpectedToken(JsonObject.class, p);
        default:
      }
      return (JsonObject) ctxt.handleUnexpectedToken(JsonObject.class, p);
    }
    return new JsonObject(ob, UnknownSource.INSTANCE);
  }
}
