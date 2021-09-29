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
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class JSONObjectDeserializer extends StdDeserializer<JSONObject> {
  private static final long serialVersionUID = 1L;

  public static final JSONObjectDeserializer instance = new JSONObjectDeserializer();

  public JSONObjectDeserializer() {
    super(JSONObject.class);
  }

  @Override
  public JSONObject deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JSONObject ob = new JSONObject();
    JsonToken t = p.getCurrentToken();
    if (t == JsonToken.START_OBJECT) {
      t = p.nextToken();
    }
    for (; t == JsonToken.FIELD_NAME; t = p.nextToken()) {
      String fieldName = p.getCurrentName();
      t = p.nextToken();
      try {
        switch (t) {
          case START_ARRAY:
            ob.put(fieldName, JSONArrayDeserializer.instance.deserialize(p, ctxt));
            continue;
          case START_OBJECT:
            ob.put(fieldName, deserialize(p, ctxt));
            continue;
          case VALUE_STRING:
            ob.put(fieldName, p.getText());
            continue;
          case VALUE_NULL:
            ob.put(fieldName, JSONObject.NULL);
            continue;
          case VALUE_TRUE:
            ob.put(fieldName, Boolean.TRUE);
            continue;
          case VALUE_FALSE:
            ob.put(fieldName, Boolean.FALSE);
            continue;
          case VALUE_NUMBER_INT:
            // Note: added conversion of byte/short
            Number num = p.getNumberValue();
            if (num instanceof Byte || num instanceof Short) {
              num = num.intValue();
            }
            ob.put(fieldName, num);
            continue;
          case VALUE_NUMBER_FLOAT:
            ob.put(fieldName, p.getNumberValue());
            continue;
          case VALUE_EMBEDDED_OBJECT:
            // Note: added conversion of byte[]
            Object o = p.getEmbeddedObject();
            if (o instanceof byte[]) {
              o = p.getText();
            }
            ob.put(fieldName, o);
            continue;
          default:
        }
      } catch (JSONException e) {
        throw ctxt.instantiationException(handledType(), e);
      }
      return (JSONObject) ctxt.handleUnexpectedToken(JSONObject.class, p);
    }
    return ob;
  }
}
