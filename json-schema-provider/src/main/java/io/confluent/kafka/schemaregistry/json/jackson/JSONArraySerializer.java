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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.Type;

public class JSONArraySerializer extends JSONBaseSerializer<JSONArray> {
  private static final long serialVersionUID = 1L;

  public static final JSONArraySerializer instance = new JSONArraySerializer();

  public JSONArraySerializer() {
    super(JSONArray.class);
  }

  @Override // since 2.6
  public boolean isEmpty(SerializerProvider provider, JSONArray value) {
    return (value == null) || value.length() == 0;
  }

  @Override
  public void serialize(
      JSONArray value,
      JsonGenerator g,
      SerializerProvider provider
  ) throws IOException {
    g.writeStartArray();
    serializeContents(value, g, provider);
    g.writeEndArray();
  }

  @Override
  public void serializeWithType(
      JSONArray value, JsonGenerator g, SerializerProvider provider, TypeSerializer typeSer
  ) throws IOException {
    g.setCurrentValue(value);
    WritableTypeId typeIdDef = typeSer.writeTypePrefix(g,
        typeSer.typeId(value, JsonToken.START_ARRAY)
    );
    serializeContents(value, g, provider);
    typeSer.writeTypeSuffix(g, typeIdDef);
  }

  @Override
  public JsonNode getSchema(
      SerializerProvider provider,
      Type typeHint
  ) throws JsonMappingException {
    return createSchemaNode("array", true);
  }

  protected void serializeContents(
      JSONArray value,
      JsonGenerator g,
      SerializerProvider provider
  ) throws IOException {
    for (int i = 0, len = value.length(); i < len; ++i) {
      Object ob = value.opt(i);
      if (ob == null || ob == JSONObject.NULL) {
        g.writeNull();
        continue;
      }
      Class<?> cls = ob.getClass();
      if (cls == JSONObject.class) {
        JSONObjectSerializer.instance.serialize((JSONObject) ob, g, provider);
      } else if (cls == JSONArray.class) {
        serialize((JSONArray) ob, g, provider);
      } else if (cls == String.class) {
        g.writeString((String) ob);
      } else if (cls == Integer.class) {
        g.writeNumber(((Integer) ob).intValue());
      } else if (cls == Long.class) {
        g.writeNumber(((Long) ob).longValue());
      } else if (cls == Boolean.class) {
        g.writeBoolean(((Boolean) ob).booleanValue());
      } else if (cls == Double.class) {
        g.writeNumber(((Double) ob).doubleValue());
      } else if (JSONObject.class.isAssignableFrom(cls)) { // sub-class
        JSONObjectSerializer.instance.serialize((JSONObject) ob, g, provider);
      } else if (JSONArray.class.isAssignableFrom(cls)) { // sub-class
        serialize((JSONArray) ob, g, provider);
      } else {
        provider.defaultSerializeValue(ob, g);
      }
    }
  }
}
