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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Iterator;

public class JSONObjectSerializer extends JSONBaseSerializer<JSONObject> {
  private static final long serialVersionUID = 1L;

  public static final JSONObjectSerializer instance = new JSONObjectSerializer();

  public JSONObjectSerializer() {
    super(JSONObject.class);
  }

  @Override // since 2.6
  public boolean isEmpty(SerializerProvider provider, JSONObject value) {
    return (value == null) || value.length() == 0;
  }

  @Override
  public void serialize(
      JSONObject value,
      JsonGenerator g,
      SerializerProvider provider
  ) throws IOException {
    g.writeStartObject(value);
    serializeContents(value, g, provider);
    g.writeEndObject();
  }

  @Override
  public void serializeWithType(
      JSONObject value, JsonGenerator g, SerializerProvider provider, TypeSerializer typeSer
  ) throws IOException {
    g.setCurrentValue(value);
    WritableTypeId typeIdDef = typeSer.writeTypePrefix(g,
        typeSer.typeId(value, JsonToken.START_OBJECT)
    );
    serializeContents(value, g, provider);
    typeSer.writeTypeSuffix(g, typeIdDef);

  }

  @Override
  public JsonNode getSchema(
      SerializerProvider provider,
      Type typeHint
  ) throws JsonMappingException {
    return createSchemaNode("object", true);
  }

  protected void serializeContents(
      JSONObject value,
      JsonGenerator g,
      SerializerProvider provider
  ) throws IOException {
    Iterator<?> it = value.keys();
    while (it.hasNext()) {
      String key = (String) it.next();
      Object ob = value.opt(key);
      if (ob == null || ob == JSONObject.NULL) {
        if (provider.isEnabled(SerializationFeature.WRITE_NULL_MAP_VALUES)) {
          g.writeNullField(key);
        }
        continue;
      }
      g.writeFieldName(key);
      Class<?> cls = ob.getClass();
      if (cls == JSONObject.class) {
        serialize((JSONObject) ob, g, provider);
      } else if (cls == JSONArray.class) {
        JSONArraySerializer.instance.serialize((JSONArray) ob, g, provider);
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
        serialize((JSONObject) ob, g, provider);
      } else if (JSONArray.class.isAssignableFrom(cls)) { // sub-class
        JSONArraySerializer.instance.serialize((JSONArray) ob, g, provider);
      } else {
        provider.defaultSerializeValue(ob, g);
      }
    }
  }
}
