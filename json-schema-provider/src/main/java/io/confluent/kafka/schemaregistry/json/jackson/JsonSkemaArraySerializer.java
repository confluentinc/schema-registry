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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.github.erosb.jsonsKema.JsonArray;
import com.github.erosb.jsonsKema.JsonBoolean;
import com.github.erosb.jsonsKema.JsonNull;
import com.github.erosb.jsonsKema.JsonNumber;
import com.github.erosb.jsonsKema.JsonObject;
import com.github.erosb.jsonsKema.JsonString;
import java.io.IOException;
import java.lang.reflect.Type;

public class JsonSkemaArraySerializer extends JsonSkemaBaseSerializer<JsonArray> {
  private static final long serialVersionUID = 1L;

  public static final JsonSkemaArraySerializer instance = new JsonSkemaArraySerializer();

  public JsonSkemaArraySerializer() {
    super(JsonArray.class);
  }

  @Override // since 2.6
  public boolean isEmpty(SerializerProvider provider, JsonArray value) {
    return (value == null) || value.length() == 0;
  }

  @Override
  public void serialize(
      JsonArray value,
      JsonGenerator g,
      SerializerProvider provider
  ) throws IOException {
    g.writeStartArray();
    serializeContents(value, g, provider);
    g.writeEndArray();
  }

  @Override
  public void serializeWithType(
      JsonArray value, JsonGenerator g, SerializerProvider provider, TypeSerializer typeSer
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
      JsonArray value,
      JsonGenerator g,
      SerializerProvider provider
  ) throws IOException {
    for (int i = 0, len = value.length(); i < len; ++i) {
      Object ob = value.get(i);
      if (ob instanceof JsonNull) {
        g.writeNull();
        continue;
      }
      if (ob instanceof JsonObject) {
        JsonSkemaObjectSerializer.instance.serialize((JsonObject) ob, g, provider);
      } else if (ob instanceof JsonArray) {
        serialize((JsonArray) ob, g, provider);
      } else if (ob instanceof JsonString) {
        g.writeString(((JsonString) ob).getValue());
      } else if (ob instanceof JsonNumber) {
        Number num = ((JsonNumber) ob).getValue();
        if (num instanceof Double) {
          g.writeNumber(num.doubleValue());
        } else if (num instanceof Float) {
          g.writeNumber(num.floatValue());
        } else if (num instanceof Long) {
          g.writeNumber(num.longValue());
        } else {
          g.writeNumber(num.intValue());
        }
      } else if (ob instanceof JsonBoolean) {
        g.writeBoolean(((JsonBoolean) ob).getValue());
      } else {
        provider.defaultSerializeValue(ob, g);
      }
    }
  }
}
