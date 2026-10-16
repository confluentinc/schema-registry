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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.github.erosb.jsonsKema.JsonArray;
import com.github.erosb.jsonsKema.JsonBoolean;
import com.github.erosb.jsonsKema.JsonNull;
import com.github.erosb.jsonsKema.JsonNumber;
import com.github.erosb.jsonsKema.JsonObject;
import com.github.erosb.jsonsKema.JsonString;
import com.github.erosb.jsonsKema.JsonValue;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

public class JsonSkemaObjectSerializer extends JSONBaseSerializer<JsonObject> {
  private static final long serialVersionUID = 1L;

  public static final JsonSkemaObjectSerializer instance = new JsonSkemaObjectSerializer();

  public JsonSkemaObjectSerializer() {
    super(JsonObject.class);
  }

  @Override // since 2.6
  public boolean isEmpty(SerializerProvider provider, JsonObject value) {
    return (value == null) || value.getProperties().isEmpty();
  }

  @Override
  public void serialize(
      JsonObject value,
      JsonGenerator g,
      SerializerProvider provider
  ) throws IOException {
    g.writeStartObject(value);
    serializeContents(value, g, provider);
    g.writeEndObject();
  }

  @Override
  public void serializeWithType(
      JsonObject value, JsonGenerator g, SerializerProvider provider, TypeSerializer typeSer
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
      JsonObject value,
      JsonGenerator g,
      SerializerProvider provider
  ) throws IOException {
    for (Map.Entry<JsonString, JsonValue> entry : value.getProperties().entrySet()) {
      String key = entry.getKey().getValue();
      JsonValue ob = entry.getValue();
      if (ob == null || ob instanceof JsonNull) {
        if (provider.isEnabled(SerializationFeature.WRITE_NULL_MAP_VALUES)) {
          g.writeNullField(key);
        }
        continue;
      }
      g.writeFieldName(key);
      if (ob instanceof JsonObject) {
        serialize((JsonObject) ob, g, provider);
      } else if (ob instanceof JsonArray) {
        JsonSkemaArraySerializer.instance.serialize((JsonArray) ob, g, provider);
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
