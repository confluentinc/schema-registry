/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rules.cel;

import static com.google.protobuf.NullValue.NULL_VALUE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import dev.cel.common.values.CelByteString;
import io.confluent.kafka.schemaregistry.rules.FieldRuleExecutor;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CelFieldExecutor extends FieldRuleExecutor {

  public static final String TYPE = "CEL_FIELD";

  private static final ObjectMapper JSON_MAPPER = JacksonMapper.newObjectMapper()
      .registerModule(new ProtobufModule());

  private CelExecutor celExecutor = new CelExecutor();

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    celExecutor.configure(configs);
  }

  public String type() {
    return TYPE;
  }

  @Override
  public FieldTransform newTransform(RuleContext ruleContext) {
    return (ctx, fieldCtx, fieldValue) -> {
      if (!fieldCtx.getType().isPrimitive()) {
        // CEL field transforms only apply to primitive types
        return fieldValue;
      }
      Object message = fieldCtx.getContainingMessage();
      Object inputMessage;
      if (message instanceof JsonNode) {
        // Defensive: containingMessage is normally an ObjectNode for JSON
        // Schema records, but wrap the conversion to surface a typed
        // RuleException rather than a raw Jackson IllegalArgumentException
        // if it ever isn't.
        try {
          inputMessage = JSON_MAPPER.convertValue(
              message, new TypeReference<Map<String, Object>>(){});
        } catch (IllegalArgumentException e) {
          throw new RuleException(ctx.rule(), e);
        }
      } else {
        inputMessage = message;
      }
      Object result = celExecutor.execute(ctx, fieldValue, new HashMap<String, Object>() {
            {
              put("value", fieldValue != null ? fieldValue : NULL_VALUE);
              put("fullName", fieldCtx.getFullName());
              put("name", fieldCtx.getName());
              put("typeName", fieldCtx.getType().name());
              put("tags", new ArrayList<>(fieldCtx.getTags()));
              put("message", inputMessage);
            }
          }
      );
      if (result instanceof com.google.protobuf.NullValue
          || result instanceof dev.cel.common.values.NullValue) {
        // CEL `null` literal evaluates to dev.cel.common.values.NullValue;
        // the `value` binding uses proto NULL_VALUE for null field inputs
        // (and so for the result of a rule that echoes a null value). Field
        // setters expect Java null — normalize both flavors here so a
        // nullable target sees null and a non-nullable target surfaces the
        // contract violation directly instead of choking on a sentinel.
        result = null;
      } else if (result instanceof ByteString) {
        result = ((ByteString) result).toByteArray();
      } else if (result instanceof CelByteString) {
        // CelByteString is what CEL bytes literals (b"...") evaluate to.
        // Hand back a raw byte[] so the per-format field setters
        // (ProtobufSchema, AvroSchema, JsonSchema) can take their normal byte[]
        // → ByteString / ByteBuffer path.
        result = ((CelByteString) result).toByteArray();
      } else if (result instanceof Number) {
        Number num = (Number) result;
        switch (fieldCtx.getType()) {
          case INT:
            result = num.intValue();
            break;
          case LONG:
            result = num.longValue();
            break;
          case FLOAT:
            result = num.floatValue();
            break;
          case DOUBLE:
            result = num.doubleValue();
            break;
          default:
            break;
        }
      }
      return result;
    };
  }
}
