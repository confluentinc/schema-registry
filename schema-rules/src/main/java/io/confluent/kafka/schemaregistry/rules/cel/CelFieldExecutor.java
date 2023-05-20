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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.rules.FieldRuleExecutor;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CelFieldExecutor implements FieldRuleExecutor {

  public static final String TYPE = "CEL_FIELD";

  private static final ObjectMapper mapper = new ObjectMapper();

  public String type() {
    return TYPE;
  }

  @Override
  public FieldTransform newTransform(RuleContext ruleContext) {
    return (ctx, fieldCtx, fieldValue) -> {
      Object message = fieldCtx.getContainingMessage();
      Object inputMessage;
      if (message instanceof JsonNode) {
        inputMessage = mapper.convertValue(message, new TypeReference<Map<String, Object>>(){});
      } else {
        inputMessage = message;
      }
      return CelExecutor.execute(ctx, fieldValue, new HashMap<String, Object>() {
            {
              put("value",
                  fieldValue != null ? fieldValue : com.google.protobuf.NullValue.NULL_VALUE);
              put("fullName", fieldCtx.getFullName());
              put("name", fieldCtx.getName());
              put("typeName", fieldCtx.getType().name());
              put("tags", new ArrayList<>(fieldCtx.getTags()));
              put("message", inputMessage);
            }
          }
      );
    };
  }
}
