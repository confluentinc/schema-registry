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

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.FieldRuleExecutor;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import java.util.ArrayList;

public class CelFieldExecutor extends CelExecutor implements FieldRuleExecutor {

  public static final String TYPE = "CEL_FIELD";

  public String type() {
    return TYPE;
  }

  @Override
  public Object transform(RuleContext ctx, Object message) throws RuleException {
    return ctx.target().transformMessage(ctx, newTransform(ctx), message);
  }

  @Override
  public FieldTransform newTransform(RuleContext ruleContext) {
    return (ctx, fieldCtx, fieldValue) ->
        execute(ctx, fieldValue, ImmutableMap.of(
            "value", fieldValue, "fullName", fieldCtx.getFullName(), "name", fieldCtx.getName(),
            "tags", new ArrayList<>(fieldCtx.getTags()),
            "message", fieldCtx.getContainingMessage()));
  }
}
