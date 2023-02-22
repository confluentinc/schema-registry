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

package io.confluent.kafka.schemaregistry.rules.jsonata;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.EvaluateRuntimeException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import java.io.IOException;

public class JsonataExecutor implements RuleExecutor {

  public static final String TYPE = "JSONATA";

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Object transform(RuleContext ctx, Object message)
      throws RuleException {
    JsonNode jsonObj = (JsonNode) message;
    Expressions expr;
    try {
      expr = Expressions.parse(ctx.rule().getExpr());
    } catch (ParseException e) {
      throw new RuleException("Could not parse expression", e);
    } catch (EvaluateRuntimeException ere) {
      throw new RuleException("Could not evaluate expression", ere);
    } catch (JsonProcessingException e) {
      throw new RuleException("Could not parse message", e);
    } catch (IOException e) {
      throw new RuleException(e);
    }
    try {
      JsonNode result = expr.evaluate(jsonObj);
      return result;
    } catch (EvaluateException e) {
      throw new RuleException("Could not evaluate expression", e);
    }
  }
}
