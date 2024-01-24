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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.config.ConfigException;

public class JsonataExecutor implements RuleExecutor {

  public static final String TYPE = "JSONATA";

  public static final String TIMEOUT_MS = "timeout.ms";
  public static final String MAX_DEPTH = "max.depth";

  private long timeoutMs = 60000;
  private int maxDepth = 1000;

  private static final int DEFAULT_CACHE_SIZE = 100;

  private final LoadingCache<String, Expressions> cache;

  public JsonataExecutor() {
    cache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_SIZE)
        .build(new CacheLoader<String, Expressions>() {
          @Override
          public Expressions load(String expr) throws Exception {
            try {
              return Expressions.parse(expr);
            } catch (ParseException e) {
              throw new RuleException("Could not parse expression", e);
            } catch (EvaluateRuntimeException ere) {
              throw new RuleException("Could not evaluate expression", ere);
            } catch (JsonProcessingException e) {
              throw new RuleException("Could not parse message", e);
            } catch (IOException e) {
              throw new RuleException(e);
            }
          }
        });
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Object timeoutMsConfig = configs.get(TIMEOUT_MS);
    if (timeoutMsConfig != null) {
      try {
        this.timeoutMs = Long.parseLong(timeoutMsConfig.toString());
      } catch (NumberFormatException e) {
        throw new ConfigException("Cannot parse " + TIMEOUT_MS);
      }
    }
    Object maxDepthConfig = configs.get(MAX_DEPTH);
    if (maxDepthConfig != null) {
      try {
        this.maxDepth = Integer.parseInt(maxDepthConfig.toString());
      } catch (NumberFormatException e) {
        throw new ConfigException("Cannot parse " + MAX_DEPTH);
      }
    }
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Object transform(RuleContext ctx, Object message)
      throws RuleException {
    JsonNode jsonObj;
    if (message instanceof JsonNode) {
      jsonObj = (JsonNode) message;
    } else {
      try {
        jsonObj = ctx.target().toJson(message);
      } catch (IOException e) {
        throw new RuleException(e);
      }
    }
    Expressions expr;
    try {
      expr = cache.get(ctx.rule().getExpr());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuleException) {
        throw (RuleException) e.getCause();
      } else {
        throw new RuleException("Could not get expression", e.getCause());
      }
    }
    try {
      JsonNode result = expr.evaluate(jsonObj, timeoutMs, maxDepth);
      return ctx.rule().getKind() == RuleKind.CONDITION ? result.asBoolean(true) : result;
    } catch (EvaluateException e) {
      throw new RuleException("Could not evaluate expression", e);
    }
  }
}
