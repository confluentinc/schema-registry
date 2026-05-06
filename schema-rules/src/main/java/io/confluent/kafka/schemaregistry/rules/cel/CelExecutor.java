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
import com.google.api.expr.v1alpha1.Type;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils.ScriptType;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;

public class CelExecutor implements RuleExecutor {

  public static final String TYPE = "CEL";

  public static final String CEL_IGNORE_GUARD_SEPARATOR = "cel.ignore.guard.separator";

  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    // Register ProtobufModule to convert CEL objects (such as NullValue) to JSON
    mapper.registerModule(new ProtobufModule());
  }

  private static final int DEFAULT_CACHE_SIZE = 1000;

  private final LoadingCache<RuleWithArgs, Script> cache;

  public CelExecutor() {
    cache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_SIZE)
        .build(new CacheLoader<RuleWithArgs, Script>() {
          @Override
          public Script load(RuleWithArgs ruleWithArgs) throws Exception {
            Object schemaHint;
            switch (ruleWithArgs.getType()) {
              case AVRO:
                schemaHint = ruleWithArgs.getAvroSchema();
                break;
              case JSON:
                schemaHint = ruleWithArgs.getJsonClass();
                break;
              case PROTOBUF:
                schemaHint = ruleWithArgs.getProtobufDesc();
                break;
              default:
                throw new IllegalArgumentException("Unsupported type " + ruleWithArgs.getType());
            }
            return CelUtils.buildScript(ruleWithArgs.getType(), ruleWithArgs.getRule(),
                schemaHint, CelUtils.toDecls(ruleWithArgs.getDeclTypes()));
          }
        });
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Object transform(RuleContext ctx, Object message) throws RuleException {
    Object input;
    if (message instanceof JsonNode) {
      input = mapper.convertValue(message, new TypeReference<Map<String, Object>>(){});
    } else {
      input = message;
    }
    Object result = execute(ctx, input, Collections.singletonMap("message", input));
    if (result instanceof Map) {
      // Convert maps to the target object type
      try {
        JsonNode jsonNode = mapper.valueToTree(result);
        result = ctx.target().fromJson(jsonNode);
      } catch (IOException e) {
        throw new RuleException(ctx.rule(), e);
      }
    }
    return result;
  }

  protected Object execute(
      RuleContext ctx, Object obj, Map<String, Object> args)
      throws RuleException {
    String expr = ctx.rule().getExpr();
    String ignoreGuardStr = ctx.getParameter(CEL_IGNORE_GUARD_SEPARATOR);
    boolean ignoreGuard = Boolean.parseBoolean(ignoreGuardStr);
    if (!ignoreGuard) {
      // An optional guard (followed by semicolon) can precede the expr
      int index = expr.indexOf(';');
      if (index >= 0) {
        String guard = expr.substring(0, index);
        if (!guard.trim().isEmpty()) {
          Object guardResult = Boolean.FALSE;
          try {
            guardResult = execute(ctx, guard, obj, args);
          } catch (RuleException e) {
            // ignore
          }
          if (Boolean.FALSE.equals(guardResult)) {
            // Skip the expr
            return ctx.rule().getKind() == RuleKind.CONDITION ? Boolean.TRUE : obj;
          }
        }
        expr = expr.substring(index + 1);
      }
    }
    return execute(ctx, expr, obj, args);
  }

  private Object execute(RuleContext ctx, String rule, Object obj, Map<String, Object> args)
      throws RuleException {
    try {
      Object msg = args.get("message");
      if (msg == null) {
        msg = obj;
      }
      ScriptType type = ScriptType.JSON;
      if (msg instanceof GenericContainer) {
        type = ScriptType.AVRO;
      } else if (msg instanceof Message) {
        type = ScriptType.PROTOBUF;
      } else if (msg instanceof List<?>) {
        // list not supported
        return obj;
      }

      Map<String, Type> types = CelUtils.toDeclTypes(args);
      RuleWithArgs ruleWithArgs;
      switch (type) {
        case AVRO:
          ruleWithArgs = new RuleWithArgs(rule, type, types, ((GenericContainer) msg).getSchema());
          break;
        case JSON:
          ruleWithArgs = new RuleWithArgs(rule, type, types, msg.getClass());
          break;
        case PROTOBUF:
          ruleWithArgs = new RuleWithArgs(rule, type, types,
              ((Message) msg).getDescriptorForType());
          break;
        default:
          throw new IllegalArgumentException("Unsupported type " + type);
      }
      Script script = cache.get(ruleWithArgs);

      return script.execute(Object.class, args);
    } catch (ScriptException e) {
      throw new RuleException(ctx.rule(), "Could not execute CEL script", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuleException) {
        RuleException re = (RuleException) e.getCause();
        if (re.getRule() == null) {
          throw new RuleException(ctx.rule(), re.getMessage(), re.getCause());
        }
        throw re;
      } else {
        throw new RuleException(ctx.rule(), "Could not get expression", e.getCause());
      }
    }
  }

  static class RuleWithArgs {
    private final String rule;
    private final ScriptType type;
    private final Map<String, Type> declTypes;
    private Schema avroSchema;
    private Class<?> jsonClass;
    private Descriptor protobufDesc;

    public RuleWithArgs(
        String rule, ScriptType type, Map<String, Type> declTypes, Schema avroSchema) {
      this.rule = rule;
      this.type = type;
      this.declTypes = declTypes;
      this.avroSchema = avroSchema;
    }

    public RuleWithArgs(
        String rule, ScriptType type, Map<String, Type> declTypes, Class<?> jsonClass) {
      this.rule = rule;
      this.type = type;
      this.declTypes = declTypes;
      this.jsonClass = jsonClass;
    }

    public RuleWithArgs(
        String rule, ScriptType type, Map<String, Type> declTypes, Descriptor protobufDesc) {
      this.rule = rule;
      this.type = type;
      this.declTypes = declTypes;
      this.protobufDesc = protobufDesc;
    }

    public String getRule() {
      return rule;
    }

    public ScriptType getType() {
      return type;
    }

    public Map<String, Type> getDeclTypes() {
      return declTypes;
    }

    public Schema getAvroSchema() {
      return avroSchema;
    }

    public Class<?> getJsonClass() {
      return jsonClass;
    }

    public Descriptor getProtobufDesc() {
      return protobufDesc;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RuleWithArgs that = (RuleWithArgs) o;
      return Objects.equals(rule, that.rule)
          && type == that.type
          && Objects.equals(declTypes, that.declTypes)
          && Objects.equals(avroSchema, that.avroSchema)
          && Objects.equals(jsonClass, that.jsonClass)
          && Objects.equals(protobufDesc, that.protobufDesc);
    }

    @Override
    public int hashCode() {
      return Objects.hash(rule, type, declTypes, avroSchema, jsonClass, protobufDesc);
    }
  }
}
