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

import static io.confluent.kafka.schemaregistry.rules.cel.avro.AvroTypeDescription.NULL_AVRO_SCHEMA;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.expr.v1alpha1.Decl;
import com.google.api.expr.v1alpha1.Type;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import io.confluent.kafka.schemaregistry.rules.cel.avro.AvroRegistry;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.BuiltinLibrary;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.pb.Checked;
import org.projectnessie.cel.extension.StringsLib;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.tools.ScriptHost.ScriptBuilder;
import org.projectnessie.cel.types.jackson.JacksonRegistry;

public class CelExecutor implements RuleExecutor {

  public static final String TYPE = "CEL";

  public static final String CEL_IGNORE_GUARD_SEPARATOR = "cel.ignore.guard.separator";

  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    // Register ProtobufModule to convert CEL objects (such as NullValue) to JSON
    mapper.registerModule(new ProtobufModule());
  }

  private static final int DEFAULT_CACHE_SIZE = 100;

  private final LoadingCache<RuleWithArgs, Script> cache;

  public CelExecutor() {
    cache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_SIZE)
        .build(new CacheLoader<RuleWithArgs, Script>() {
          @Override
          public Script load(RuleWithArgs ruleWithArgs) throws Exception {
            // Build the script factory
            ScriptHost.Builder scriptHostBuilder = ScriptHost.newBuilder();
            switch (ruleWithArgs.getType()) {
              case AVRO:
                scriptHostBuilder = scriptHostBuilder.registry(AvroRegistry.newRegistry());
                break;
              case JSON:
                scriptHostBuilder = scriptHostBuilder.registry(JacksonRegistry.newRegistry());
                break;
              case PROTOBUF:
                break;
              default:
                throw new IllegalArgumentException("Unsupported type " + ruleWithArgs.getType());
            }
            ScriptHost scriptHost = scriptHostBuilder.build();

            ScriptBuilder scriptBuilder = scriptHost
                .buildScript(ruleWithArgs.getRule())
                .withDeclarations(new ArrayList<>(ruleWithArgs.getDecls().values()));
            switch (ruleWithArgs.getType()) {
              case AVRO:
                // Register our Avro type
                scriptBuilder = scriptBuilder.withTypes(ruleWithArgs.getAvroSchema());
                break;
              case JSON:
                // Register our Jackson object message type
                scriptBuilder = scriptBuilder.withTypes(ruleWithArgs.getJsonClass());
                break;
              case PROTOBUF:
                // Use buildPartial to ignore missing required fields
                scriptBuilder = scriptBuilder.withTypes(
                    DynamicMessage.newBuilder(ruleWithArgs.getProtobufDesc()).buildPartial());
                break;
              default:
                throw new IllegalArgumentException("Unsupported type " + ruleWithArgs.getType());
            }
            scriptBuilder = scriptBuilder.withLibraries(new StringsLib(), new BuiltinLibrary());
            return scriptBuilder.build();
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
    Object result = execute(ctx, input, ImmutableMap.of("message", input));
    if (result instanceof Map) {
      // Convert maps to the target object type
      try {
        JsonNode jsonNode = mapper.valueToTree(result);
        result = ctx.target().fromJson(jsonNode);
      } catch (IOException e) {
        throw new RuleException(e);
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
            guardResult = execute(guard, obj, args);
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
    return execute(expr, obj, args);
  }

  private Object execute(String rule, Object obj, Map<String, Object> args)
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

      Map<String, Decl> decls = toDecls(args);
      RuleWithArgs ruleWithArgs = null;
      switch (type) {
        case AVRO:
          ruleWithArgs = new RuleWithArgs(rule, type, decls, ((GenericContainer) msg).getSchema());
          break;
        case JSON:
          ruleWithArgs = new RuleWithArgs(rule, type, decls, msg.getClass());
          break;
        case PROTOBUF:
          ruleWithArgs = new RuleWithArgs(rule, type, decls,
              ((Message) msg).getDescriptorForType());
          break;
        default:
          throw new IllegalArgumentException("Unsupported type " + type);
      }
      Script script = cache.get(ruleWithArgs);

      return script.execute(Object.class, args);
    } catch (ScriptException e) {
      throw new RuleException("Could not execute CEL script", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuleException) {
        throw (RuleException) e.getCause();
      } else {
        throw new RuleException("Could not get expression", e.getCause());
      }
    }
  }

  private static Map<String, Decl> toDecls(Map<String, Object> args) {
    return args.entrySet().stream()
        .map(e -> Decls.newVar(e.getKey(), findType(e.getValue())))
        .collect(Collectors.toMap(Decl::getName, e -> e));
  }

  private static Type findType(Object arg) {
    if (arg == null) {
      return Checked.checkedNull;
    } else if (arg instanceof GenericContainer) {
      return findTypeForAvroType(((GenericContainer) arg).getSchema());
    } else if (arg instanceof Message) {
      return Decls.newObjectType(((Message) arg).getDescriptorForType().getFullName());
    } else {
      return findTypeForClass(arg.getClass());
    }
  }

  private static Type findTypeForAvroType(Schema schema) {
    Schema.Type type = schema.getType();
    switch (type) {
      case BOOLEAN:
        return Checked.checkedBool;
      case INT:
      case LONG:
        return Checked.checkedInt;
      case BYTES:
      case FIXED:
        return Checked.checkedBytes;
      case FLOAT:
      case DOUBLE:
        return Checked.checkedDouble;
      case STRING:
        return Checked.checkedString;
      // TODO duration, timestamp
      case ARRAY:
        return Checked.checkedListDyn;
      case MAP:
        return Checked.checkedMapStringDyn;
      case ENUM:
        return Decls.newObjectType(schema.getFullName());
      case NULL:
        return Checked.checkedNull;
      case RECORD:
        return Decls.newObjectType(schema.getFullName());
      case UNION:
        if (schema.getTypes().size() == 2 && schema.getTypes().contains(NULL_AVRO_SCHEMA)) {
          for (Schema memberSchema : schema.getTypes()) {
            if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
              return findTypeForAvroType(memberSchema);
            }
          }
        }
        throw new IllegalArgumentException("Unsupported union type");
      default:
        throw new IllegalArgumentException("Unsupported type " + type);
    }
  }

  private static Type findTypeForClass(Class<?> type) {
    Class<?> rawClass = type;
    if (rawClass == boolean.class || rawClass == Boolean.class) {
      return Checked.checkedBool;
    } else if (rawClass == long.class
        || rawClass == Long.class
        || rawClass == int.class
        || rawClass == Integer.class
        || rawClass == short.class
        || rawClass == Short.class
        || rawClass == byte.class
        || rawClass == Byte.class) {
      return Checked.checkedInt;
    } else if (rawClass == byte[].class || rawClass == ByteString.class) {
      return Checked.checkedBytes;
    } else if (rawClass == double.class
        || rawClass == Double.class
        || rawClass == float.class
        || rawClass == Float.class) {
      return Checked.checkedDouble;
    } else if (rawClass == String.class) {
      return Checked.checkedString;
    } else if (rawClass == Duration.class || rawClass == java.time.Duration.class) {
      return Checked.checkedDuration;
    } else if (rawClass == Timestamp.class
        || Instant.class.isAssignableFrom(rawClass)
        || ZonedDateTime.class.isAssignableFrom(rawClass)) {
      return Checked.checkedTimestamp;
    } else if (Map.class.isAssignableFrom(rawClass)) {
      return Checked.checkedMapStringDyn;
    } else if (List.class.isAssignableFrom(rawClass)) {
      return Checked.checkedListDyn;
    } else {
      return Decls.newObjectType(rawClass.getName());
    }
  }

  enum ScriptType {
    AVRO,
    JSON,
    PROTOBUF
  }

  static class RuleWithArgs {
    private final String rule;
    private final ScriptType type;
    private final Map<String, Decl> decls;
    private Schema avroSchema;
    private Class<?> jsonClass;
    private Descriptor protobufDesc;

    public RuleWithArgs(String rule, ScriptType type, Map<String, Decl> decls, Schema avroSchema) {
      this.rule = rule;
      this.type = type;
      this.decls = decls;
      this.avroSchema = avroSchema;
    }

    public RuleWithArgs(String rule, ScriptType type, Map<String, Decl> decls, Class<?> jsonClass) {
      this.rule = rule;
      this.type = type;
      this.decls = decls;
      this.jsonClass = jsonClass;
    }

    public RuleWithArgs(String rule, ScriptType type, Map<String, Decl> decls, Descriptor protobufDesc) {
      this.rule = rule;
      this.type = type;
      this.decls = decls;
      this.protobufDesc = protobufDesc;
    }

    public String getRule() {
      return rule;
    }

    public ScriptType getType() {
      return type;
    }

    public Map<String, Decl> getDecls() {
      return decls;
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
          && Objects.equals(decls, that.decls)
          && Objects.equals(avroSchema, that.avroSchema)
          && Objects.equals(jsonClass, that.jsonClass)
          && Objects.equals(protobufDesc, that.protobufDesc);
    }

    @Override
    public int hashCode() {
      return Objects.hash(rule, type, decls, avroSchema, jsonClass, protobufDesc);
    }
  }
}
