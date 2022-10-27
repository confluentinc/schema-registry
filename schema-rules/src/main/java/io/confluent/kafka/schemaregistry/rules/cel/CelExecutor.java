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

import com.google.api.expr.v1alpha1.Decl;
import com.google.api.expr.v1alpha1.Type;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import io.confluent.kafka.schemaregistry.rules.cel.avro.AvroRegistry;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.pb.Checked;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.tools.ScriptHost.ScriptBuilder;
import org.projectnessie.cel.types.jackson.JacksonRegistry;

public class CelExecutor implements RuleExecutor {

  public static final String TYPE = "CEL";

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Object transform(RuleContext ctx, Object message)
      throws RuleException {
    Object result = execute(ctx, message, ImmutableMap.of("message", message));
    if (ctx.rule().getKind() == RuleKind.CONSTRAINT) {
      return Boolean.TRUE.equals(result) ? message : null;
    } else {
      return message;
    }
  }

  protected Object execute(
      RuleContext ctx, Object obj, Map<String, Object> args)
      throws RuleException {
    return execute(ctx.rule().getBody(), obj, args);
  }

  private Object execute(String rule, Object obj, Map<String, Object> args)
      throws RuleException {
    try {
      boolean isAvro = false;
      boolean isProto = false;
      if (obj instanceof GenericContainer) {
        isAvro = true;
      } else if (obj instanceof Message) {
        isProto = true;
      } else if (obj instanceof List<?>) {
        // list not supported
        return obj;
      } else if (obj instanceof Map<?, ?>) {
        // map not supported
        return obj;
      }

      // Build the script factory
      ScriptHost.Builder scriptHostBuilder = ScriptHost.newBuilder();
      if (isAvro) {
        scriptHostBuilder = scriptHostBuilder.registry(AvroRegistry.newRegistry());
      } else if (!isProto) {
        scriptHostBuilder = scriptHostBuilder.registry(JacksonRegistry.newRegistry());
      }
      ScriptHost scriptHost = scriptHostBuilder.build();

      ScriptBuilder scriptBuilder = scriptHost.buildScript(rule).withDeclarations(toDecls(args));
      if (isAvro) {
        // Register our Avro type
        scriptBuilder = scriptBuilder.withTypes(obj);
      } else if (!isProto) {
        // Register our Jackson object message type
        scriptBuilder = scriptBuilder.withTypes(obj.getClass());
      }
      Script script = scriptBuilder.build();

      return script.execute(Object.class, args);
    } catch (ScriptException e) {
      throw new RuleException("Could not execute CEL script", e);
    }
  }

  private static Decl[] toDecls(Map<String, Object> args) {
    return args.entrySet().stream()
        .map(e -> Decls.newVar(e.getKey(), findType(e.getValue())))
        .toArray(Decl[]::new);
  }

  private static Type findType(Object arg) {
    if (arg instanceof GenericContainer) {
      return findTypeForAvroType(((GenericContainer) arg).getSchema());
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
      case NULL:
        return Checked.checkedNull;
      default:
        return Decls.newObjectType(schema.getFullName());
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
}
