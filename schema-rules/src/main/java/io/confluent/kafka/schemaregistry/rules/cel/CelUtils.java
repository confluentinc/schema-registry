/*
 * Copyright 2026 Confluent Inc.
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

import com.google.api.expr.v1alpha1.Decl;
import com.google.api.expr.v1alpha1.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.rules.cel.avro.AvroRegistry;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.BuiltinLibrary;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
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

/**
 * Shared CEL helpers used by both the transform-path executor ({@link CelExecutor}) and
 * the validation-path executor ({@code CelValidator} in schema-serializer). All methods
 * are stateless. Lives in schema-rules so it can reference {@link AvroRegistry} and
 * {@link BuiltinLibrary} without forcing those into a new module.
 */
public final class CelUtils {

  private CelUtils() {
  }

  public enum ScriptType {
    AVRO,
    JSON,
    PROTOBUF
  }

  /**
   * Build a CEL {@link Script} for {@code (type, expr, schemaHint, decls)}. Selects the
   * registry, applies declarations, registers the schema-hint type, and binds the
   * standard libraries.
   */
  public static Script buildScript(
      ScriptType type, String expr, Object schemaHint, List<Decl> decls)
      throws ScriptException {
    ScriptHost.Builder scriptHostBuilder = ScriptHost.newBuilder();
    switch (type) {
      case AVRO:
        scriptHostBuilder = scriptHostBuilder.registry(AvroRegistry.newRegistry());
        break;
      case JSON:
        scriptHostBuilder = scriptHostBuilder.registry(JacksonRegistry.newRegistry());
        break;
      case PROTOBUF:
        break;
      default:
        throw new IllegalArgumentException("Unsupported type " + type);
    }
    ScriptHost scriptHost = scriptHostBuilder.build();

    ScriptBuilder scriptBuilder = scriptHost
        .buildScript(expr)
        .withDeclarations(decls);
    switch (type) {
      case AVRO:
        scriptBuilder = scriptBuilder.withTypes((Schema) schemaHint);
        break;
      case JSON:
        scriptBuilder = scriptBuilder.withTypes((Class<?>) schemaHint);
        break;
      case PROTOBUF:
        // Use buildPartial to ignore missing required fields
        scriptBuilder = scriptBuilder.withTypes(
            DynamicMessage.newBuilder((Descriptor) schemaHint).buildPartial());
        break;
      default:
        throw new IllegalArgumentException("Unsupported type " + type);
    }
    return scriptBuilder.withLibraries(new StringsLib(), new BuiltinLibrary()).build();
  }

  public static Map<String, Type> toDeclTypes(Map<String, Object> args) {
    return args.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> findType(e.getValue())));
  }

  public static List<Decl> toDecls(Map<String, Type> args) {
    return args.entrySet().stream()
        .map(e -> Decls.newVar(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }

  public static Type findType(Object arg) {
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

  public static Type findTypeForAvroType(Schema schema) {
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

  public static Type findTypeForClass(Class<?> type) {
    if (type == boolean.class || type == Boolean.class) {
      return Checked.checkedBool;
    } else if (type == long.class
        || type == Long.class
        || type == int.class
        || type == Integer.class
        || type == short.class
        || type == Short.class
        || type == byte.class
        || type == Byte.class) {
      return Checked.checkedInt;
    } else if (type == byte[].class || type == ByteString.class) {
      return Checked.checkedBytes;
    } else if (type == double.class
        || type == Double.class
        || type == float.class
        || type == Float.class) {
      return Checked.checkedDouble;
    } else if (type == String.class) {
      return Checked.checkedString;
    } else if (type == Duration.class || type == java.time.Duration.class) {
      return Checked.checkedDuration;
    } else if (type == Timestamp.class
        || Instant.class.isAssignableFrom(type)
        || ZonedDateTime.class.isAssignableFrom(type)) {
      return Checked.checkedTimestamp;
    } else if (Map.class.isAssignableFrom(type)) {
      return Checked.checkedMapStringDyn;
    } else if (List.class.isAssignableFrom(type)) {
      return Checked.checkedListDyn;
    } else {
      return Decls.newObjectType(type.getName());
    }
  }
}
