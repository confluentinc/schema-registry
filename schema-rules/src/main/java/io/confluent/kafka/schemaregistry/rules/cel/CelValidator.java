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

import com.google.api.expr.v1alpha1.Type;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.apache.avro.generic.GenericContainer;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.ValidationRule;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleExecutor;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils.ScriptType;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;

/**
 * Validation-rule executor backed by CEL. Each instance owns its own script cache; the
 * cache key is {@code (expr, scriptType, thisType, schemaHint)} — narrow enough that the
 * same compiled {@link Script} is reused across records that share the same field shape.
 *
 * <p>Instantiated per serializer (lazily, the first time a validation rule fires) so that
 * deserializer-only flows pay nothing.
 */
public final class CelValidator implements ValidationRuleExecutor {

  private static final int DEFAULT_CACHE_SIZE = 1000;

  private final LoadingCache<ValidationKey, Script> cache;

  public CelValidator() {
    this(DEFAULT_CACHE_SIZE);
  }

  public CelValidator(int cacheSize) {
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .build(new CacheLoader<ValidationKey, Script>() {
          @Override
          public Script load(ValidationKey key) throws Exception {
            return CelUtils.buildScript(key.type, key.expr, key.schemaHint,
                Collections.singletonList(Decls.newVar("this", key.thisType)));
          }
        });
  }

  @Override
  public Object execute(ValidationRule rule, Object schema, Object value) throws RuleException {
    if (value == null) {
      // Walkers are expected to enforce skip-on-null before invoking the executor; a null
      // here means a non-compliant caller. Surface the contract violation explicitly
      // rather than NPE on value.getClass() or trip a confusing CEL evaluation error.
      throw new RuleException(
          "Validation rule '"
              + (rule.getName() == null ? "unnamed" : rule.getName())
              + "' received a null value; walkers must enforce skip-on-null before "
              + "invoking the executor.");
    }
    if (rule.getExpr() == null || rule.getExpr().isEmpty()) {
      throw new RuleException(
          "Validation rule '"
              + (rule.getName() == null ? "unnamed" : rule.getName())
              + "' has no expression");
    }
    ScriptType scriptType;
    Type thisType;
    if (schema instanceof Descriptor) {
      scriptType = ScriptType.PROTOBUF;
      // Walker passes the field's message-type descriptor for nested-message values and
      // the containing-type descriptor for primitive field values, so derive "this" type
      // from the value: a Message → its fully-qualified name; otherwise → the primitive's
      // Java class.
      if (value instanceof Message) {
        // The schema-side descriptor (parsed from the schema text) and the runtime
        // descriptor (from a generated class) are separate instances even when they
        // describe the same type — CEL's protobuf integration won't unify them. Use the
        // runtime descriptor for type registration so field access on `this` resolves.
        Descriptor valueDesc = ((Message) value).getDescriptorForType();
        thisType = Decls.newObjectType(valueDesc.getFullName());
        schema = valueDesc;
      } else {
        thisType = CelUtils.findTypeForClass(value.getClass());
      }
    } else if (schema instanceof Schema) {
      scriptType = ScriptType.AVRO;
      // Mirror the protobuf path: use the value's own schema for type registration so
      // CEL doesn't see distinct schema instances of the same logical type. Falls back
      // to the walker's hint for primitive field values (which aren't GenericContainers).
      if (value instanceof GenericContainer) {
        Schema valueSchema = ((GenericContainer) value).getSchema();
        thisType = CelUtils.findTypeForAvroType(valueSchema);
        schema = valueSchema;
      } else {
        thisType = CelUtils.findTypeForAvroType((Schema) schema);
      }
    } else if (schema instanceof Class<?>) {
      scriptType = ScriptType.JSON;
      thisType = CelUtils.findTypeForClass((Class<?>) schema);
    } else {
      throw new RuleException(
          "Unsupported schema type hint for validation rule '"
              + (rule.getName() == null ? "unnamed" : rule.getName()) + "': "
              + (schema == null ? "null" : schema.getClass()));
    }
    ValidationKey key = new ValidationKey(rule.getExpr(), scriptType, thisType, schema);
    try {
      Script script = cache.get(key);
      return script.execute(Object.class, Collections.singletonMap("this", value));
    } catch (ScriptException e) {
      throw new RuleException(
          "Could not execute validation rule '"
              + (rule.getName() == null ? "unnamed" : rule.getName())
              + "'"
              + (rule.getDoc() == null || rule.getDoc().isEmpty()
                  ? "" : " (" + rule.getDoc() + ")"), e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new RuleException(
          "Could not compile validation rule '"
              + (rule.getName() == null ? "unnamed" : rule.getName()) + "'", cause);
    }
  }

  /**
   * Cache key. Carries the resolved CEL type for {@code this} rather than a Map of
   * declarations — saves a Map allocation per call and keeps the key narrow.
   */
  private static final class ValidationKey {
    private final String expr;
    private final ScriptType type;
    private final Type thisType;
    private final Object schemaHint;

    ValidationKey(String expr, ScriptType type, Type thisType, Object schemaHint) {
      this.expr = expr;
      this.type = type;
      this.thisType = thisType;
      this.schemaHint = schemaHint;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ValidationKey)) {
        return false;
      }
      ValidationKey that = (ValidationKey) o;
      return type == that.type
          && Objects.equals(expr, that.expr)
          && Objects.equals(thisType, that.thisType)
          && Objects.equals(schemaHint, that.schemaHint);
    }

    @Override
    public int hashCode() {
      return Objects.hash(expr, type, thisType, schemaHint);
    }
  }
}
