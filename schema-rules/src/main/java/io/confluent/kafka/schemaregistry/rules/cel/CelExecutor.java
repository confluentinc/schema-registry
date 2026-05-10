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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import dev.cel.common.types.CelType;
import dev.cel.common.values.CelByteString;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils.ScriptType;
import io.confluent.kafka.schemaregistry.rules.cel.avro.AvroResultWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;

public class CelExecutor implements RuleExecutor {

  public static final String TYPE = "CEL";

  public static final String CEL_IGNORE_GUARD_SEPARATOR = "cel.ignore.guard.separator";

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
      .registerModule(new ProtobufModule());

  private static final int DEFAULT_CACHE_SIZE = 1000;

  private final LoadingCache<RuleWithArgs, CelRuntime.Program> cache;

  public CelExecutor() {
    cache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_SIZE)
        .build(new CacheLoader<RuleWithArgs, CelRuntime.Program>() {
          @Override
          public CelRuntime.Program load(RuleWithArgs ruleWithArgs) throws Exception {
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
            return CelUtils.buildProgram(ruleWithArgs.getType(), ruleWithArgs.getRule(),
                schemaHint, CelUtils.toVarDecls(ruleWithArgs.getDeclTypes()));
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
      // ObjectNode → Map; ArrayNode and scalar nodes can't satisfy a
      // Map<String, Object> target — let bind() see them as their natural
      // shape (List or scalar) so its dispatch can pass them through or
      // route to the right ScriptType.
      JsonNode node = (JsonNode) message;
      try {
        if (node.isObject()) {
          input = JSON_MAPPER.convertValue(node, new TypeReference<Map<String, Object>>(){});
        } else if (node.isArray()) {
          input = JSON_MAPPER.convertValue(node, new TypeReference<List<Object>>(){});
        } else {
          input = JSON_MAPPER.convertValue(node, Object.class);
        }
      } catch (IllegalArgumentException e) {
        throw new RuleException(ctx.rule(), e);
      }
    } else {
      input = message;
    }
    // Bind once so we have the converted message in scope for the identity
    // short-circuit below. evaluateWithGuard then handles guard parsing and
    // runs the body against the same bindings (no rebinding between guard and
    // body — they share type, decl-types and celArgs, only the cache key
    // differs).
    Bindings b = bind(ctx, input, Collections.singletonMap("message", input));
    if (b == null) {
      return message;
    }
    Object result = evaluateWithGuard(ctx, b);
    // Identity short-circuit: return the original `message` reference if
    // either (a) the rule returned the same value we bound (e.g., expression
    // is just `message`) or (b) evaluateWithGuard returned `b.obj()` because
    // a guard skipped the body. Both mean "no transformation happened" — the
    // caller should see byte-for-byte what they passed in. Without (b),
    // JsonNode inputs with guard-skipped TRANSFORM rules would get a
    // Jackson-converted Map back instead of their JsonNode.
    if (result == b.boundMessage() || result == b.obj()) {
      return message;
    }
    // CONDITION rules return Boolean as a framework signal (pass/fail), not
    // data to coerce against the target schema. Pass through unchanged — the
    // framework's Boolean.TRUE.equals check treats anything-not-TRUE as fail,
    // so sentinel types (NullValue, CelByteString) just surface as failures.
    if (ctx.rule().getKind() == RuleKind.CONDITION) {
      return result;
    }
    if (ctx.target() instanceof AvroSchema) {
      // Avro target: walker handles every Schema.Type — records, primitives,
      // enums, bytes/fixed, logical types, unions. Sidesteps the JSON
      // intermediate that loses bytes (Jackson's base64 default), enum
      // symbols, and fixed types. Scalars get the same coercion/validation
      // (e.g., CEL's widened Long narrowed to int, CelByteString unwrapped to
      // ByteBuffer, String wrapped as EnumSymbol) as nested record fields.
      try {
        Schema avroSchema = ((AvroSchema) ctx.target()).rawSchema();
        return AvroResultWriter.convert(result, avroSchema);
      } catch (RuntimeException e) {
        throw new RuleException(ctx.rule(), e);
      }
    }
    if (result instanceof Map) {
      // JSON Schema / Protobuf: Jackson roundtrip. Unwrap CEL-flavored values
      // first (NullValue → null, CelByteString → byte[]) so ProtobufModule
      // doesn't emit "zeroValue" and downstream parsers accept the JsonNode.
      Object converted = unwrapCelValuesForJson(result);
      try {
        JsonNode jsonNode = JSON_MAPPER.valueToTree(converted);
        return ctx.target().fromJson(jsonNode);
      } catch (IOException e) {
        throw new RuleException(ctx.rule(), e);
      }
    }
    // Scalar fall-through for JSON Schema / Protobuf: normalize CEL sentinels
    // so downstream serializers see Java null and byte[] rather than NullValue
    // / CelByteString.
    return unwrapCelValuesForJson(result);
  }

  /**
   * Recursively normalize a CEL eval result so Jackson can serialize it back
   * into JSON that downstream Avro / JSON-Schema parsers will accept:
   * <ul>
   *   <li>{@link com.google.protobuf.NullValue} → Java {@code null} (otherwise
   *       Jackson's ProtobufModule emits {@code "zeroValue"})</li>
   *   <li>{@link dev.cel.common.values.CelByteString} → {@code byte[]}</li>
   * </ul>
   * Maps and lists are walked recursively; other values pass through.
   */
  private static Object unwrapCelValuesForJson(Object value) {
    if (value == null
        || value instanceof NullValue
        || value instanceof dev.cel.common.values.NullValue) {
      return null;
    }
    if (value instanceof CelByteString) {
      return ((CelByteString) value).toByteArray();
    }
    if (value instanceof Map) {
      Map<?, ?> in = (Map<?, ?>) value;
      Map<String, Object> out = new LinkedHashMap<>(in.size());
      for (Map.Entry<?, ?> e : in.entrySet()) {
        out.put(String.valueOf(e.getKey()), unwrapCelValuesForJson(e.getValue()));
      }
      return out;
    }
    if (value instanceof List) {
      List<?> in = (List<?>) value;
      List<Object> out = new ArrayList<>(in.size());
      for (Object e : in) {
        out.add(unwrapCelValuesForJson(e));
      }
      return out;
    }
    return value;
  }

  /**
   * Entry point used by {@link CelFieldExecutor} and any subclass that wants
   * the full guard-then-body flow. Equivalent to
   * {@code bind} → {@code evaluateWithGuard}.
   */
  protected Object execute(
      RuleContext ctx, Object obj, Map<String, Object> args)
      throws RuleException {
    Bindings b = bind(ctx, obj, args);
    if (b == null) {
      return obj;          // list-shaped message — not supported, pass through.
    }
    return evaluateWithGuard(ctx, b);
  }

  /**
   * Inspect the bound {@code message} to decide ScriptType, then convert every
   * arg to its CEL-friendly shape. The returned {@link Bindings} is reusable
   * across multiple {@code evaluate} calls (e.g., guard then body) — only the
   * rule expression changes between them. Returns {@code null} if the message
   * is a List (not a supported CEL binding shape).
   */
  protected Bindings bind(RuleContext ctx, Object obj, Map<String, Object> args)
      throws RuleException {
    Object msg = args.get("message");
    if (msg == null) {
      msg = obj;
    }
    if (msg == null) {
      return null;
    }
    ScriptType type;
    Object schemaHint;
    if (msg instanceof GenericContainer) {
      type = ScriptType.AVRO;
      schemaHint = ((GenericContainer) msg).getSchema();
    } else if (msg instanceof Message) {
      type = ScriptType.PROTOBUF;
      schemaHint = ((Message) msg).getDescriptorForType();
    } else if (msg instanceof List<?>) {
      return null;
    } else {
      type = ScriptType.JSON;
      schemaHint = msg.getClass();
    }

    Map<String, CelType> declTypes = CelUtils.toDeclTypes(args);
    // Convert each variable value to a CEL-compatible shape (Avro records →
    // maps, narrow numerics widened, JSON POJOs → maps via Jackson, etc.).
    // Proto Messages pass through unchanged — Google cel-java's first-class
    // proto support handles them natively.
    Map<String, Object> celArgs = new HashMap<>(args.size());
    for (Map.Entry<String, Object> e : args.entrySet()) {
      Object converted = type == ScriptType.JSON
          ? CelUtils.toCelValueForJson(e.getValue(), JSON_MAPPER)
          : CelUtils.toCelValue(e.getValue());
      celArgs.put(e.getKey(), converted);
    }
    return new Bindings(type, schemaHint, declTypes, celArgs, obj);
  }

  /**
   * Apply the optional guard (everything before the first {@code ;} in the
   * rule expression) and then evaluate the body. Reuses {@code b} for both —
   * only the rule expression (and therefore the cache key) differs.
   */
  protected Object evaluateWithGuard(RuleContext ctx, Bindings b) throws RuleException {
    String expr = ctx.rule().getExpr();
    String ignoreGuardStr = ctx.getParameter(CEL_IGNORE_GUARD_SEPARATOR);
    boolean ignoreGuard = Boolean.parseBoolean(ignoreGuardStr);
    if (!ignoreGuard) {
      int index = expr.indexOf(';');
      if (index >= 0) {
        String guard = expr.substring(0, index);
        if (!guard.trim().isEmpty()) {
          Object guardResult = Boolean.FALSE;
          try {
            guardResult = evaluate(ctx, guard, b);
          } catch (RuleException e) {
            // ignore — exception in guard is treated as false (skip the body).
          }
          if (Boolean.FALSE.equals(guardResult)) {
            return ctx.rule().getKind() == RuleKind.CONDITION ? Boolean.TRUE : b.obj();
          }
        }
        expr = expr.substring(index + 1);
      }
    }
    return evaluate(ctx, expr, b);
  }

  /**
   * Look up (or compile) the program for {@code rule} and run it against the
   * pre-converted args in {@code b}.
   */
  protected Object evaluate(RuleContext ctx, String rule, Bindings b) throws RuleException {
    try {
      CelRuntime.Program program = cache.get(b.cacheKey(rule));
      return program.eval(b.celArgs());
    } catch (CelEvaluationException e) {
      throw new RuleException(ctx.rule(), "Could not execute CEL script", e);
    } catch (ExecutionException e) {
      // Guava cache wraps anything thrown by load() — including CelValidationException
      // from the compiler — as ExecutionException. Unwrap and re-throw.
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

  /**
   * Pre-converted CEL invocation context. Holds the {@link ScriptType}, a
   * format-specific schema hint, the {@code declTypes} declarations, and the
   * already-CEL-shaped {@code celArgs}. Reused across multiple {@code evaluate}
   * calls within a single {@code transform}/{@code execute} so guard and body
   * share the same conversions.
   */
  protected static final class Bindings {
    private final ScriptType type;
    private final Object schemaHint;
    private final Map<String, CelType> declTypes;
    private final Map<String, Object> celArgs;
    private final Object obj;

    Bindings(ScriptType type, Object schemaHint, Map<String, CelType> declTypes,
             Map<String, Object> celArgs, Object obj) {
      this.type = type;
      this.schemaHint = schemaHint;
      this.declTypes = declTypes;
      this.celArgs = celArgs;
      this.obj = obj;
    }

    public ScriptType type() {
      return type;
    }

    /**
     * Read-only view of the converted CEL args. Wrapped to prevent subclasses
     * from mutating between {@code bind} and {@code evaluate} (which would
     * break the cache key invariants and the identity short-circuit). The
     * wrapper preserves reference semantics on values, so
     * {@link #boundMessage()} still compares equal to the result of an
     * identity-rule evaluation.
     */
    public Map<String, Object> celArgs() {
      return Collections.unmodifiableMap(celArgs);
    }

    public Object obj() {
      return obj;
    }

    /**
     * The value bound for the {@code message} variable, used by the identity
     * short-circuit in {@link CelExecutor#transform}.
     */
    public Object boundMessage() {
      return celArgs.get("message");
    }

    RuleWithArgs cacheKey(String rule) {
      switch (type) {
        case AVRO:
          return new RuleWithArgs(rule, type, declTypes, (Schema) schemaHint);
        case JSON:
          return new RuleWithArgs(rule, type, declTypes, (Class<?>) schemaHint);
        case PROTOBUF:
          return new RuleWithArgs(rule, type, declTypes, (Descriptor) schemaHint);
        default:
          throw new IllegalArgumentException("Unsupported type " + type);
      }
    }
  }

  static class RuleWithArgs {
    private final String rule;
    private final ScriptType type;
    private final Map<String, CelType> declTypes;
    private Schema avroSchema;
    private Class<?> jsonClass;
    private Descriptor protobufDesc;

    public RuleWithArgs(
        String rule, ScriptType type, Map<String, CelType> declTypes, Schema avroSchema) {
      this.rule = rule;
      this.type = type;
      this.declTypes = declTypes;
      this.avroSchema = avroSchema;
    }

    public RuleWithArgs(
        String rule, ScriptType type, Map<String, CelType> declTypes, Class<?> jsonClass) {
      this.rule = rule;
      this.type = type;
      this.declTypes = declTypes;
      this.jsonClass = jsonClass;
    }

    public RuleWithArgs(
        String rule, ScriptType type, Map<String, CelType> declTypes, Descriptor protobufDesc) {
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

    public Map<String, CelType> getDeclTypes() {
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
