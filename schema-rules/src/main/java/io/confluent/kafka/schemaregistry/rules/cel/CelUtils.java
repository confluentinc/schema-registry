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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelOptions;
import dev.cel.common.CelValidationException;
import dev.cel.common.CelVarDecl;
import dev.cel.common.types.CelKind;
import dev.cel.common.types.CelType;
import dev.cel.common.types.ListType;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructTypeReference;
import dev.cel.common.values.CelByteString;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerBuilder;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.extensions.CelExtensions;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelFunctionBinding;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeBuilder;
import dev.cel.runtime.CelRuntimeFactory;
import dev.cel.runtime.CelStandardFunctions;
import dev.cel.runtime.CelStandardFunctions.StandardFunction;
import io.confluent.kafka.schemaregistry.rules.cel.avro.AvroCelTypeProvider;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.BuiltinLibrary;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

/**
 * Shared CEL helpers used by both the transform-path executor ({@link CelExecutor}) and
 * the validation-path executor ({@link CelValidator}). All methods are stateless.
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
   * Regex engine used by the CEL {@code matches} / {@code matches_string}
   * overloads. Selectable per-rule (via {@code params.cel.regex.engine}) or
   * globally (via the executor config). The unconfigured default is
   * {@link #DEFAULT}.
   */
  public enum RegexEngine {
    PCRE,
    RE2;

    /**
     * The default engine when none is configured. Change this single value to
     * flip the default for {@link CelExecutor} and the no-engine
     * {@link CelUtils#buildProgram} overload. Does <em>not</em> affect
     * {@link CelValidator}, which always uses {@link #RE2} for ReDoS safety
     * on the hot serialize path.
     */
    public static final RegexEngine DEFAULT = PCRE;

    public static RegexEngine fromString(String s) {
      if (s == null || s.isEmpty()) {
        return DEFAULT;
      }
      try {
        return RegexEngine.valueOf(s.trim().toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Unknown cel.regex.engine value: " + s + " (expected 'pcre' or 're2')");
      }
    }
  }

  private static final int PCRE_CACHE_SIZE = 1000;
  private static final LoadingCache<String, Pattern> PCRE_CACHE = CacheBuilder.newBuilder()
      .maximumSize(PCRE_CACHE_SIZE)
      .build(new CacheLoader<String, Pattern>() {
        @Override
        public Pattern load(String regex) {
          return Pattern.compile(regex);
        }
      });

  /**
   * PCRE-mode replacement for the stdlib {@code matches}/{@code matches_string}
   * overloads. Uses {@link java.util.regex.Pattern#matches()} (full-string
   * match) to mirror cel-java's RE2 default behavior.
   */
  private static boolean pcreMatches(String value, String regex) throws CelEvaluationException {
    Pattern pattern;
    try {
      pattern = PCRE_CACHE.get(regex);
    } catch (ExecutionException | UncheckedExecutionException e) {
      // Guava's LoadingCache wraps loader exceptions: checked → ExecutionException,
      // RuntimeException → UncheckedExecutionException. Pattern.compile()'s
      // PatternSyntaxException is a RuntimeException, so it always arrives here as
      // UncheckedExecutionException; ExecutionException is just defensive (the
      // loader doesn't currently throw any checked exceptions). Either way, unwrap
      // to keep the user-facing message focused on the regex problem itself.
      Throwable cause = e.getCause();
      throw new CelEvaluationException(
          "failed to compile regex: " + (cause != null ? cause.getMessage() : e.getMessage()));
    }
    return pattern.matcher(value).matches();
  }

  /**
   * Build a CEL {@link CelRuntime.Program} for {@code (type, expr, schemaHint, varDecls)}.
   * The compiler is configured with the standard library, our {@link BuiltinLibrary},
   * and any format-specific type registration; the runtime mirrors the compile-side
   * configuration so resolved overload IDs find their bindings.
   *
   * <p>For AVRO and JSON values, callers should pre-convert native values
   * ({@link GenericRecord}, Jackson {@code JsonNode}, etc.) to CEL-native shapes
   * (Map / List / scalar) via {@link #toCelValue(Object)} before binding. Field
   * access ({@code this.field}, {@code this["field"]}) then works through CEL's
   * built-in map semantics — no custom type provider is required.
   */
  public static CelRuntime.Program buildProgram(
      ScriptType type, String expr, Object schemaHint, List<CelVarDecl> varDecls)
      throws CelValidationException, CelEvaluationException {
    return buildProgram(type, expr, schemaHint, varDecls, RegexEngine.DEFAULT);
  }

  public static CelRuntime.Program buildProgram(
      ScriptType type, String expr, Object schemaHint, List<CelVarDecl> varDecls,
      RegexEngine regexEngine)
      throws CelValidationException, CelEvaluationException {
    return buildCompiledRule(type, expr, schemaHint, varDecls, regexEngine).program;
  }

  /**
   * Like {@link #buildProgram} but exposes the compiled AST's result-type
   * {@link CelKind} alongside the runnable program. Lets callers (notably
   * {@link CelValidator}) reject rules whose result type doesn't fit their
   * contract, at compile time rather than first eval.
   */
  public static CompiledRule buildCompiledRule(
      ScriptType type, String expr, Object schemaHint, List<CelVarDecl> varDecls,
      RegexEngine regexEngine)
      throws CelValidationException, CelEvaluationException {
    // For AVRO records, first try declaring `this` as a struct (so field
    // accesses type-check against actual Avro field types). On compile
    // failure, fall back to declaring `this` as a dynamic map — handles edge
    // cases the type provider can't represent (complex unions, etc.) without
    // forcing every rule to use dyn.
    if (type == ScriptType.AVRO && schemaHint instanceof Schema
        && hasAnyStructTypedVar(varDecls)) {
      try {
        return doBuildProgram(type, expr, schemaHint, varDecls, regexEngine);
      } catch (CelValidationException firstAttempt) {
        // Retry with `this` (and any other StructTypeReference vars) downgraded
        // to MapType<STRING, DYN>.
        List<CelVarDecl> fallbackVarDecls = downgradeStructsToMap(varDecls);
        try {
          return doBuildProgram(type, expr, schemaHint, fallbackVarDecls, regexEngine);
        } catch (CelValidationException secondAttempt) {
          // Fallback also failed — surface the original error (more informative).
          throw firstAttempt;
        }
      }
    }
    return doBuildProgram(type, expr, schemaHint, varDecls, regexEngine);
  }

  /**
   * Compiled rule output: the runnable {@link CelRuntime.Program} plus the
   * compile-time result-type {@link CelKind}. Callers that need to validate
   * the result type (e.g., a validator that requires bool/string) get both
   * from one compile pass.
   */
  public static final class CompiledRule {
    public final CelRuntime.Program program;
    public final CelKind resultKind;

    CompiledRule(CelRuntime.Program program, CelKind resultKind) {
      this.program = program;
      this.resultKind = resultKind;
    }
  }

  private static CompiledRule doBuildProgram(
      ScriptType type, String expr, Object schemaHint, List<CelVarDecl> varDecls,
      RegexEngine regexEngine)
      throws CelValidationException, CelEvaluationException {
    // CelExtensions.strings() supplies charAt/indexOf/lastIndexOf/lowerAscii/
    // upperAscii/replace/split/substring/trim/join. The class implements both
    // CelCompilerLibrary and CelRuntimeLibrary, so the same instance binds to
    // both sides.
    // standardCelCompilerBuilder() omits comprehension macros (has/all/exists/
    // exists_one/map/filter); they have to be wired in explicitly. Without
    // them, expressions like `tags.exists_one(x, x == 'PII')` fail to compile.
    CelCompilerBuilder compilerBuilder = CelCompilerFactory.standardCelCompilerBuilder()
        .setOptions(CelOptions.DEFAULT)
        .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
        .addLibraries(new BuiltinLibrary(), CelExtensions.strings())
        .addVarDeclarations(varDecls);
    CelRuntimeBuilder runtimeBuilder = CelRuntimeFactory.standardCelRuntimeBuilder()
        .setOptions(CelOptions.DEFAULT)
        .addLibraries(new BuiltinLibrary(), CelExtensions.strings());

    if (regexEngine == RegexEngine.PCRE) {
      // Replace stdlib RE2-backed matches with java.util.regex. Despite what
      // CelRuntimeBuilder.addFunctionBindings's javadoc says about replacing
      // duplicate overload IDs, it actually throws on duplicates unless both
      // sides are DynamicDispatchOverload (see DefaultDispatcher.Builder).
      // Stdlib matches isn't dynamic-dispatch, so we subset the standard
      // functions to drop MATCHES, then add our own bindings under the same
      // overload IDs the compiler resolves to.
      // setStandardEnvironmentEnabled(false) is required by cel-java when
      // overriding standard function bindings (else build() throws).
      runtimeBuilder
          .setStandardEnvironmentEnabled(false)
          .setStandardFunctions(
              CelStandardFunctions.newBuilder()
                  .excludeFunctions(StandardFunction.MATCHES)
                  .build())
          .addFunctionBindings(
              CelFunctionBinding.from("matches", String.class, String.class,
                  CelUtils::pcreMatches),
              CelFunctionBinding.from("matches_string", String.class, String.class,
                  CelUtils::pcreMatches));
    }

    if (type == ScriptType.PROTOBUF) {
      Descriptor desc = (Descriptor) schemaHint;
      compilerBuilder.addMessageTypes(desc);
      runtimeBuilder.addMessageTypes(desc);
    } else if (type == ScriptType.AVRO && schemaHint instanceof Schema
        && hasAnyStructTypedVar(varDecls)) {
      // Register Avro records as CEL StructTypes so `this.field` type-checks
      // against actual Avro field types instead of dyn. Runtime values flow
      // through as plain Maps (toCelValue converts GenericRecord → Map);
      // Google cel-java's runtime selects fields on Maps natively, so no
      // matching CelValueProvider is needed.
      AvroCelTypeProvider avroTypes = AvroCelTypeProvider.forSchema((Schema) schemaHint);
      if (avroTypes.hasAnyType()) {
        compilerBuilder.setTypeProvider(avroTypes);
      }
    }
    // JSON: schemaHint reflected in varDecls (typically as MapType(STRING, DYN));
    // no extra registration needed.

    CelCompiler compiler = compilerBuilder.build();
    CelRuntime runtime = runtimeBuilder.build();
    CelAbstractSyntaxTree ast = compiler.compile(expr).getAst();
    return new CompiledRule(runtime.createProgram(ast), ast.getResultType().kind());
  }

  private static boolean hasAnyStructTypedVar(List<CelVarDecl> varDecls) {
    for (CelVarDecl decl : varDecls) {
      if (decl.type() instanceof StructTypeReference) {
        return true;
      }
    }
    return false;
  }

  private static List<CelVarDecl> downgradeStructsToMap(List<CelVarDecl> varDecls) {
    List<CelVarDecl> out = new java.util.ArrayList<>(varDecls.size());
    for (CelVarDecl decl : varDecls) {
      if (decl.type() instanceof StructTypeReference) {
        out.add(CelVarDecl.newVarDeclaration(
            decl.name(), MapType.create(SimpleType.STRING, SimpleType.DYN)));
      } else {
        out.add(decl);
      }
    }
    return out;
  }

  /**
   * Convenience wrapper to construct {@link CelVarDecl} list from a name→type
   * map, preserving insertion order.
   */
  public static List<CelVarDecl> toVarDecls(Map<String, CelType> args) {
    return args.entrySet().stream()
        .map(e -> CelVarDecl.newVarDeclaration(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }

  /**
   * Build a name→{@link CelType} map by inferring each value's type. Insertion order
   * is preserved (uses {@link LinkedHashMap}) so generated declarations match argument
   * binding order.
   */
  public static Map<String, CelType> toDeclTypes(Map<String, Object> args) {
    Map<String, CelType> out = new LinkedHashMap<>(args.size());
    for (Map.Entry<String, Object> e : args.entrySet()) {
      out.put(e.getKey(), findCelType(e.getValue()));
    }
    return out;
  }

  /**
   * Infer the {@link CelType} for a runtime value. Used to declare {@code this} (and
   * other variables) in the compiler so the type-checker can validate field access.
   */
  public static CelType findCelType(Object arg) {
    // Treat all null flavors (Java null, proto NullValue, dev.cel NullValue)
    // uniformly as DYN. NULL_TYPE would restrict the var to null-only
    // operations at compile time — `value != null ? value + 1 : 0` would
    // fail to compile because `+` has no NULL_TYPE overload, defeating the
    // user's null-handling intent. DYN lets the rule compile and dispatches
    // by actual value at runtime. Also collapses three cache entries (J →
    // NULL_TYPE, P → DYN, C → DYN) into a single DYN entry.
    if (arg == null
        || arg instanceof NullValue
        || arg instanceof dev.cel.common.values.NullValue) {
      return SimpleType.DYN;
    } else if (arg instanceof GenericContainer) {
      return findCelTypeForAvroSchema(((GenericContainer) arg).getSchema());
    } else if (arg instanceof Message) {
      return StructTypeReference.create(
          ((Message) arg).getDescriptorForType().getFullName());
    } else {
      return findCelTypeForClass(arg.getClass());
    }
  }

  /**
   * Map an Avro {@link Schema} to a {@link CelType}.
   *
   * <ul>
   *   <li>Primitives map to their natural CEL counterpart (BOOLEAN→BOOL,
   *       INT/LONG→INT, FLOAT/DOUBLE→DOUBLE, STRING/ENUM→STRING,
   *       BYTES/FIXED→BYTES, NULL→NULL_TYPE).</li>
   *   <li>{@code RECORD} → {@link StructTypeReference} so the
   *       {@link AvroCelTypeProvider} registered by
   *       {@link #buildProgram} resolves field types at type-check time.
   *       Runtime values still flow through as JDK maps (via
   *       {@link #toCelValue}); the struct declaration is purely for compile-side
   *       field validation. If the provider can't represent the record cleanly,
   *       {@link #buildProgram} retries with {@code this} downgraded to
   *       {@code map<string, dyn>}.</li>
   *   <li>{@code ARRAY} → {@code list<dyn>}; {@code MAP} → {@code map<string, dyn>}.</li>
   *   <li>{@code UNION}: nullable {@code [null, X]} unwraps to X; multi-branch
   *       unions (and any unrecognized type) fall back to {@code dyn}.</li>
   * </ul>
   */
  public static CelType findCelTypeForAvroSchema(Schema schema) {
    // Logical-typed fields (timestamp-millis on long, decimal on bytes/fixed,
    // date on int, uuid on string, etc.) carry runtime values whose Java type
    // diverges from the underlying Avro primitive — Instant / BigDecimal /
    // LocalDate / UUID. Declaring the underlying CEL type (INT / BYTES /
    // STRING) creates a compile/runtime mismatch. Use DYN so the runtime can
    // dispatch against whatever the value actually is, mirroring how
    // AvroResultWriter bypasses primitive narrowing for logical types.
    if (schema.getLogicalType() != null) {
      return SimpleType.DYN;
    }
    Schema.Type type = schema.getType();
    switch (type) {
      case BOOLEAN:
        return SimpleType.BOOL;
      case INT:
      case LONG:
        return SimpleType.INT;
      case BYTES:
      case FIXED:
        return SimpleType.BYTES;
      case FLOAT:
      case DOUBLE:
        return SimpleType.DOUBLE;
      case STRING:
        return SimpleType.STRING;
      case ARRAY:
        return ListType.create(SimpleType.DYN);
      case MAP:
        return MapType.create(SimpleType.STRING, SimpleType.DYN);
      case ENUM:
        // Pre-converted to string via toCelValue; report as string for type-checking.
        return SimpleType.STRING;
      case NULL:
        return SimpleType.NULL_TYPE;
      case RECORD:
        // Declare as a struct reference so the AvroCelTypeProvider
        // (registered in buildProgram) resolves field types. If the type
        // provider can't represent this record cleanly, the type-checker will
        // fail and the caller falls back to MapType<STRING, DYN>.
        return StructTypeReference.create(schema.getFullName());
      case UNION:
        if (schema.getTypes().size() == 2 && containsNull(schema.getTypes())) {
          for (Schema memberSchema : schema.getTypes()) {
            if (memberSchema.getType() != Schema.Type.NULL) {
              return findCelTypeForAvroSchema(memberSchema);
            }
          }
        }
        // Multi-branch unions: fall back to dyn (matches AvroCelTypeProvider's
        // field-level handling). Runtime values flow through unchanged and
        // CEL's dyn dispatch handles primitives + field access correctly.
        return SimpleType.DYN;
      default:
        // Defensive: future Avro types added without updating this switch.
        return SimpleType.DYN;
    }
  }

  private static boolean containsNull(List<Schema> schemas) {
    for (Schema s : schemas) {
      if (s.getType() == Schema.Type.NULL) {
        return true;
      }
    }
    return false;
  }

  /**
   * Map a Java {@link Class} to a {@link CelType}. Used when the value isn't a
   * GenericContainer or proto Message (i.e., a JSON-derived primitive value or a
   * primitive Avro/Proto field bound to {@code this}).
   */
  public static CelType findCelTypeForClass(Class<?> type) {
    if (type == boolean.class || type == Boolean.class) {
      return SimpleType.BOOL;
    } else if (type == long.class
        || type == Long.class
        || type == int.class
        || type == Integer.class
        || type == short.class
        || type == Short.class
        || type == byte.class
        || type == Byte.class) {
      return SimpleType.INT;
    } else if (type == byte[].class
        || type == ByteString.class
        || ByteBuffer.class.isAssignableFrom(type)) {
      return SimpleType.BYTES;
    } else if (type == double.class
        || type == Double.class
        || type == float.class
        || type == Float.class) {
      return SimpleType.DOUBLE;
    } else if (type == String.class || CharSequence.class.isAssignableFrom(type)) {
      // Covers Utf8 (Avro's string representation in some configurations) and
      // any other CharSequence implementation. toCelValue normalizes Utf8 to
      // String at runtime, so the declaration matches the bound value.
      return SimpleType.STRING;
    } else if (type == Duration.class || type == java.time.Duration.class) {
      return SimpleType.DURATION;
    } else if (type == Timestamp.class
        || Instant.class.isAssignableFrom(type)
        || ZonedDateTime.class.isAssignableFrom(type)
        || java.time.OffsetDateTime.class.isAssignableFrom(type)
        || java.time.LocalDateTime.class.isAssignableFrom(type)) {
      // Single-instant types map to TIMESTAMP. LocalDate / LocalTime are
      // partial temporals (no time-zone or no date) — they fall through to
      // DYN below since CEL's TIMESTAMP type implies a full instant.
      return SimpleType.TIMESTAMP;
    } else if (Map.class.isAssignableFrom(type)) {
      return MapType.create(SimpleType.STRING, SimpleType.DYN);
    } else if (List.class.isAssignableFrom(type)) {
      return ListType.create(SimpleType.DYN);
    } else {
      // Unknown class — covers logical-type Java reps (BigDecimal, UUID,
      // LocalDate, LocalTime) and arbitrary POJO beans alike. Declare DYN so
      // CEL's runtime dispatches against whatever the value actually is. POJO
      // beans get Jackson-converted to Map at runtime; CEL's DYN allows
      // .field access on the resulting Map. Declaring MapType<STRING, DYN>
      // would be wrong for non-Map values (BigDecimal etc.) — comparison and
      // arithmetic ops wouldn't type-check against a Map type.
      return SimpleType.DYN;
    }
  }

  /**
   * Convert a native value to a CEL-compatible shape:
   * <ul>
   *   <li>{@link IndexedRecord} (covers both {@link GenericRecord} and
   *       {@code SpecificRecord}) → JDK {@link Map} (Google cel-java's
   *       runtime selects fields on Maps natively)</li>
   *   <li>{@link Utf8} / {@link GenericEnumSymbol} → {@link String}</li>
   *   <li>Narrow numeric types ({@link Integer}, {@link Short}, {@link Byte},
   *       {@link Float}) widened to CEL's natural widths
   *       ({@code long} / {@code double})</li>
   *   <li>{@code byte[]} / {@link ByteString} → {@link CelByteString}
   *       (CEL bytes literals only compare equal to other CelByteStrings)</li>
   *   <li>Lists/maps recursively converted</li>
   * </ul>
   * Other values (proto Messages, BigDecimal, primitives at correct width, etc.)
   * pass through unchanged.
   */
  public static Object toCelValue(Object value) {
    // Normalize all null flavors (Java null, proto NullValue) to CEL's own
    // NullValue. Google cel-java's runtime treats Java null as "unknown
    // attribute" (references surface as CelUnknownSet) and doesn't
    // recognize proto NullValue as CEL null either. Converting to dev.cel
    // NullValue makes `value == null` dispatch correctly regardless of
    // which null flavor the caller bound.
    if (value == null || value instanceof NullValue) {
      return dev.cel.common.values.NullValue.NULL_VALUE;
    }
    // IndexedRecord covers both GenericRecord and SpecificRecord (Avro-
    // generated POJOs). GenericRecord alone misses SpecificRecord — they're
    // parallel subtypes of IndexedRecord, not parent/child. Other Avro
    // GenericContainer types (GenericFixed, GenericEnumSymbol, GenericArray)
    // are intentionally NOT caught here — each has its own dedicated case
    // below; treating them as records would call getFields() on a non-record
    // schema and throw.
    if (value instanceof IndexedRecord) {
      return avroRecordToMap((IndexedRecord) value);
    }
    if (value instanceof GenericEnumSymbol) {
      return value.toString();
    }
    // Normalize all CharSequence subclasses (Utf8, StringBuilder, StringBuffer,
    // CharBuffer, etc.) to String. findCelTypeForClass declares any
    // CharSequence as STRING; without this, CEL string ops would dispatch
    // against a raw Utf8/StringBuilder and may not recognize it.
    if (value instanceof CharSequence) {
      return value.toString();
    }
    if (value instanceof Integer) {
      return ((Integer) value).longValue();
    }
    if (value instanceof Short) {
      return ((Short) value).longValue();
    }
    if (value instanceof Byte) {
      return ((Byte) value).longValue();
    }
    if (value instanceof Float) {
      return ((Float) value).doubleValue();
    }
    // CEL's bytes literal `b"..."` is parsed as CelByteString, and CelByteString
    // equality only matches other CelByteStrings — comparing against a raw
    // byte[] / ByteBuffer / GenericFixed returns false. Wrap every Avro/proto
    // bytes carrier so equality dispatches correctly and the AvroResultWriter
    // sees a uniform shape on output.
    if (value instanceof byte[]) {
      return CelByteString.of((byte[]) value);
    }
    if (value instanceof ByteString) {
      return CelByteString.of(((ByteString) value).toByteArray());
    }
    if (value instanceof ByteBuffer) {
      // Defensive duplicate so position-mutating reads here don't leak back to
      // whatever Avro reader handed us this buffer.
      ByteBuffer buf = ((ByteBuffer) value).duplicate();
      byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      return CelByteString.of(bytes);
    }
    if (value instanceof GenericFixed) {
      return CelByteString.of(((GenericFixed) value).bytes());
    }
    if (value instanceof List) {
      List<?> in = (List<?>) value;
      java.util.List<Object> out = new java.util.ArrayList<>(in.size());
      for (Object e : in) {
        out.add(toCelValue(e));
      }
      return out;
    }
    if (value instanceof Map) {
      Map<?, ?> in = (Map<?, ?>) value;
      Map<String, Object> out = new LinkedHashMap<>(in.size());
      for (Map.Entry<?, ?> e : in.entrySet()) {
        out.put(String.valueOf(e.getKey()), toCelValue(e.getValue()));
      }
      return out;
    }
    return value;
  }

  /**
   * JSON-flavored variant of {@link #toCelValue(Object)}: additionally converts
   * Jackson {@link JsonNode} and arbitrary POJOs to maps via the supplied
   * {@link ObjectMapper}. Used on the JSON validation/transform path where the
   * value passed in might be a bean — we materialize beans as maps so the CEL
   * runtime's native map field-access path applies.
   *
   * <p>Primitives, strings, byte buffers, proto Messages, Avro {@link GenericRecord}
   * and {@link Utf8} delegate to {@link #toCelValue(Object)}.
   */
  public static Object toCelValueForJson(Object value, ObjectMapper mapper) {
    // Same null normalization as toCelValue — convert J/P to dev.cel
    // NullValue so `value == null` dispatches correctly. Without this, a
    // direct Java-null binding becomes "unknown attribute" at the runtime.
    if (value == null || value instanceof NullValue) {
      return dev.cel.common.values.NullValue.NULL_VALUE;
    }
    if (value instanceof JsonNode) {
      // For object/array nodes, Jackson produces Map/List of Java natives via
      // the Map.class target — but Jackson keeps integers as Integer, not Long.
      // Pipe through toCelValue so narrow numerics get widened recursively
      // (CEL's int64 overloads won't dispatch on a raw Integer).
      JsonNode node = (JsonNode) value;
      if (node.isObject()) {
        return toCelValue(mapper.convertValue(node, new TypeReference<Map<String, Object>>() {}));
      }
      if (node.isArray()) {
        return toCelValue(
            mapper.convertValue(node, new TypeReference<java.util.List<Object>>() {}));
      }
      return toCelValue(mapper.convertValue(node, Object.class));
    }
    // dev.cel NullValue and CelByteString are CEL's own internal value types
    // — may appear when a previous evaluation's result is rebound (chained
    // transforms). Pass through unchanged; Jackson would otherwise serialize
    // CelByteString as a struct.
    if (value instanceof dev.cel.common.values.NullValue
        || value instanceof CelByteString) {
      return value;
    }
    // Pass-through cases that are already CEL-friendly or are handled by
    // toCelValue. byte[]/ByteString/ByteBuffer get wrapped as CelByteString
    // there (CEL bytes literals don't compare equal to raw byte[]).
    // GenericFixed routes via GenericContainer. Utf8 isn't listed separately
    // because it extends CharSequence.
    if (value instanceof CharSequence
        || value instanceof Number
        || value instanceof Boolean
        || value instanceof byte[]
        || value instanceof ByteString
        || value instanceof ByteBuffer
        || value instanceof Map
        || value instanceof List
        || value instanceof Message
        || value instanceof GenericContainer) {
      return toCelValue(value);
    }
    // Treat as POJO bean — Jackson uses property accessors / public fields
    // to convert to a Map. Pipe through toCelValue to widen narrow numerics
    // (Integer → Long) inside the resulting map; without that,
    // `this.intField <= 150` won't dispatch because CEL only registers the
    // int64 overload.
    return toCelValue(mapper.convertValue(value, new TypeReference<Map<String, Object>>() {}));
  }

  private static Map<String, Object> avroRecordToMap(IndexedRecord record) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (Schema.Field f : record.getSchema().getFields()) {
      out.put(f.name(), toCelValue(record.get(f.pos())));
    }
    return out;
  }
}
