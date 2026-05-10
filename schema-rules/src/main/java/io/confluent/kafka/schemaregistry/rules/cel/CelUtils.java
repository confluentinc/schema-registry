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
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.CelVarDecl;
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
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeBuilder;
import dev.cel.runtime.CelRuntimeFactory;
import io.confluent.kafka.schemaregistry.rules.cel.avro.AvroCelTypeProvider;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.BuiltinLibrary;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
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
    // For AVRO records, try Approach A first (CelTypeProvider with proper field
    // types). On compile failure, fall back to Approach B (declare `this` as
    // dynamic map). The fallback handles edge cases the type provider can't
    // represent (complex unions, etc.) without forcing every rule to use dyn.
    if (type == ScriptType.AVRO && schemaHint instanceof Schema
        && hasStructTypedThis(varDecls)) {
      try {
        return doBuildProgram(type, expr, schemaHint, varDecls);
      } catch (CelValidationException firstAttempt) {
        // Retry with `this` (and any other StructTypeReference vars) downgraded
        // to MapType<STRING, DYN> — Approach B.
        List<CelVarDecl> fallbackVarDecls = downgradeStructsToMap(varDecls);
        try {
          return doBuildProgram(type, expr, schemaHint, fallbackVarDecls);
        } catch (CelValidationException secondAttempt) {
          // Fallback also failed — surface the original error (more informative).
          throw firstAttempt;
        }
      }
    }
    return doBuildProgram(type, expr, schemaHint, varDecls);
  }

  private static CelRuntime.Program doBuildProgram(
      ScriptType type, String expr, Object schemaHint, List<CelVarDecl> varDecls)
      throws CelValidationException, CelEvaluationException {
    // CelExtensions.strings() supplies charAt/indexOf/lastIndexOf/lowerAscii/
    // upperAscii/replace/split/substring/trim/join. The class implements both
    // CelCompilerLibrary and CelRuntimeLibrary, so the same instance binds to
    // both sides.
    // standardCelCompilerBuilder() omits comprehension macros (has/all/exists/
    // exists_one/map/filter); they have to be wired in explicitly. Without
    // them, expressions like `tags.exists_one(x, x == 'PII')` fail to compile.
    CelCompilerBuilder compilerBuilder = CelCompilerFactory.standardCelCompilerBuilder()
        .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
        .addLibraries(new BuiltinLibrary(), CelExtensions.strings())
        .addVarDeclarations(varDecls);
    CelRuntimeBuilder runtimeBuilder = CelRuntimeFactory.standardCelRuntimeBuilder()
        .addLibraries(new BuiltinLibrary(), CelExtensions.strings());

    if (type == ScriptType.PROTOBUF) {
      Descriptor desc = (Descriptor) schemaHint;
      compilerBuilder.addMessageTypes(desc);
      runtimeBuilder.addMessageTypes(desc);
    } else if (type == ScriptType.AVRO && schemaHint instanceof Schema
        && hasStructTypedThis(varDecls)) {
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
    return runtime.createProgram(ast);
  }

  private static boolean hasStructTypedThis(List<CelVarDecl> varDecls) {
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
    if (arg == null) {
      return SimpleType.NULL_TYPE;
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
   * Map an Avro {@link Schema} to a {@link CelType}. Records and maps both map to
   * {@code map<string, dyn>} because we pre-convert {@link GenericRecord} values to
   * JDK maps before binding.
   */
  public static CelType findCelTypeForAvroSchema(Schema schema) {
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
        // Approach A: declare as a struct reference so the AvroCelTypeProvider
        // (registered in buildProgram) resolves field types. If the type
        // provider can't represent this record cleanly, the type-checker will
        // fail and the caller falls back to MapType+toCelValue (Approach B).
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
    } else if (type == byte[].class || type == ByteString.class) {
      return SimpleType.BYTES;
    } else if (type == double.class
        || type == Double.class
        || type == float.class
        || type == Float.class) {
      return SimpleType.DOUBLE;
    } else if (type == String.class) {
      return SimpleType.STRING;
    } else if (type == Duration.class || type == java.time.Duration.class) {
      return SimpleType.DURATION;
    } else if (type == Timestamp.class
        || Instant.class.isAssignableFrom(type)
        || ZonedDateTime.class.isAssignableFrom(type)) {
      return SimpleType.TIMESTAMP;
    } else if (Map.class.isAssignableFrom(type)) {
      return MapType.create(SimpleType.STRING, SimpleType.DYN);
    } else if (List.class.isAssignableFrom(type)) {
      return ListType.create(SimpleType.DYN);
    } else {
      // Unknown POJO bean class: bind as a generic map. JSON values flow
      // through toCelValueForJson which Jackson-converts beans to Map<String,
      // Object>, so map-typed access is the right declaration. (Returning a
      // StructTypeReference would fail at compile because the CEL compiler
      // has no type provider for arbitrary POJO classes.)
      return MapType.create(SimpleType.STRING, SimpleType.DYN);
    }
  }

  /**
   * Convert a native value to a CEL-compatible shape:
   * <ul>
   *   <li>{@link GenericRecord} → JDK {@link Map} (Google cel-java's runtime
   *       selects fields on Maps natively)</li>
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
    if (value == null) {
      return null;
    }
    if (value instanceof GenericRecord) {
      return avroRecordToMap((GenericRecord) value);
    }
    if (value instanceof Utf8) {
      return value.toString();
    }
    if (value instanceof GenericEnumSymbol) {
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
    // byte[] returns false. Wrap incoming Java byte[] (the type
    // ProtobufSchema#fieldTransform converts ByteString to before invoking the
    // field-rule executor) so equality dispatches correctly.
    if (value instanceof byte[]) {
      return CelByteString.of((byte[]) value);
    }
    if (value instanceof ByteString) {
      return CelByteString.of(((ByteString) value).toByteArray());
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
      Map<String, Object> out = new HashMap<>(in.size());
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
    if (value == null) {
      return null;
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
    // Pass CEL's own internal value types through unchanged. NullValue (both
    // proto and dev.cel flavors) and CelByteString may appear when a previous
    // CEL evaluation's result is rebound (e.g., chained transforms). Letting
    // them fall through to Jackson would either turn NullValue into Java null
    // (which the CEL runtime treats as unknown — references become
    // CelUnknownSet) or serialize CelByteString as a struct.
    if (value instanceof NullValue
        || value instanceof dev.cel.common.values.NullValue
        || value instanceof CelByteString) {
      return value;
    }
    // Pass-through cases that are already CEL-friendly or are handled by
    // toCelValue. byte[]/ByteString get wrapped as CelByteString there (CEL
    // bytes literals don't compare equal to raw byte[]). Utf8 isn't listed
    // separately because it extends CharSequence.
    if (value instanceof CharSequence
        || value instanceof Number
        || value instanceof Boolean
        || value instanceof byte[]
        || value instanceof ByteString
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

  private static Map<String, Object> avroRecordToMap(GenericRecord record) {
    Map<String, Object> out = new HashMap<>();
    for (Schema.Field f : record.getSchema().getFields()) {
      out.put(f.name(), toCelValue(record.get(f.pos())));
    }
    return out;
  }
}
