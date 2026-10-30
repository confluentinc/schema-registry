/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import com.squareup.wire.schema.internal.parser.MessageElement;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.TypeTooDeepException;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.common.ToLogicalContext;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.policy.FieldPairing;
import io.confluent.kafka.schemaregistry.type.logical.policy.Incompatibility;
import io.confluent.kafka.schemaregistry.type.logical.policy.Incompatibility.Rule;
import io.confluent.kafka.schemaregistry.type.logical.policy.Invalidity;
import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker;
import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoPublicImportWrappers;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the {@link LogicalTypeChecker} validity and compatibility checks that
 * {@code CompatibilityPolicy.LOGICAL} adds on top of a schema's native compatibility check.
 *
 * <p>These checks are <b>additive</b>: the native Avro/JSON/Protobuf compatibility check still
 * runs, as {@code LENIENT}, and any findings here are appended to the same error list, so
 * {@code LOGICAL} can only ever make a registration stricter than {@code LENIENT}, never more
 * permissive.
 *
 * <p>The checks run in {@link Mode#FLINK} and {@link Mode#ICEBERG_V2} -- the union of downstream
 * consumers currently gated, excluding {@code ICEBERG_V3}, which has no way yet to know a subject's
 * target Iceberg format version.
 */
public final class LogicalPolicyChecker {

  private static final Logger log = LoggerFactory.getLogger(LogicalPolicyChecker.class);

  static final Mode[] MODES = {Mode.FLINK, Mode.ICEBERG_V2};

  private LogicalPolicyChecker() {
  }

  /**
   * Converts a native parsed schema into a {@link LogicalType} by dispatching on its schema type,
   * using the canonical {@link LogicalTypeVersion#V2} reading. This is what {@code format=logical}
   * exposes over the REST API.
   *
   * @throws IllegalArgumentException if the schema type has no logical-type converter
   */
  public static LogicalType toLogicalType(ParsedSchema parsedSchema) {
    return toLogicalType(parsedSchema, LogicalTypeVersion.V2);
  }

  /**
   * As {@link #toLogicalType(ParsedSchema)}, but selects the JSON derivation edition explicitly.
   * Avro and Protobuf have no edition concept, so {@code edition} only affects a JSON schema.
   *
   * @throws IllegalArgumentException if the schema type has no logical-type converter
   */
  private static LogicalType toLogicalType(ParsedSchema parsedSchema, LogicalTypeVersion edition) {
    String schemaType = parsedSchema.schemaType();
    if (schemaType == null || AvroSchema.TYPE.equalsIgnoreCase(schemaType)) {
      return AvroToLogicalTypeConverter.toLogicalType((AvroSchema) parsedSchema);
    } else if (JsonSchema.TYPE.equalsIgnoreCase(schemaType)) {
      return JsonToLogicalTypeConverter.toLogicalType((JsonSchema) parsedSchema, edition);
    } else if (ProtobufSchema.TYPE.equalsIgnoreCase(schemaType)) {
      return ProtoToLogicalTypeConverter.toLogicalType((ProtobufSchema) parsedSchema);
    }
    throw new IllegalArgumentException(
        "format=logical is not supported for schema type '" + schemaType + "'");
  }

  /**
   * The logical type the policy checks: the {@link LogicalTypeVersion#V1} reading, with a Protobuf
   * file as Flink reads it rather than as {@code format=logical} shows it.
   *
   * @throws TypeTooDeepException if it nests more than {@link ToLogicalContext#MAX_TYPE_DEPTH} deep
   *     with its named types inlined
   */
  static LogicalType toPolicyLogicalType(ParsedSchema parsedSchema) {
    LogicalType logicalType = ProtobufSchema.TYPE.equalsIgnoreCase(parsedSchema.schemaType())
        ? protobufLogicalType((ProtobufSchema) parsedSchema)
        : toLogicalType(parsedSchema, LogicalTypeVersion.V1);
    // The converters bound inline nesting only; the checks also walk through named types, so a
    // chain of references could overflow them.
    if (InlinedDepth.of(logicalType) > ToLogicalContext.MAX_TYPE_DEPTH) {
      throw new TypeTooDeepException("Schema nests types more than "
          + ToLogicalContext.MAX_TYPE_DEPTH + " deep with its named types inlined");
    }
    return logicalType;
  }

  /**
   * A Protobuf file as Flink reads it: a single top-level message is its own row, and a file with
   * several wraps each message, in file order, as the converter's multi-message reading does. So
   * every message is checked, and a file changing between one and several messages is seen as the
   * reshaping it is.
   */
  private static LogicalType protobufLogicalType(ProtobufSchema schema) {
    // A file declaring nothing but a public import is written, and read by Flink, as the file it
    // re-exports: checked as that, not as the bare reference format=logical keeps for it.
    String root = ProtoPublicImportWrappers.rootParam(schema);
    schema = ProtoPublicImportWrappers.resolve(schema);
    // A root param picks the one message its records carry, of a file that may declare several.
    if (root != null) {
      return ProtoToLogicalTypeConverter.toLogicalType(schema.copy(root), false);
    }
    return ProtoToLogicalTypeConverter.toLogicalType(schema, declaresSeveralMessages(schema));
  }

  /**
   * Whether the policy checks a Protobuf file as several messages, each wrapped, rather than as its
   * one message: {@code schema} as {@link ProtoPublicImportWrappers#resolve} leaves it.
   */
  static boolean declaresSeveralMessages(ProtobufSchema schema) {
    return schema.rawSchema().getTypes().stream()
        .filter(type -> type instanceof MessageElement).count() > 1;
  }


  /**
   * Runs the logical validity and compatibility checks for {@code newSchema} and returns any
   * findings as human-readable error strings (empty if all pass).
   *
   * <p>Validity runs on {@code newSchema} regardless of compatibility level, since it is a property
   * of the schema itself, not of a change. Compatibility runs against previous versions selected by
   * {@code level}: transitive levels compare against every previous version, non-transitive levels
   * against the latest only, and {@code NONE} skips comparison entirely. Direction follows the
   * level -- backward levels require the new schema to read the old, forward levels the reverse,
   * full levels both.
   *
   * <p>If {@code newSchema} cannot be converted to a logical type the registration is rejected (a
   * finding is returned). If a <i>previous</i> schema cannot be converted, that comparison is
   * skipped rather than failing the registration -- an old version being unconvertible must not
   * block a new one (per design decision).
   *
   * <p>Both schemas are derived by {@link #toPolicyLogicalType}: under
   * {@link LogicalTypeVersion#V1}, matching provenance, and with a Protobuf file of several
   * messages read as Flink reads it --
   * not the {@link LogicalTypeVersion#V2} canonical reading {@link #toLogicalType(ParsedSchema)}
   * uses for {@code format=logical}. The two editions can disagree on structural kind (a singleton
   * JSON {@code oneOf} collapses to its member type under V2 but stays a {@code UNION} under V1),
   * so mixing editions across the two call sites would make the same field look edited when it is
   * not.
   *
   * @param newSchema       the schema being registered
   * @param previousSchemas prior versions, ascending (oldest first, latest last)
   * @param level           the configured compatibility level, or {@code null} (treated as NONE)
   */
  static List<String> check(
      ParsedSchema newSchema,
      List<ParsedSchemaHolder> previousSchemas,
      CompatibilityLevel level) {
    return check(newSchema, previousSchemas, level, Rule.defaults());
  }

  /**
   * As {@link #check(ParsedSchema, List, CompatibilityLevel)}, comparing with only
   * {@code enabledRules}, as {@link #parseChecks} resolves them.
   */
  static List<String> check(
      ParsedSchema newSchema,
      List<ParsedSchemaHolder> previousSchemas,
      CompatibilityLevel level,
      Set<Rule> enabledRules) {
    List<String> errors = new ArrayList<>();

    LogicalType newLogical;
    try {
      newLogical = toPolicyLogicalType(newSchema);
    } catch (RuntimeException e) {
      errors.add(describeUnconvertible(e));
      return errors;
    }

    // Validity: a property of the new schema itself, independent of any previous version.
    errors.addAll(describeValidity(newLogical));

    List<ParsedSchemaHolder> toCompare = selectForComparison(previousSchemas, level);
    boolean checksBackward = checksBackward(level);
    boolean checksForward = checksForward(level);
    for (ParsedSchemaHolder holder : toCompare) {
      LogicalType previousLogical;
      try {
        previousLogical = toPolicyLogicalType(holder.schema());
      } catch (RuntimeException e) {
        // Skip an unconvertible previous version rather than blocking this registration.
        log.warn("Skipping logical compatibility against a previous version that could not be "
            + "converted to a logical type: {}", e.getMessage());
        continue;
      }
      SchemaType schemaType = sharedSchemaType(newSchema, holder.schema());
      if (checksBackward) {
        // BACKWARD: the new schema must read data written with the previous schema.
        addCompareErrors(errors, previousLogical, newLogical, FieldPairing.byName(),
            enabledRules, schemaType);
      }
      if (checksForward) {
        // FORWARD: the previous schema must read data written with the new schema.
        addCompareErrors(errors, newLogical, previousLogical, FieldPairing.byName(),
            enabledRules, schemaType);
      }
    }
    return errors;
  }

  /**
   * The format both schemas are in, or {@code null}: renames are recognised by format, so only
   * between two versions of the same one.
   */
  static SchemaType sharedSchemaType(ParsedSchema schema, ParsedSchema other) {
    return schema.schemaType().equals(other.schemaType())
        ? SchemaType.of(schema.schemaType()) : null;
  }

  static List<String> describeValidity(LogicalType newLogical) {
    Map<FindingKey, Map<Mode, String>> byFinding = new LinkedHashMap<>();
    for (Mode mode : MODES) {
      for (Invalidity invalidity
          : LogicalTypeChecker.validate(mode, newLogical).getInvalidities()) {
        byFinding
            .computeIfAbsent(
                new FindingKey(invalidity.getRule().name(), invalidity.getPath()),
                k -> new LinkedHashMap<>())
            .put(mode, invalidity.getMessage());
      }
    }
    List<String> errors = new ArrayList<>();
    for (Map.Entry<FindingKey, Map<Mode, String>> entry : byFinding.entrySet()) {
      errors.add(describeFinding(entry.getKey(), entry.getValue()));
    }
    return errors;
  }

  static void addCompareErrors(List<String> errors, LogicalType original, LogicalType update,
      FieldPairing pairing, Set<Rule> enabledRules, SchemaType schemaType) {
    Map<FindingKey, Map<Mode, String>> byFinding = new LinkedHashMap<>();
    for (Mode mode : MODES) {
      for (Incompatibility incompatibility
          : LogicalTypeChecker.compare(mode, original, update, pairing, enabledRules, schemaType)
              .getIncompatibilities()) {
        byFinding
            .computeIfAbsent(
                new FindingKey(incompatibility.getRule().name(), incompatibility.getPath()),
                k -> new LinkedHashMap<>())
            .put(mode, incompatibility.getMessage());
      }
    }
    for (Map.Entry<FindingKey, Map<Mode, String>> entry : byFinding.entrySet()) {
      errors.add(describeFinding(entry.getKey(), entry.getValue()));
    }
  }

  /**
   * Renders a schema that could not be converted to a {@link LogicalType} at all, in the same
   * {@code {errorType, category, description, additionalInfo}} shape {@link #describeFinding}
   * produces -- a bare string here used to sit next to those objects in the same {@code details}
   * array, breaking a client that parses every element as one of them.
   *
   * <p>{@code errorType} is {@link Invalidity.Rule#UNREPRESENTABLE_TYPE}: the same "consumer's type
   * system cannot represent this at all" judgment {@link ValidityChecker} makes once a schema
   * converts, just reached here because it never got that far. {@code category} lists every mode,
   * since a schema that cannot become a {@link LogicalType} blocks all of them alike, not one in
   * particular.
   *
   * <p>{@link TypeTooDeepException} gets its own customer-facing wording per the error-message
   * spec, naming the actual limit rather than the internal exception; every other conversion
   * failure falls back to a generic, still-actionable message built around the raw exception
   * text, which was never written to be customer-facing and is the one message in this package
   * not fully conformed to that spec.
   */
  static String describeUnconvertible(RuntimeException e) {
    String description = e instanceof TypeTooDeepException
        ? "The schema nesting exceeds '" + ToLogicalContext.MAX_TYPE_DEPTH + "' levels. Flatten "
            + "it to '" + ToLogicalContext.MAX_TYPE_DEPTH + "' levels or fewer."
        : "This schema can't be converted to a table. " + e.getMessage();
    String modes = Arrays.stream(MODES)
        .map(mode -> "\"" + mode + "\"")
        .collect(Collectors.joining(", "));
    return "{errorType:\"" + Invalidity.Rule.UNREPRESENTABLE_TYPE + "\", category:[" + modes
        + "], description:\"" + description + "\", additionalInfo:\"\"}";
  }

  /**
   * {@code version} carrying the schema id {@code id} of {@code earlierVersion}, in the shape of
   * the other findings, every mode named.
   */
  public static String describeRepeatedId(int version, int id, int earlierVersion) {
    String modes = Arrays.stream(MODES)
        .map(mode -> "\"" + mode + "\"")
        .collect(Collectors.joining(", "));
    return "{errorType:\"SCHEMA_ID_REPEATED\", category:[" + modes + "], description:\"Version "
        + version + " has schema id " + id + ", as version " + earlierVersion + " does\", "
        + "additionalInfo:\"\"}";
  }

  /**
   * Renders one finding as {@code {errorType:"<rule>", category:["<mode>", ...],
   * description:"<message>", additionalInfo:"<path>"}} -- mirroring the {@code {errorType,
   * description, additionalInfo}} shape the native JSON/Protobuf checks already use (see
   * {@code io.confluent.kafka.schemaregistry.json.diff.Difference}), with {@code category} added
   * to carry which mode(s) flagged it.
   *
   * <p>{@code category} lists every mode that flagged this {@code (rule, path)} -- {@link
   * Mode#FLINK} and {@link Mode#ICEBERG_V2} frequently flag the same underlying violation at the
   * same path, since their checkers are independent implementations. Only one message is ever
   * shown: when modes disagree on the wording, {@link #MODES}' order (FLINK before ICEBERG_V2)
   * picks a single representative rather than repeating the same finding once per mode.
   */
  private static String describeFinding(FindingKey key, Map<Mode, String> messagesByMode) {
    String modes = messagesByMode.keySet().stream()
        .map(mode -> "\"" + mode + "\"")
        .collect(Collectors.joining(", "));
    String message = messagesByMode.values().iterator().next();
    return "{errorType:\"" + key.rule + "\", category:[" + modes + "], description:\""
        + message + "\", additionalInfo:\"" + key.path + "\"}";
  }

  /** Groups a validity/compatibility finding by the rule it violated and the field path. */
  private static final class FindingKey {
    private final String rule;
    private final String path;

    FindingKey(String rule, String path) {
      this.rule = rule;
      this.path = path == null ? "" : path;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FindingKey)) {
        return false;
      }
      FindingKey that = (FindingKey) o;
      return rule.equals(that.rule) && path.equals(that.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(rule, path);
    }
  }

  /**
   * Resolves {@code schema.logical.checks} entries, each {@code +RULE} or {@code -RULE}, against
   * {@link Rule#defaults()}: a rule not named keeps its default.
   *
   * @throws IllegalArgumentException for an entry without a sign, an unknown rule, or a rule both
   *     enabled and disabled
   */
  public static Set<Rule> parseChecks(List<String> entries) {
    Set<Rule> enabled = Rule.defaults();
    Map<Rule, Boolean> seen = new EnumMap<>(Rule.class);
    for (String entry : entries) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty() || (trimmed.charAt(0) != '+' && trimmed.charAt(0) != '-')) {
        throw new IllegalArgumentException("Entry '" + entry + "' must start with '+' to enable "
            + "a check or '-' to disable it");
      }
      boolean enable = trimmed.charAt(0) == '+';
      String name = trimmed.substring(1).trim().toUpperCase(Locale.ROOT);
      Rule rule;
      try {
        rule = Rule.valueOf(name);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unknown check '" + trimmed.substring(1)
            + "'; expected one of " + Arrays.toString(Rule.values()));
      }
      Boolean previous = seen.put(rule, enable);
      if (previous != null && previous != enable) {
        throw new IllegalArgumentException("Check '" + rule + "' is both enabled and disabled");
      }
      if (enable) {
        enabled.add(rule);
      } else {
        enabled.remove(rule);
      }
    }
    return enabled;
  }

  /**
   * Returns whether {@code LOGICAL} can be combined with {@code level}: only the backward and full
   * levels, since Iceberg supports only backward-compatible evolution. NONE and null are rejected.
   */
  public static boolean supportsLevel(CompatibilityLevel level) {
    return checksBackward(level);
  }

  /**
   * The error reported for a {@code level} that {@link #supportsLevel} rejects.
   */
  public static String unsupportedLevelMessage(CompatibilityLevel level) {
    return "compatibilityLevel='" + level + "' doesn't work with compatibilityPolicy='LOGICAL'. "
        + "Use BACKWARD or FULL.";
  }

  private static List<ParsedSchemaHolder> selectForComparison(
      List<ParsedSchemaHolder> previousSchemas, CompatibilityLevel level) {
    if (previousSchemas.isEmpty() || level == null || level == CompatibilityLevel.NONE) {
      return List.of();
    }
    if (isTransitive(level)) {
      return previousSchemas;
    }
    // Non-transitive: latest version only, which is the last element (ascending order).
    return List.of(previousSchemas.get(previousSchemas.size() - 1));
  }

  static boolean isTransitive(CompatibilityLevel level) {
    return level == CompatibilityLevel.BACKWARD_TRANSITIVE
        || level == CompatibilityLevel.FORWARD_TRANSITIVE
        || level == CompatibilityLevel.FULL_TRANSITIVE;
  }

  static boolean checksBackward(CompatibilityLevel level) {
    return level == CompatibilityLevel.BACKWARD
        || level == CompatibilityLevel.BACKWARD_TRANSITIVE
        || level == CompatibilityLevel.FULL
        || level == CompatibilityLevel.FULL_TRANSITIVE;
  }

  static boolean checksForward(CompatibilityLevel level) {
    return level == CompatibilityLevel.FORWARD
        || level == CompatibilityLevel.FORWARD_TRANSITIVE
        || level == CompatibilityLevel.FULL
        || level == CompatibilityLevel.FULL_TRANSITIVE;
  }
}
