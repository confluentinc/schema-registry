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

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.policy.Incompatibility;
import io.confluent.kafka.schemaregistry.type.logical.policy.Invalidity;
import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker;
import io.confluent.kafka.schemaregistry.type.logical.policy.LogicalTypeChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the {@link LogicalTypeChecker} validity and compatibility checks that
 * {@code CompatibilityPolicy.LOGICAL} adds on top of a schema's native compatibility check.
 *
 * <p>These checks are <b>additive</b>: the native Avro/JSON/Protobuf compatibility check still runs
 * unchanged, and any findings here are appended to the same error list, so {@code LOGICAL} can only
 * ever make a registration stricter, never more permissive.
 *
 * <p>The checks run in {@link Mode#FLINK} and {@link Mode#ICEBERG_V2} -- the union of downstream
 * consumers currently gated, excluding {@code ICEBERG_V3}, which has no way yet to know a subject's
 * target Iceberg format version.
 */
public final class LogicalPolicyChecker {

  private static final Logger log = LoggerFactory.getLogger(LogicalPolicyChecker.class);

  private static final Mode[] MODES = {Mode.FLINK, Mode.ICEBERG_V2};

  private LogicalPolicyChecker() {
  }

  /**
   * Converts a native parsed schema into a {@link LogicalType} by dispatching on its schema type.
   *
   * @throws IllegalArgumentException if the schema type has no logical-type converter
   */
  public static LogicalType toLogicalType(ParsedSchema parsedSchema) {
    String schemaType = parsedSchema.schemaType();
    if (schemaType == null || AvroSchema.TYPE.equalsIgnoreCase(schemaType)) {
      return AvroToLogicalTypeConverter.toLogicalType((AvroSchema) parsedSchema);
    } else if (JsonSchema.TYPE.equalsIgnoreCase(schemaType)) {
      return JsonToLogicalTypeConverter.toLogicalType((JsonSchema) parsedSchema);
    } else if (ProtobufSchema.TYPE.equalsIgnoreCase(schemaType)) {
      return ProtoToLogicalTypeConverter.toLogicalType((ProtobufSchema) parsedSchema);
    }
    throw new IllegalArgumentException(
        "format=logical is not supported for schema type '" + schemaType + "'");
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
   * @param newSchema       the schema being registered
   * @param previousSchemas prior versions, ascending (oldest first, latest last)
   * @param level           the configured compatibility level, or {@code null} (treated as NONE)
   */
  static List<String> check(
      ParsedSchema newSchema,
      List<ParsedSchemaHolder> previousSchemas,
      CompatibilityLevel level) {
    List<String> errors = new ArrayList<>();

    LogicalType newLogical;
    try {
      newLogical = toLogicalType(newSchema);
    } catch (RuntimeException e) {
      errors.add("Schema cannot be represented as a logical type: " + e.getMessage());
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
        previousLogical = toLogicalType(holder.schema());
      } catch (RuntimeException e) {
        // Skip an unconvertible previous version rather than blocking this registration.
        log.warn("Skipping logical compatibility against a previous version that could not be "
            + "converted to a logical type: {}", e.getMessage());
        continue;
      }
      if (checksBackward) {
        // BACKWARD: the new schema must read data written with the previous schema.
        addCompareErrors(errors, previousLogical, newLogical);
      }
      if (checksForward) {
        // FORWARD: the previous schema must read data written with the new schema.
        addCompareErrors(errors, newLogical, previousLogical);
      }
    }
    return errors;
  }

  private static List<String> describeValidity(LogicalType newLogical) {
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

  private static void addCompareErrors(
      List<String> errors, LogicalType original, LogicalType update) {
    Map<FindingKey, Map<Mode, String>> byFinding = new LinkedHashMap<>();
    for (Mode mode : MODES) {
      for (Incompatibility incompatibility
          : LogicalTypeChecker.compare(mode, original, update).getIncompatibilities()) {
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

  private static boolean isTransitive(CompatibilityLevel level) {
    return level == CompatibilityLevel.BACKWARD_TRANSITIVE
        || level == CompatibilityLevel.FORWARD_TRANSITIVE
        || level == CompatibilityLevel.FULL_TRANSITIVE;
  }

  private static boolean checksBackward(CompatibilityLevel level) {
    return level == CompatibilityLevel.BACKWARD
        || level == CompatibilityLevel.BACKWARD_TRANSITIVE
        || level == CompatibilityLevel.FULL
        || level == CompatibilityLevel.FULL_TRANSITIVE;
  }

  private static boolean checksForward(CompatibilityLevel level) {
    return level == CompatibilityLevel.FORWARD
        || level == CompatibilityLevel.FORWARD_TRANSITIVE
        || level == CompatibilityLevel.FULL
        || level == CompatibilityLevel.FULL_TRANSITIVE;
  }
}
