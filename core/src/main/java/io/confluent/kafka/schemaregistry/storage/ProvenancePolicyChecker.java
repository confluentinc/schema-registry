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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.policy.FieldPairing;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoPublicImportWrappers;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceComputer;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceReport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code LOGICAL}'s checks over the subject's provenance: the second implementation, beside
 * {@link LogicalPolicyChecker}, until it replaces it. Not yet called by registration.
 *
 * <p>The rules, the logical types compared and the findings are {@link LogicalPolicyChecker}'s;
 * what differs is the pairing and what it needs. Provenance is computed over the whole history, the
 * candidate last, so a version that cannot be converted or blocks provenance rejects the
 * registration, where {@link LogicalPolicyChecker} skips a previous version it cannot convert.
 * Paired {@link PairBy#NAME by name}, the comparisons are otherwise {@link LogicalPolicyChecker}'s;
 * {@link PairBy#CHECK_ID by check id}, members continue as provenance continues them, through a
 * kind change too.
 *
 * <p>Protobuf provenance is computed with every file's messages wrapped, as a file of several is
 * checked; a file the policy checks as its one message takes that message's ids, so a file
 * changing between one message and several is still the reshaping it is.
 *
 * <p>A caller with a compatibility group uses {@link LogicalPolicyChecker}: provenance does not
 * follow groups.
 */
final class ProvenancePolicyChecker {

  /** How the writer's and the reader's members are paired. */
  enum PairBy {
    /**
     * As {@link LogicalPolicyChecker} pairs them.
     */
    NAME,
    /** By provenance check id. */
    CHECK_ID
  }

  private ProvenancePolicyChecker() {
  }

  /**
   * The findings for registering {@code newSchema} after {@code history}, empty if all pass.
   *
   * @param newSchema the schema being registered
   * @param history every version of the subject, soft-deleted ones included, in version order
   * @param previousVersions the positions in {@code history} the level compares against,
   *     ascending: those {@link LogicalPolicyChecker} is given
   * @param level the configured compatibility level, or {@code null} (treated as NONE)
   * @param pairBy how members are paired
   */
  static List<String> check(ParsedSchema newSchema, List<ParsedSchema> history,
      List<Integer> previousVersions, CompatibilityLevel level, PairBy pairBy) {
    List<String> errors = new ArrayList<>();
    LogicalType newLogical;
    try {
      newLogical = LogicalPolicyChecker.toPolicyLogicalType(newSchema);
    } catch (RuntimeException e) {
      errors.add(LogicalPolicyChecker.describeUnconvertible(e.getMessage()));
      return errors;
    }
    errors.addAll(LogicalPolicyChecker.describeValidity(newLogical));

    List<Integer> toCompare = selectForComparison(previousVersions, level);
    if (toCompare.isEmpty()) {
      return errors;
    }
    Provenance provenance;
    try {
      provenance = new Provenance(history, newSchema, newLogical, toCompare);
    } catch (RuntimeException e) {
      errors.add(describeBlocked(e.getMessage()));
      return errors;
    }
    int candidate = history.size();
    for (int version : toCompare) {
      LogicalType previousLogical = provenance.logical(version);
      if (LogicalPolicyChecker.checksBackward(level)) {
        LogicalPolicyChecker.addCompareErrors(errors, previousLogical, newLogical,
            provenance.pairing(pairBy, version, candidate));
      }
      if (LogicalPolicyChecker.checksForward(level)) {
        LogicalPolicyChecker.addCompareErrors(errors, newLogical, previousLogical,
            provenance.pairing(pairBy, candidate, version));
      }
    }
    return errors;
  }

  private static List<Integer> selectForComparison(List<Integer> previousVersions,
      CompatibilityLevel level) {
    if (previousVersions.isEmpty() || level == null || level == CompatibilityLevel.NONE) {
      return List.of();
    }
    if (LogicalPolicyChecker.isTransitive(level)) {
      return previousVersions;
    }
    return List.of(previousVersions.get(previousVersions.size() - 1));
  }

  /**
   * A history provenance cannot be computed through, in the shape of the other findings. Every
   * mode is named, since no pairing is known for any.
   */
  private static String describeBlocked(String message) {
    String modes = Arrays.stream(LogicalPolicyChecker.MODES)
        .map(mode -> "\"" + mode + "\"")
        .collect(Collectors.joining(", "));
    return "{errorType:\"PROVENANCE_UNAVAILABLE\", category:[" + modes
        + "], description:\"Schema provenance cannot be computed: " + message
        + "\", additionalInfo:\"\"}";
  }

  /** The history's logical types as the policy checks them, and their check ids. */
  private static final class Provenance {

    private final List<LogicalType> logicals = new ArrayList<>();
    private final List<Boolean> singleMessage = new ArrayList<>();
    private final ProvenanceReport report;
    private final Map<Integer, ProvenanceReport.Version> reported = new HashMap<>();

    Provenance(List<ParsedSchema> history, ParsedSchema newSchema, LogicalType newLogical,
        List<Integer> toCompare) {
      List<ParsedSchema> versions = new ArrayList<>(history);
      versions.add(newSchema);
      List<SchemaType> schemaTypes = new ArrayList<>();
      List<LogicalType> computed = new ArrayList<>();
      for (int i = 0; i < versions.size(); i++) {
        ParsedSchema schema = versions.get(i);
        LogicalType logical;
        try {
          logical = i == history.size() ? newLogical
              : LogicalPolicyChecker.toPolicyLogicalType(schema);
        } catch (RuntimeException e) {
          throw new IllegalArgumentException(
              "version " + (i + 1) + " has no logical type: " + e.getMessage(), e);
        }
        logicals.add(logical);
        schemaTypes.add(SchemaType.of(schema.schemaType()));
        if (schema instanceof ProtobufSchema) {
          // Wrapped, as a file of several messages is checked; one of one message keeps its own.
          singleMessage.add(!LogicalPolicyChecker.declaresSeveralMessages(
              ProtoPublicImportWrappers.resolve((ProtobufSchema) schema)));
          computed.add(ProvenanceHistory.logicalTypeOf(schema, true));
        } else {
          singleMessage.add(false);
          computed.add(logical);
        }
      }
      Set<Integer> kept = new HashSet<>(toCompare);
      kept.add(history.size());
      report = ProvenanceComputer.report(schemaTypes, computed::get, kept::contains);
      for (ProvenanceReport.Version version : report.getVersions()) {
        reported.put(version.getIndex(), version);
      }
    }

    LogicalType logical(int version) {
      return logicals.get(version);
    }

    FieldPairing pairing(PairBy pairBy, int writer, int reader) {
      return pairBy == PairBy.NAME ? FieldPairing.byName()
          : FieldPairing.byIds(checkIds(writer), checkIds(reader));
    }

    // A file checked as its one message is that message, the wrapper's first member.
    private Map<List<Integer>, Integer> checkIds(int version) {
      Map<List<Integer>, Integer> ids = reported.get(version).checkIds();
      if (!singleMessage.get(version)) {
        return ids;
      }
      Map<List<Integer>, Integer> unwrapped = new HashMap<>();
      ids.forEach((path, id) -> {
        if (path.size() > 1 && path.get(0) == 0) {
          unwrapped.put(path.subList(1, path.size()), id);
        }
      });
      return unwrapped;
    }
  }
}
