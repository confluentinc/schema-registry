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
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.policy.FieldPairing;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoPublicImportWrappers;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code LOGICAL}'s checks over the subject's provenance. Paired by check id, they give the
 * provenance endpoint's compatible flag: what registration will answer once LOGICAL pairs by
 * provenance. Registration and the compatibility checks still use {@link LogicalPolicyChecker},
 * which also remains as the fallback below.
 *
 * <p>The rules, the logical types compared and the findings are {@link LogicalPolicyChecker}'s;
 * what differs is the pairing and what it needs. Each version is matched against its predecessor
 * alone, so provenance is computed only over the versions from the oldest one compared to the
 * last, the candidate after them: they pair as they would over the whole history, and none before
 * them is parsed. A candidate that blocks provenance — one through which no range can be computed
 * — is rejected. A range already blocked by an earlier version, which only a version registered
 * before the subject was checked this way or by IMPORT can be, is checked at registration by
 * {@link LogicalPolicyChecker} instead, as before, with a warning: the name-paired check converts
 * only the versions it compares, skips one it cannot convert, and needs no provenance. A stored
 * version judged is rejected instead, as no provenance pairs it with the versions compared.
 * Paired {@link PairBy#NAME by name}, the comparisons are otherwise {@link LogicalPolicyChecker}'s;
 * {@link PairBy#CHECK_ID by check id}, members continue as provenance continues them, through a
 * kind change too.
 *
 * <p>Protobuf provenance is computed with every file's messages wrapped, as a file of several is
 * checked; a file the policy checks as its one message takes that message's ids, so a file
 * changing between one message and several is still the reshaping it is.
 *
 * <p>A compatibility group selects the versions compared; provenance ignores groups, as the
 * endpoint's ids do, so the range includes other groups' versions between them.
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

  private static final Logger log = LoggerFactory.getLogger(ProvenancePolicyChecker.class);

  private ProvenancePolicyChecker() {
  }

  /**
   * The findings for registering {@code newSchema} after {@code history}, empty if all pass.
   *
   * @param subject the subject, for the warning when an earlier version blocks provenance
   * @param newSchema the schema being registered, or the stored version judged
   * @param history the subject's versions, soft-deleted ones included, in version order: every one
   *     from the oldest compared to the latest, and any before them, which are not parsed
   * @param versionNumbers each version's number in the registry, in the order of {@code history}
   * @param previousVersions the positions in {@code history} the level compares against,
   *     ascending: those {@link LogicalPolicyChecker} is given
   * @param level the configured compatibility level, or {@code null} (treated as NONE)
   * @param pairBy how members are paired
   * @param candidateNumber the candidate's number: the stored version judged, or the number after
   *     the last for a schema being registered
   * @param registering whether {@code newSchema} is being registered, not a stored version judged
   */
  static List<String> check(String subject, ParsedSchema newSchema,
      List<? extends ParsedSchemaHolder> history, List<Integer> versionNumbers,
      List<Integer> previousVersions, CompatibilityLevel level, PairBy pairBy,
      int candidateNumber, boolean registering) {
    List<String> errors = new ArrayList<>();
    LogicalType newLogical;
    try {
      newLogical = LogicalPolicyChecker.toPolicyLogicalType(newSchema);
    } catch (RuntimeException e) {
      errors.add(LogicalPolicyChecker.describeUnconvertible(e));
      return errors;
    }
    errors.addAll(LogicalPolicyChecker.describeValidity(newLogical));

    List<Integer> selected = selectForComparison(previousVersions, level);
    if (selected.isEmpty()) {
      return errors;
    }
    // The range: from the oldest version compared to the last, positions taken from its start.
    int from = selected.get(0);
    List<ParsedSchemaHolder> range = new ArrayList<>(history.subList(from, history.size()));
    List<Integer> rangeNumbers = new ArrayList<>(versionNumbers.subList(from, history.size()));
    List<Integer> toCompare = new ArrayList<>(selected.size());
    selected.forEach(version -> toCompare.add(version - from));
    int candidate = range.size();
    List<ParsedSchemaHolder> versions = new ArrayList<>(range);
    versions.add(new SimpleParsedSchemaHolder(newSchema));
    List<Integer> numbers = new ArrayList<>(rangeNumbers);
    numbers.add(candidateNumber);
    Provenance provenance;
    int[] reached = {-1};
    try {
      provenance = new Provenance(versions, numbers, newLogical, toCompare, reached);
    } catch (RuntimeException e) {
      if (isStoreFailure(e)) {
        throw e;
      }
      // Versions are converted and walked in order, so one failing before the candidate fails
      // without it too: the range is blocked already.
      if (reached[0] < candidate) {
        if (!registering) {
          // Registration must not be blocked by an old version; a verdict cannot skip one.
          errors.add(describeBlocked("through an earlier version", e.getMessage()));
          return errors;
        }
        log.warn("An earlier version of subject {} blocks schema provenance ({}), so LOGICAL "
            + "pairs it by name", subject, e.getMessage());
        return LogicalPolicyChecker.check(newSchema, holders(history, previousVersions), level);
      }
      errors.add(describeBlocked(
          registering ? "with the schema being registered" : "with this version",
          e.getMessage()));
      return errors;
    }
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

  /**
   * A stored version that could not be read, as a lazy holder reports it: a failure of the
   * registry, not of the version, unlike a schema that no longer parses.
   */
  private static boolean isStoreFailure(RuntimeException e) {
    return e instanceof IllegalStateException
        && e.getCause() instanceof SchemaRegistryException
        && !(e.getCause() instanceof InvalidSchemaException);
  }

  private static List<ParsedSchemaHolder> holders(List<? extends ParsedSchemaHolder> history,
      List<Integer> previousVersions) {
    List<ParsedSchemaHolder> holders = new ArrayList<>(previousVersions.size());
    for (int version : previousVersions) {
      holders.add(history.get(version));
    }
    return holders;
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
   * Provenance that cannot be computed, {@code where} as the description says, in the shape of the
   * other findings. Every mode is named, since no pairing is known for any.
   */
  private static String describeBlocked(String where, String message) {
    String modes = Arrays.stream(LogicalPolicyChecker.MODES)
        .map(mode -> "\"" + mode + "\"")
        .collect(Collectors.joining(", "));
    return "{errorType:\"PROVENANCE_UNAVAILABLE\", category:[" + modes
        + "], description:\"Schema provenance cannot be computed " + where + ": " + message
        + "\", additionalInfo:\"\"}";
  }

  /** The history's logical types as the policy checks them, and their check ids. */
  private static final class Provenance {

    private final List<LogicalType> logicals = new ArrayList<>();
    private final List<Boolean> singleMessage = new ArrayList<>();
    private final ProvenanceReport report;
    private final Map<Integer, ProvenanceReport.Version> reported = new HashMap<>();

    // The last version's logical type is lastLogical, the candidate's. reached[0] is left at the
    // index of the version being converted, then walked, when a failure is thrown.
    Provenance(List<ParsedSchemaHolder> versions, List<Integer> numbers, LogicalType lastLogical,
        List<Integer> toCompare, int[] reached) {
      List<SchemaType> schemaTypes = new ArrayList<>();
      List<LogicalType> computed = new ArrayList<>();
      for (int i = 0; i < versions.size(); i++) {
        ParsedSchema schema;
        LogicalType logical;
        LogicalType forProvenance;
        reached[0] = i;
        try {
          schema = versions.get(i).schema();
          logical = i == versions.size() - 1 ? lastLogical
              : LogicalPolicyChecker.toPolicyLogicalType(schema);
          // Protobuf wrapped, as a file of several messages is checked.
          forProvenance = schema instanceof ProtobufSchema
              ? ProvenanceHistory.logicalTypeOf(schema, true) : logical;
        } catch (RuntimeException e) {
          if (isStoreFailure(e)) {
            throw e;
          }
          // As the endpoint names a version with no logical form.
          throw new IllegalArgumentException("Version " + numbers.get(i) + ": " + e.getMessage(),
              e);
        }
        logicals.add(logical);
        computed.add(forProvenance);
        schemaTypes.add(SchemaType.of(schema.schemaType()));
        // A file checked as its one message keeps that message's ids.
        singleMessage.add(schema instanceof ProtobufSchema
            && !LogicalPolicyChecker.declaresSeveralMessages(
                ProtoPublicImportWrappers.resolve((ProtobufSchema) schema)));
      }
      Set<Integer> kept = new HashSet<>(toCompare);
      kept.add(versions.size() - 1);
      report = ProvenanceHistory.report(schemaTypes, numbers, i -> {
        reached[0] = i;
        return computed.get(i);
      }, kept::contains);
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
