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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceAlgorithm;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

/**
 * What a provenance endpoint computes from a subject's history, independent of where the history
 * is stored — so the registry and any client standing in for it answer identically.
 *
 * <p>Provenance is computed over the requested range only — its two ends and every version between
 * them — and then sliced to what the request returns. Ids are allocated from the range's first
 * version, so they are comparable within one response and not across responses for different
 * ranges: what a consumer relies on is the pairing within a response. Resolving a request to
 * versions is here too, so that "latest" and a schema id mean the same thing on both sides;
 * turning a failure into an error is left to the caller, which knows its own error model.
 */
public final class ProvenanceHistory {

  private ProvenanceHistory() {
  }

  /**
   * When {@code version} was registered, in epoch millis: its {@code createTs} where the registry
   * recorded one apart from its {@code ts} (a rewritten record, as after a soft delete), else its
   * {@code ts}; null where neither is known.
   */
  public static Long registeredAt(SchemaMetadata version) {
    return version.getCreateTimestamp() != null
        ? version.getCreateTimestamp() : version.getTimestamp();
  }

  private static boolean isDeleted(SchemaMetadata version) {
    return Boolean.TRUE.equals(version.getDeleted());
  }

  /**
   * The latest version not soft-deleted, as {@code "latest"} means everywhere else.
   */
  public static OptionalInt latestVersion(List<SchemaMetadata> history) {
    for (int i = history.size() - 1; i >= 0; i--) {
      if (!isDeleted(history.get(i))) {
        return OptionalInt.of(history.get(i).getVersion());
      }
    }
    return OptionalInt.empty();
  }

  /**
   * {@code version} if the history holds it.
   */
  public static OptionalInt version(List<SchemaMetadata> history, int version) {
    for (SchemaMetadata entry : history) {
      if (entry.getVersion() == version) {
        return OptionalInt.of(version);
      }
    }
    return OptionalInt.empty();
  }

  /**
   * The version carrying schema id {@code schemaId}. A schema re-registered after a soft delete
   * can sit under more than one version; the latest is taken.
   */
  public static OptionalInt versionCarrying(List<SchemaMetadata> history, int schemaId) {
    for (int i = history.size() - 1; i >= 0; i--) {
      if (history.get(i).getId() == schemaId) {
        return OptionalInt.of(history.get(i).getVersion());
      }
    }
    return OptionalInt.empty();
  }

  /**
   * The whole history's provenance, from each version's logical type, as {@link #logicalTypesOf}
   * gives them.
   *
   * @param history the subject's versions, in version order; each one's schema type decides the
   *     identity rules it is matched by
   * @param logicalTypes each version's logical type, in the same order as {@code history}
   * @throws RecursiveTypeException if a version's schema refers to itself
   * @throws AmbiguousProvenanceException if the history's names and aliases do not determine one
   *     identity per location
   * @throws TooManyLocationsException if a version has more locations than provenance computes
   */
  public static SchemaProvenance compute(String subject, List<SchemaMetadata> history,
      List<LogicalType> logicalTypes) {
    return compute(subject, history, logicalTypes, ProvenanceAlgorithm.LATEST);
  }

  /**
   * As {@link #compute(String, List, List, ProvenanceAlgorithm)}, by the version named
   * {@code algorithm}: a version's name, {@link ProvenanceAlgorithm#LATEST_NAME} or none for the
   * latest, or {@link ProvenanceAlgorithm#DYNAMIC_NAME}, under which each version is matched to
   * its predecessor by the version effective when it was registered. A version of the algorithm
   * changes the matching rules, never the logical type's edition, which is the Metastore's.
   *
   * @throws IllegalArgumentException if no version has that name
   * @throws UnsupportedProvenanceAlgorithmException if a dynamic range's transitions fall to an
   *     algorithm other than v1
   */
  public static SchemaProvenance compute(String subject, List<SchemaMetadata> history,
      List<LogicalType> logicalTypes, String algorithm) {
    if (!ProvenanceAlgorithm.isDynamic(algorithm)) {
      return compute(subject, history, logicalTypes, ProvenanceAlgorithm.of(algorithm));
    }
    // The first version is matched to no predecessor, so only the later ones name an algorithm.
    for (SchemaMetadata entry : history.subList(Math.min(1, history.size()), history.size())) {
      ProvenanceAlgorithm effective = ProvenanceAlgorithm.effectiveAt(registeredAt(entry));
      if (effective != ProvenanceAlgorithm.V1) {
        // Until each transition is matched by its own algorithm.
        throw new UnsupportedProvenanceAlgorithmException("Dynamic provenance by "
            + effective.getName() + " is not supported yet");
      }
    }
    SchemaProvenance provenance = compute(subject, history, logicalTypes, ProvenanceAlgorithm.V1);
    provenance.setAlgorithm(ProvenanceAlgorithm.DYNAMIC_NAME);
    return provenance;
  }

  /**
   * As {@link #compute(String, List, List)}, by the named version of the algorithm, which the
   * result records.
   */
  public static SchemaProvenance compute(String subject, List<SchemaMetadata> history,
      List<LogicalType> logicalTypes, ProvenanceAlgorithm algorithm) {
    switch (algorithm) {
      case V1:
        return computeV1(subject, history, logicalTypes);
      default:
        throw new IllegalArgumentException("Unsupported provenance algorithm " + algorithm);
    }
  }

  private static SchemaProvenance computeV1(String subject, List<SchemaMetadata> history,
      List<LogicalType> logicalTypes) {
    List<SchemaType> schemaTypes = new ArrayList<>(history.size());
    List<Integer> ids = new ArrayList<>(history.size());
    List<Integer> versions = new ArrayList<>(history.size());
    for (SchemaMetadata entry : history) {
      schemaTypes.add(SchemaType.of(entry.getSchemaType()));
      ids.add(entry.getId());
      versions.add(entry.getVersion());
    }
    ProvenanceReport report;
    try {
      report = ProvenanceComputer.report(schemaTypes, logicalTypes);
    } catch (AmbiguousProvenanceException e) {
      // The computer counts versions from 0 within the history; a caller knows them by number.
      int index = e.version();
      throw index >= 0 && index < versions.size() ? e.withVersion(versions.get(index)) : e;
    }
    SchemaProvenance encoded = SchemaProvenanceEncoder.encode(subject, report, ids, versions);
    encoded.setAlgorithm(ProvenanceAlgorithm.V1.getName());
    return encoded;
  }

  /**
   * Each of {@code schemas} as {@link #logicalTypeOf} converts it.
   *
   * @throws io.confluent.kafka.schemaregistry.type.logical.ValidationException if a schema has no
   *     logical form
   */
  public static List<LogicalType> logicalTypesOf(List<ParsedSchema> schemas,
      boolean includeMultipleMessages) {
    List<LogicalType> logicalTypes = new ArrayList<>(schemas.size());
    for (ParsedSchema schema : schemas) {
      logicalTypes.add(logicalTypeOf(schema, includeMultipleMessages));
    }
    return logicalTypes;
  }

  /**
   * The logical type provenance is computed on: edition V1, the one the Metastore's columns follow.
   * Only the JSON reader differs by edition, keeping a bare one-branch union and naming union
   * branches by position. With {@code includeMultipleMessages}, a Protobuf schema is rooted at a
   * synthetic struct over all its top-level messages; other formats have one root regardless.
   */
  public static LogicalType logicalTypeOf(ParsedSchema schema, boolean includeMultipleMessages) {
    if (schema instanceof JsonSchema) {
      return JsonToLogicalTypeConverter.toLogicalType((JsonSchema) schema, LogicalTypeVersion.V1);
    }
    return includeMultipleMessages && schema instanceof ProtobufSchema
        ? ProtoToLogicalTypeConverter.toLogicalType((ProtobufSchema) schema, true)
        : LogicalTypeConversion.toLogicalType(schema);
  }

  /**
   * The part of {@code history} a range covers: every version from the lower end to the higher,
   * soft-deleted ones included, in version order. Provenance is computed over exactly this.
   */
  public static List<SchemaMetadata> range(List<SchemaMetadata> history, int from, int to) {
    int low = Math.min(from, to);
    int high = Math.max(from, to);
    return history.stream()
        .filter(e -> e.getVersion() >= low && e.getVersion() <= high)
        .collect(Collectors.toList());
  }

  /**
   * The versions a request asked for, from the whole history: both ends, or everything between
   * them. Everything is shared with {@code whole}, which is never modified.
   */
  public static SchemaProvenance slice(SchemaProvenance whole, int from, int to,
      boolean includeInterior) {
    int low = Math.min(from, to);
    int high = Math.max(from, to);
    List<ProvenanceVersion> versions = whole.getVersions().stream()
        .filter(v -> includeInterior
            ? v.getVersion() >= low && v.getVersion() <= high
            : v.getVersion() == low || v.getVersion() == high)
        .collect(Collectors.toList());
    return new SchemaProvenance(whole.getSubject(), whole.getAlgorithm(), versions);
  }
}
