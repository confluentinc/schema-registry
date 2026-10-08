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
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceAlgorithm;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.TypeTooDeepException;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
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
   * The provenance of {@code history}, every version reported, by the default algorithm.
   *
   * @see #compute(String, List, List, boolean, boolean, String)
   */
  public static SchemaProvenance compute(String subject, List<SchemaMetadata> history,
      List<? extends ParsedSchemaHolder> schemas, boolean includeMultipleMessages) {
    return compute(subject, history, schemas, includeMultipleMessages, true, null);
  }

  /**
   * The provenance of {@code history}, by the version of the algorithm named {@code algorithm}: a
   * version's name, none for {@link ProvenanceAlgorithm#DEFAULT}, or
   * {@link ProvenanceAlgorithm#DYNAMIC_NAME}, under which each version is matched to its
   * predecessor by the version effective when it was registered. A version of the algorithm
   * changes the matching rules, never the logical type's edition, which is the Metastore's.
   *
   * <p>Each version is parsed and converted, as {@link #logicalTypeOf} converts it, only when the
   * computation reaches it, and none is held past the next: a range costs the memory of its report,
   * not of all its versions. So a failure is the first in version order.
   *
   * @param history the subject's versions, in version order; each one's schema type decides the
   *     identity rules it is matched by
   * @param schemas each version's schema, in the same order as {@code history}
   * @param includeInterior whether every version is reported, or only the first and last; every
   *     version is computed either way
   * @throws RecursiveTypeException if a version's schema refers to itself
   * @throws AmbiguousProvenanceException if the history's names and aliases do not determine one
   *     identity per location
   * @throws TooManyLocationsException if a version has more locations than provenance computes
   * @throws TypeTooDeepException if a version nests its types too deep, as converted or walked
   * @throws IllegalArgumentException if no version of the algorithm has that name, or there is not
   *     one schema per version
   * @throws UnsupportedProvenanceAlgorithmException if a dynamic range's transitions fall to an
   *     algorithm other than v1
   * @throws io.confluent.kafka.schemaregistry.type.logical.ValidationException if a schema has no
   *     logical form
   */
  public static SchemaProvenance compute(String subject, List<SchemaMetadata> history,
      List<? extends ParsedSchemaHolder> schemas, boolean includeMultipleMessages,
      boolean includeInterior, String algorithm) {
    if (schemas.size() != history.size()) {
      throw new IllegalArgumentException("Expected one schema per version, got " + schemas.size()
          + " schemas for " + history.size() + " versions");
    }
    IntFunction<LogicalType> versionAt =
        i -> logicalTypeOf(schemas.get(i).schema(), includeMultipleMessages);
    int last = history.size() - 1;
    IntPredicate reported = includeInterior ? i -> true : i -> i == 0 || i == last;
    if (!ProvenanceAlgorithm.isDynamic(algorithm)) {
      return compute(subject, history, versionAt, reported, ProvenanceAlgorithm.of(algorithm));
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
    SchemaProvenance provenance =
        compute(subject, history, versionAt, reported, ProvenanceAlgorithm.V1);
    provenance.setAlgorithm(ProvenanceAlgorithm.DYNAMIC_NAME);
    return provenance;
  }

  private static SchemaProvenance compute(String subject, List<SchemaMetadata> history,
      IntFunction<LogicalType> versionAt, IntPredicate reported, ProvenanceAlgorithm algorithm) {
    switch (algorithm) {
      case V1:
        return computeV1(subject, history, versionAt, reported);
      default:
        throw new IllegalArgumentException("Unsupported provenance algorithm " + algorithm);
    }
  }

  // The registry's number for the computer's version index, or -1 if there is none.
  private static int versionNumber(List<Integer> versions, int index) {
    return index >= 0 && index < versions.size() ? versions.get(index) : -1;
  }

  private static SchemaProvenance computeV1(String subject, List<SchemaMetadata> history,
      IntFunction<LogicalType> versionAt, IntPredicate reported) {
    List<SchemaType> schemaTypes = new ArrayList<>(history.size());
    List<Integer> ids = new ArrayList<>(history.size());
    List<Integer> versions = new ArrayList<>(history.size());
    for (SchemaMetadata entry : history) {
      // Schema Registry leaves an Avro schema's type unset.
      schemaTypes.add(SchemaType.of(
          entry.getSchemaType() != null ? entry.getSchemaType() : AvroSchema.TYPE));
      ids.add(entry.getId());
      versions.add(entry.getVersion());
    }
    // The version last converted is the one being converted or walked when either fails.
    int[] reached = {-1};
    IntFunction<LogicalType> tracked = i -> {
      reached[0] = i;
      return versionAt.apply(i);
    };
    ProvenanceReport report;
    try {
      report = ProvenanceComputer.report(schemaTypes, tracked, reported);
    } catch (RecursiveTypeException e) {
      throw e.atVersion(versionNumber(versions, reached[0]));
    } catch (TypeTooDeepException e) {
      int number = versionNumber(versions, reached[0]);
      throw number < 0 ? e
          : new TypeTooDeepException("Version " + number + ": " + e.getMessage(), e);
    } catch (ValidationException e) {
      int number = versionNumber(versions, reached[0]);
      throw number < 0 ? e
          : new ValidationException("Version " + number + ": " + e.getMessage(), e);
    } catch (AmbiguousProvenanceException e) {
      // The computer counts versions from 0 within the history; a caller knows them by number.
      int index = e.version();
      throw index >= 0 && index < versions.size() ? e.withVersion(versions.get(index)) : e;
    } catch (TooManyLocationsException e) {
      int index = e.version();
      throw index >= 0 && index < versions.size() ? e.withVersion(versions.get(index)) : e;
    }
    SchemaProvenance encoded = SchemaProvenanceEncoder.encode(subject, report, ids, versions);
    encoded.setAlgorithm(ProvenanceAlgorithm.V1.getName());
    return encoded;
  }

  /**
   * The locations of one version, as any range holding it reports them: a location's path, names
   * and kind follow from its own schema, never its history. Its pids are allocated from it alone.
   *
   * @throws RecursiveTypeException if the schema refers to itself
   * @throws TooManyLocationsException if it has more locations than provenance computes
   * @throws TypeTooDeepException if it nests its types too deep, as converted or walked
   * @throws io.confluent.kafka.schemaregistry.type.logical.ValidationException if it has no
   *     logical form
   */
  public static ProvenanceVersion locations(String subject, int version, int schemaId,
      ParsedSchema schema, boolean includeMultipleMessages) {
    // Only the type, id and version of the metadata are read.
    SchemaMetadata metadata =
        new SchemaMetadata(schemaId, version, schema.schemaType(), schema.references(), "");
    return compute(subject, Collections.singletonList(metadata),
        held(Collections.singletonList(schema)), includeMultipleMessages).getVersions().get(0);
  }

  /**
   * Schemas already parsed, as holders for {@link #compute}: each is still converted only when the
   * computation reaches it.
   */
  public static List<ParsedSchemaHolder> held(List<? extends ParsedSchema> schemas) {
    return schemas.stream().map(SimpleParsedSchemaHolder::new).collect(Collectors.toList());
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
