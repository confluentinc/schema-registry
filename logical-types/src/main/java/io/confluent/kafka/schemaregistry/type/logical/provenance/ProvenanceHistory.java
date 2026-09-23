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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
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

  /** One version of a subject's history, in version order. */
  public static final class Entry {

    private final int version;
    private final int schemaId;
    private final boolean deleted;

    public Entry(int version, int schemaId, boolean deleted) {
      this.version = version;
      this.schemaId = schemaId;
      this.deleted = deleted;
    }

    public int getVersion() {
      return version;
    }

    public int getSchemaId() {
      return schemaId;
    }

    public boolean isDeleted() {
      return deleted;
    }
  }

  /**
   * The latest version not soft-deleted, as {@code "latest"} means everywhere else.
   */
  public static OptionalInt latestVersion(List<Entry> history) {
    for (int i = history.size() - 1; i >= 0; i--) {
      if (!history.get(i).isDeleted()) {
        return OptionalInt.of(history.get(i).getVersion());
      }
    }
    return OptionalInt.empty();
  }

  /**
   * {@code version} if the history holds it.
   */
  public static OptionalInt version(List<Entry> history, int version) {
    for (Entry entry : history) {
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
  public static OptionalInt versionCarrying(List<Entry> history, int schemaId) {
    for (int i = history.size() - 1; i >= 0; i--) {
      if (history.get(i).getSchemaId() == schemaId) {
        return OptionalInt.of(history.get(i).getVersion());
      }
    }
    return OptionalInt.empty();
  }

  /**
   * The whole history's provenance, verbose, from each version's parsed schema.
   *
   * @param schemas each version's schema, in the same order as {@code history}
   * @throws RecursiveTypeException if a version's schema refers to itself
   * @throws io.confluent.kafka.schemaregistry.type.logical.ValidationException if a version's
   *     schema has no logical form
   */
  public static SchemaProvenance compute(String subject, List<Entry> history,
      List<ParsedSchema> schemas) {
    return compute(subject, history, schemas, false);
  }

  /**
   * As {@link #compute(String, List, List)}; with {@code includeMultipleMessages}, each Protobuf
   * version is rooted at a synthetic struct over all its top-level messages. Other formats have
   * one root regardless, and ignore it.
   */
  public static SchemaProvenance compute(String subject, List<Entry> history,
      List<ParsedSchema> schemas, boolean includeMultipleMessages) {
    return compute(subject, history, schemas, includeMultipleMessages, ProvenanceAlgorithm.LATEST);
  }

  /**
   * As {@link #compute(String, List, List, boolean)}, by the named version of the algorithm, which
   * the result records.
   */
  public static SchemaProvenance compute(String subject, List<Entry> history,
      List<ParsedSchema> schemas, boolean includeMultipleMessages, ProvenanceAlgorithm algorithm) {
    switch (algorithm) {
      case V1:
        return computeV1(subject, history, schemas, includeMultipleMessages);
      default:
        throw new IllegalArgumentException("Unsupported provenance algorithm " + algorithm);
    }
  }

  private static SchemaProvenance computeV1(String subject, List<Entry> history,
      List<ParsedSchema> schemas, boolean includeMultipleMessages) {
    List<LogicalType> logicalTypes = new ArrayList<>(history.size());
    List<IdentityPolicy> policies = new ArrayList<>(history.size());
    List<Integer> ids = new ArrayList<>(history.size());
    List<Integer> versions = new ArrayList<>(history.size());
    for (int i = 0; i < history.size(); i++) {
      ParsedSchema schema = schemas.get(i);
      logicalTypes.add(includeMultipleMessages && schema instanceof ProtobufSchema
          ? ProtoToLogicalTypeConverter.toLogicalType((ProtobufSchema) schema, true)
          : LogicalTypeConversion.toLogicalType(schema));
      policies.add(IdentityPolicy.forSchemaType(schema.schemaType()));
      ids.add(history.get(i).getSchemaId());
      versions.add(history.get(i).getVersion());
    }
    SchemaProvenance encoded = SchemaProvenanceEncoder.encode(
        subject, ProvenanceComputer.report(logicalTypes, policies), ids, versions, true);
    encoded.setAlgorithm(ProvenanceAlgorithm.V1.getName());
    return encoded;
  }

  /**
   * The part of {@code history} a range covers: every version from the lower end to the higher,
   * soft-deleted ones included, in version order. Provenance is computed over exactly this.
   */
  public static List<Entry> range(List<Entry> history, int from, int to) {
    int low = Math.min(from, to);
    int high = Math.max(from, to);
    return history.stream()
        .filter(e -> e.getVersion() >= low && e.getVersion() <= high)
        .collect(Collectors.toList());
  }

  /**
   * The versions a request asked for, from the whole history: both ends, or everything between
   * them. Names are dropped unless asked for; everything else is shared with {@code whole}, which
   * is never modified.
   */
  public static SchemaProvenance slice(SchemaProvenance whole, int from, int to,
      boolean includeInterior, boolean verbose) {
    int low = Math.min(from, to);
    int high = Math.max(from, to);
    List<ProvenanceVersion> versions = whole.getVersions().stream()
        .filter(v -> includeInterior
            ? v.getVersion() >= low && v.getVersion() <= high
            : v.getVersion() == low || v.getVersion() == high)
        .map(v -> verbose ? v : withoutNames(v))
        .collect(Collectors.toList());
    return new SchemaProvenance(whole.getSubject(), whole.getAlgorithm(), versions);
  }

  private static ProvenanceVersion withoutNames(ProvenanceVersion version) {
    List<ProvenanceField> fields = version.getFields().stream()
        .map(m -> new ProvenanceField(m.getPath(), null, m.getPid()))
        .collect(Collectors.toList());
    return new ProvenanceVersion(version.getVersion(), version.getId(), fields);
  }
}
