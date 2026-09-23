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
 * <p>Provenance is computed over the whole history from its first version and then sliced to the
 * request: allocation is anchored at the first version so that every caller agrees on the ids, and
 * a version's ids depend only on the versions before it. Resolving a request to versions is here
 * too, so that "latest" and a schema id mean the same thing on both sides; turning a failure into
 * an error is left to the caller, which knows its own error model.
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
    return SchemaProvenanceEncoder.encode(
        subject, ProvenanceComputer.report(logicalTypes, policies), ids, versions, true);
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
    return new SchemaProvenance(whole.getSubject(), versions);
  }

  private static ProvenanceVersion withoutNames(ProvenanceVersion version) {
    List<ProvenanceField> fields = version.getFields().stream()
        .map(m -> new ProvenanceField(m.getPath(), null, m.getPid(), m.getDefaultValue()))
        .collect(Collectors.toList());
    return new ProvenanceVersion(version.getVersion(), version.getId(), fields);
  }
}
