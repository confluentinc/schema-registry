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

package io.confluent.kafka.schemaregistry.type.logical.common;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.Schema;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mutable context for converting a format-specific schema to logical type {@link Schema}.
 * Wraps the input {@link ParsedSchema} and accumulates conversion-time state
 * (named types — both local and lazy-promoted externals — plus union branch
 * metadata and cycle tracking).
 *
 * <p><b>Named types vs externals.</b> {@link #putNamedType} populates the unified
 * map fed to {@link io.confluent.kafka.schemaregistry.type.logical.LogicalType#getNamedTypes}.
 * Entries that originated from an external schema are also marked via
 * {@link #addExternalType} so the resulting LT can distinguish them via
 * {@link io.confluent.kafka.schemaregistry.type.logical.LogicalType#getExternalTypes}.
 * Field handlers consult {@link #isExternalType} to decide whether a
 * NAMED_TYPE_REF body should be lazy-promoted as external (and re-emitted as
 * an import on write) or treated as locally defined.
 *
 * @param <T> the type used for cycle detection (e.g., org.apache.avro.Schema or String)
 */
public final class ToLogicalContext<T> extends CycleContext<T> {

  private final ParsedSchema parsedSchema;
  private Map<String, Object> unionMetadata;
  private final Map<String, Schema> namedTypes = new LinkedHashMap<>();
  private final Set<String> externalTypes = new LinkedHashSet<>();
  // typeName → original $ref URI for synthetic-wrapper externals (JSON only).
  // Populated by the JSON reader when it generates Ref<N>-style names for
  // non-canonical $refs; consumed by the JSON writer to emit a local
  // `#/$defs/<name>` passthrough whose body is the original URI.
  private final Map<String, String> externalImports = new LinkedHashMap<>();
  // Reverse map of externalImports: URI → already-assigned typeName. Used to
  // dedupe Ref<N> assignment so the same URI seen in multiple field positions
  // resolves to a single synthesized name.
  private final Map<String, String> assignedRefNameByUri = new LinkedHashMap<>();
  // Per-LT counter for Ref<N> synthesis. Bumps past names already in
  // namedTypes (populated by the active schema's $defs walk before the body
  // walk runs) so a synthesized name never silently shadows an authored one.
  private int nextRefIndex = 1;

  public ToLogicalContext(
      ParsedSchema parsedSchema, Map<String, Object> unionMetadata) {
    this.parsedSchema = parsedSchema;
    this.unionMetadata = unionMetadata != null ? unionMetadata : Map.of();
    // Seed externals from the SR reference list. Each entry's `name` is the
    // import string used in the schema text — for proto enum-only files
    // (which have no top-level messages and so don't get walked by the
    // reader's pre-walk) this is the only signal that the named type is
    // external. Per-format readers add MORE FQNs via addExternalType during
    // their own pre-walks (e.g., the proto reader walks each external file's
    // FileDescriptor to register every message/enum FQN).
    if (parsedSchema != null && parsedSchema.references() != null) {
      for (SchemaReference ref : parsedSchema.references()) {
        externalTypes.add(ref.getName());
      }
    }
  }

  public ToLogicalContext(ParsedSchema parsedSchema) {
    this(parsedSchema, Map.of());
  }

  public ParsedSchema getParsedSchema() {
    return parsedSchema;
  }

  public List<SchemaReference> getReferences() {
    return parsedSchema != null && parsedSchema.references() != null
        ? parsedSchema.references() : List.of();
  }

  public Map<String, Object> getUnionMetadata() {
    return unionMetadata;
  }

  public void setUnionMetadata(Map<String, Object> unionMetadata) {
    this.unionMetadata = unionMetadata != null ? unionMetadata : Map.of();
  }

  public Map<String, Schema> getNamedTypes() {
    return namedTypes;
  }

  public void putNamedType(String name, Schema schema) {
    namedTypes.put(name, schema);
  }

  public boolean hasNamedType(String name) {
    return namedTypes.containsKey(name);
  }

  /**
   * Mark {@code name} as an externally-defined named type (its body originated
   * from a {@code resolvedReferences} entry rather than from the active
   * schema). Used both at pre-walk time (to flag what's external) and at
   * field-handler time (to propagate external status into the LT's
   * {@code externalTypes} set).
   */
  public void addExternalType(String name) {
    externalTypes.add(name);
  }

  public boolean isExternalType(String name) {
    return externalTypes.contains(name);
  }

  public Set<String> getExternalTypes() {
    return externalTypes;
  }

  /**
   * Synthesize (or look up) a {@code Ref<N>} typeName for a non-canonical
   * {@code $ref} URI. Same URI returns the same name; new URIs get the next
   * available index that isn't already in {@code namedTypes}. The returned
   * name is automatically registered as external and bound to {@code uri} via
   * {@link #getExternalImports}.
   */
  public String synthesizeRefName(String uri) {
    String existing = assignedRefNameByUri.get(uri);
    if (existing != null) {
      return existing;
    }
    String name;
    do {
      name = "Ref" + nextRefIndex++;
    } while (namedTypes.containsKey(name));
    assignedRefNameByUri.put(uri, name);
    externalImports.put(name, uri);
    externalTypes.add(name);
    return name;
  }

  /**
   * Direct registration for synthetic externals discovered from the active
   * schema's $defs (entries carrying the {@code logical.ref} marker). Caller
   * has the typeName and URI already; we just record the binding and mark
   * the name as external.
   */
  public void registerSyntheticExternal(String name, String uri) {
    if (assignedRefNameByUri.putIfAbsent(uri, name) == null) {
      // Keep the synthesizer counter past any user-assigned Ref<N> name so
      // future synthesis doesn't collide with what we just registered.
      if (name.startsWith("Ref")) {
        try {
          int n = Integer.parseInt(name.substring(3));
          if (n >= nextRefIndex) {
            nextRefIndex = n + 1;
          }
        } catch (NumberFormatException ignored) {
          // not a Ref<N> — leave the counter alone
        }
      }
    }
    externalImports.put(name, uri);
    externalTypes.add(name);
  }

  public Map<String, String> getExternalImports() {
    return externalImports;
  }
}
