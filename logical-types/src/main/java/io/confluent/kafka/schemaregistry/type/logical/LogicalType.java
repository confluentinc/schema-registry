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

package io.confluent.kafka.schemaregistry.type.logical;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Represents a logical type with its root schema, locally-defined named types,
 * and external schema references. Per-field default values live on
 * {@link Schema.Field}; a path-keyed mirror is also exposed via
 * {@link #getDefaultValues()} for callers that need positional access (e.g.,
 * Connect-style serialization).
 *
 * <p>Default-value path semantics (matching the Flink converters):
 * the key is a list of indices that walks from the root into the field.
 * For {@code STRUCT(a INT, b STRUCT(b1 STRING))}, {@code a}'s default is at
 * {@code [0]} and {@code b.b1}'s is at {@code [1, 0]}. ARRAY elements use
 * index {@code 0}; MAP key/value use {@code 0}/{@code 1}.
 */
public class LogicalType {

  private final String namespace;
  private final Schema rootSchema;
  private final Map<String, Schema> namedTypes;
  private final Set<String> externalTypes;
  private final Map<String, String> externalImports;
  private final List<SchemaReference> references;
  private final Map<String, String> resolvedReferences;
  private final Map<List<Integer>, Object> defaultValues;

  public LogicalType(final Schema rootSchema) {
    this(null, rootSchema, Map.of(), Set.of(), Map.of(),
        List.of(), Map.of(), Map.of());
  }

  public LogicalType(
      final Schema rootSchema,
      final Map<String, Schema> namedTypes) {
    this(null, rootSchema, namedTypes, Set.of(), Map.of(),
        List.of(), Map.of(), Map.of());
  }

  public LogicalType(
      final Schema rootSchema,
      final Map<String, Schema> namedTypes,
      final List<SchemaReference> references,
      final Map<String, String> resolvedReferences) {
    this(null, rootSchema, namedTypes, Set.of(), Map.of(),
        references, resolvedReferences, Map.of());
  }

  public LogicalType(
      final String namespace,
      final Schema rootSchema,
      final Map<String, Schema> namedTypes,
      final List<SchemaReference> references,
      final Map<String, String> resolvedReferences) {
    this(namespace, rootSchema, namedTypes, Set.of(), Map.of(),
        references, resolvedReferences, Map.of());
  }

  public LogicalType(
      final String namespace,
      final Schema rootSchema,
      final Map<String, Schema> namedTypes,
      final List<SchemaReference> references,
      final Map<String, String> resolvedReferences,
      final Map<List<Integer>, Object> defaultValues) {
    this(namespace, rootSchema, namedTypes, Set.of(), Map.of(),
        references, resolvedReferences, defaultValues);
  }

  public LogicalType(
      final String namespace,
      final Schema rootSchema,
      final Map<String, Schema> namedTypes,
      final Set<String> externalTypes,
      final List<SchemaReference> references,
      final Map<String, String> resolvedReferences,
      final Map<List<Integer>, Object> defaultValues) {
    this(namespace, rootSchema, namedTypes, externalTypes, Map.of(),
        references, resolvedReferences, defaultValues);
  }

  public LogicalType(
      final String namespace,
      final Schema rootSchema,
      final Map<String, Schema> namedTypes,
      final Set<String> externalTypes,
      final Map<String, String> externalImports,
      final List<SchemaReference> references,
      final Map<String, String> resolvedReferences,
      final Map<List<Integer>, Object> defaultValues) {
    // Treat empty string as no namespace, so callers don't have to.
    this.namespace = (namespace == null || namespace.isEmpty()) ? null : namespace;
    this.rootSchema = requireNonNull(rootSchema, "rootSchema");
    this.namedTypes = namedTypes != null
        ? Collections.unmodifiableMap(namedTypes) : Map.of();
    this.externalTypes = externalTypes != null
        ? Collections.unmodifiableSet(externalTypes) : Set.of();
    this.externalImports = externalImports != null
        ? Collections.unmodifiableMap(externalImports) : Map.of();
    this.references = references != null
        ? Collections.unmodifiableList(references) : List.of();
    this.resolvedReferences = resolvedReferences != null
        ? Collections.unmodifiableMap(resolvedReferences) : Map.of();
    this.defaultValues = defaultValues != null
        ? Collections.unmodifiableMap(defaultValues) : Map.of();
  }

  public Schema getRootSchema() {
    return rootSchema;
  }

  /**
   * Locally-defined named types, keyed by their qualified name.
   *
   * <p><b>Nesting convention</b>: a key like {@code Outer.Inner} where
   * {@code Outer} is also a key in this map indicates that {@code Inner} is
   * nested inside {@code Outer}. The dot acts as the nesting separator. When a
   * key has dots but no prefix matches another key, the dots are interpreted
   * as a namespace prefix and the type is top-level. See {@link #parentOf} and
   * {@link #nestingTree} for accessors that interpret the convention.
   *
   * <p>Only the Protobuf writer emits the nesting structurally (as proto's
   * native nested messages). Avro and JSON Schema emit dotted-name types as
   * flat top-level entries (their full names just contain dots).
   */
  public Map<String, Schema> getNamedTypes() {
    return namedTypes;
  }

  /**
   * FQNs in {@link #namedTypes} whose body originated from an external schema
   * (an entry in {@link #resolvedReferences}) rather than from a locally-
   * defined type. Writers use this set to decide whether to re-emit the type
   * as an inline definition (for entries NOT in this set) or as an import /
   * {@code $ref} (for entries IN this set).
   *
   * <p>The set is populated by readers when they lazily promote external
   * bodies into {@code namedTypes}, and by the DDL visitor by inferring
   * external-ness from usage (any {@code NAMED_TYPE_REF} FQN not declared
   * locally).
   */
  public Set<String> getExternalTypes() {
    return externalTypes;
  }

  /**
   * True iff {@code fqn} is a key in {@link #getExternalTypes}.
   */
  public boolean isExternal(String fqn) {
    return externalTypes.contains(fqn);
  }

  /**
   * Per-FQN binding from a synthetic-wrapper external typeName to the original
   * {@code $ref} URI it stands in for. Populated by the JSON reader when it
   * synthesizes {@code Ref<N>}-style names for non-canonical $refs (whole-doc,
   * arbitrary JSON Pointers, and other shapes that don't extract cleanly to a
   * {@code $defs} key), and by the DDL visitor when it parses
   * {@code DECLARE x FOR '<uri>'} statements.
   *
   * <p>The JSON writer consults this map to emit those externals as a local
   * {@code #/$defs/<name>} reference whose body is a single
   * {@code {"$ref": "<uri>", "logical.ref": true}} passthrough — keeping the
   * indirection scoped to the active document instead of requiring a
   * separately-published wrapper subject. Empty for LT-canonical workspaces.
   *
   * <p>Avro and Proto writers reject any LT carrying entries here: synthetic
   * wrappers are JSON-specific (the URI-vs-name impedance only exists for
   * JSON's $ref model). Use a fresh, format-appropriate LT instead.
   *
   * <p><b>Independence from {@link #getReferences()}.</b> {@code externalImports}
   * carries wire-format binding (typeName → URI used in {@code $ref}); the
   * {@code references} list carries SR coordinates (subject + version used by
   * the SR client to fetch external content). They are populated independently
   * — DDL's {@code DECLARE} populates {@code externalImports} but never the
   * references list. To produce an SR-publishable LT from DDL, attach a
   * {@code references} list (with matching {@code resolvedReferences}
   * content) separately after the visitor runs.
   */
  public Map<String, String> getExternalImports() {
    return externalImports;
  }

  /**
   * Subset of {@link #getNamedTypes} that are NOT in {@link #getExternalTypes}
   * — the entries the user authored locally. Writers use this accessor to
   * iterate "the local stuff to inline-emit" without re-emitting external
   * bodies that should appear as imports.
   */
  public Map<String, Schema> getLocalNamedTypes() {
    if (externalTypes.isEmpty()) {
      return namedTypes;
    }
    Map<String, Schema> local = new java.util.LinkedHashMap<>();
    for (Map.Entry<String, Schema> e : namedTypes.entrySet()) {
      if (!externalTypes.contains(e.getKey())) {
        local.put(e.getKey(), e.getValue());
      }
    }
    return Collections.unmodifiableMap(local);
  }

  public List<SchemaReference> getReferences() {
    return references;
  }

  public Map<String, String> getResolvedReferences() {
    return resolvedReferences;
  }

  /** The document-level namespace, or null if none. */
  public String getNamespace() {
    return namespace;
  }

  /**
   * Names of locally-defined types that are not reachable from the root via
   * the named-type-ref graph. These are dropped by single-root encoders such
   * as Avro; multi-root encoders (Protobuf top-level messages, JSON $defs)
   * may still emit them.
   */
  public Set<String> findOrphanNamedTypes() {
    if (namedTypes.isEmpty()) {
      return Set.of();
    }
    Set<String> reachable = new LinkedHashSet<>();
    Deque<Schema> work = new ArrayDeque<>();
    work.add(rootSchema);
    while (!work.isEmpty()) {
      Schema cur = work.pop();
      Set<String> refs = new LinkedHashSet<>();
      collectNamedRefs(cur, refs);
      for (String name : refs) {
        if (reachable.add(name)) {
          Schema body = namedTypes.get(name);
          if (body != null) {
            work.push(body);
          }
        }
      }
    }
    Set<String> orphans = new LinkedHashSet<>(namedTypes.keySet());
    orphans.removeAll(reachable);
    return orphans;
  }

  /**
   * Returns the parent named-type key of {@code key} under the dotted-path
   * nesting convention, or {@code null} if {@code key} is top-level.
   *
   * <p>The parent is the longest dotted prefix of {@code key} that itself
   * exists in {@link #namedTypes}. Example: with named types {@code Outer}
   * and {@code Outer.Inner}, {@code parentOf("Outer.Inner")} returns
   * {@code "Outer"}; {@code parentOf("Outer")} returns {@code null} (no
   * matching prefix → top-level).
   *
   * <p>Dots that do not correspond to a defined parent are namespace dots
   * (top-level). So {@code parentOf("com.example.Foo")} with no
   * {@code com.example} type defined returns {@code null} — the dots are
   * just the namespace prefix.
   */
  public String parentOf(String key) {
    return parentOf(key, namedTypes.keySet());
  }

  /**
   * Static form of {@link #parentOf(String)} for use during validation
   * before a {@link LogicalType} instance exists (e.g., inside the visitor).
   */
  public static String parentOf(String key, Set<String> definedTypes) {
    if (key == null) {
      return null;
    }
    int dot = key.lastIndexOf('.');
    while (dot > 0) {
      String candidate = key.substring(0, dot);
      if (definedTypes.contains(candidate)) {
        return candidate;
      }
      dot = key.lastIndexOf('.', dot - 1);
    }
    return null;
  }

  /**
   * Walks parent links via {@link #parentOf} until reaching a top-level type.
   * Returns {@code key} itself if {@code key} is already top-level.
   */
  public String topLevelAncestor(String key) {
    String cur = key;
    String parent;
    while ((parent = parentOf(cur)) != null) {
      cur = parent;
    }
    return cur;
  }

  /**
   * Returns true if {@code key} represents a nested type — i.e. it has a
   * parent in {@link #namedTypes} per the dotted-path convention.
   */
  public boolean isNested(String key) {
    return parentOf(key) != null;
  }

  /**
   * Group named types by parent. The map keys cover every entry of
   * {@link #namedTypes}; the values are the immediate children (one nesting
   * level deep). Top-level types appear under the {@code null} parent.
   *
   * <p>Used by writers that emit nesting natively (Protobuf): walk the entry
   * for {@code null} (top-level messages), then recursively walk each child's
   * entry to nest its children inside.
   */
  public Map<String, List<String>> nestingTree() {
    Map<String, List<String>> tree = new java.util.LinkedHashMap<>();
    tree.put(null, new java.util.ArrayList<>());
    for (String name : namedTypes.keySet()) {
      tree.put(name, new java.util.ArrayList<>());
    }
    for (String name : namedTypes.keySet()) {
      String parent = parentOf(name);
      tree.get(parent).add(name);
    }
    return tree;
  }

  /**
   * Walk {@code schema} and collect the qualified names of every
   * {@code NAMED_TYPE_REF} encountered. Recurses through structural composites
   * (struct, union, array, multiset, map) but does <em>not</em> follow the
   * named-type-ref itself — callers do that via {@link #namedTypes}.
   */
  public static void collectNamedRefs(Schema schema, Set<String> sink) {
    if (schema == null) {
      return;
    }
    switch (schema.getType()) {
      case NAMED_TYPE_REF:
        sink.add(schema.getQualifiedName());
        return;
      case STRUCT:
        for (Schema.Field f : schema.getFields()) {
          collectNamedRefs(f.getSchema(), sink);
        }
        return;
      case UNION:
        for (Schema.UnionBranch b : schema.getBranches()) {
          collectNamedRefs(b.getSchema(), sink);
        }
        return;
      case ARRAY:
      case MULTISET:
        collectNamedRefs(schema.getElementType(), sink);
        return;
      case MAP:
        collectNamedRefs(schema.getKeyType(), sink);
        collectNamedRefs(schema.getValueType(), sink);
        return;
      default:
        // ENUM and primitives have no nested type references.
    }
  }

  /**
   * True iff {@code fqn} is part of a cycle in the named-type-ref graph
   * formed by {@link #namedTypes} — i.e., a self-reference or mutual
   * recursion through other peers.
   */
  public boolean isCyclic(String fqn) {
    return isCyclic(fqn, namedTypes);
  }

  /**
   * Static form of {@link #isCyclic(String)} for use during conversion when
   * a builder map is being populated and the {@link LogicalType} instance
   * does not yet exist.
   */
  public static boolean isCyclic(String fqn, Map<String, Schema> namedTypes) {
    Schema body = namedTypes.get(fqn);
    if (body == null) {
      return false;
    }
    Set<String> seen = new LinkedHashSet<>();
    Deque<String> queue = new ArrayDeque<>();
    Set<String> firstHopRefs = new LinkedHashSet<>();
    collectNamedRefs(body, firstHopRefs);
    queue.addAll(firstHopRefs);
    while (!queue.isEmpty()) {
      String name = queue.poll();
      if (name.equals(fqn)) {
        return true;
      }
      if (!seen.add(name)) {
        continue;
      }
      Schema next = namedTypes.get(name);
      if (next == null) {
        continue;
      }
      Set<String> refs = new LinkedHashSet<>();
      collectNamedRefs(next, refs);
      queue.addAll(refs);
    }
    return false;
  }

  /**
   * Path-keyed map of field-default values collected during conversion.
   *
   * <p>Each key is a list of zero-based indices that walks from the root schema
   * into the field carrying the default. Each container type contributes
   * indices to the path according to a fixed convention:
   *
   * <ul>
   *   <li><b>STRUCT:</b> the field's positional index. A struct with three
   *       fields contributes {@code 0}, {@code 1}, or {@code 2} for its first,
   *       second, or third field respectively.</li>
   *   <li><b>MAP:</b> {@code 0} for the key type, {@code 1} for the value type.</li>
   *   <li><b>MULTISET:</b> {@code 0} for the element type.</li>
   *   <li><b>ARRAY:</b> <i>format-dependent</i>. The Avro reader contributes
   *       {@code 0} for the element type; the Proto and JSON readers contribute
   *       no index (descending into the element type uses the parent path
   *       directly). See each {@code *ToLogicalTypeConverter}'s class-level
   *       Javadoc for the format-specific rule. (TODO: unify across formats.)</li>
   *   <li><b>Wrapper structs</b> (proto {@code _RepeatedWrapper},
   *       {@code _ElementWrapper}, {@code _OneofWrapper} marked with
   *       {@code flink.wrapped=true}) are <i>transparent</i>: they contribute no
   *       index — descending into the wrapped payload uses the parent path.
   *       This keeps default-value paths aligned with the structure of the
   *       returned schema (which doesn't surface the wrapper layer).</li>
   *   <li><b>Leaf types</b> (primitives, enum, named-type-ref) carry the value
   *       and do not extend the path.</li>
   * </ul>
   *
   * <p>Worked examples (assuming the root is a STRUCT):
   *
   * <pre>
   * STRUCT { a: INT (default 1) }
   *   → { [0] = 1 }
   *
   * STRUCT { a: INT, m: MAP&lt;STRING, INT&gt; (key default "k", value default 5) }
   *   → { [1, 0] = "k", [1, 1] = 5 }
   *
   * STRUCT { nested: STRUCT { value: STRUCT { a: INT (default 17), b: INT (default 18) } } }
   *   → { [0, 0, 0] = 17, [0, 0, 1] = 18 }
   *
   * STRUCT {
   *   arr: ARRAY&lt;STRUCT { value: STRUCT { a: INT (default 17), b: INT (default 18) } }&gt;
   * }
   *   → Proto:      { [0, 0, 0] = 17, [0, 0, 1] = 18 }      // ARRAY adds no index
   *   → Avro/JSON:  { [0, 0, 0, 0] = 17, [0, 0, 0, 1] = 18 } // ARRAY adds [0]
   *
   * STRUCT {
   *   wrappedMap: MAP&lt;BIGINT, BIGINT&gt; (proto-wrapped, key default 15, value default 16)
   * }
   *   → { [0, 0] = 15, [0, 1] = 16 }   // wrapper is transparent; just MAP key/value
   * </pre>
   */
  public Map<List<Integer>, Object> getDefaultValues() {
    return defaultValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LogicalType)) {
      return false;
    }
    LogicalType that = (LogicalType) o;
    return Objects.equals(rootSchema, that.rootSchema)
        && Objects.equals(namedTypes, that.namedTypes)
        && Objects.equals(externalTypes, that.externalTypes)
        && Objects.equals(externalImports, that.externalImports)
        && Objects.equals(references, that.references)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(defaultValues, that.defaultValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rootSchema, namedTypes, externalTypes, externalImports,
        references, namespace, defaultValues);
  }

  @Override
  public String toString() {
    return "LogicalType{"
        + "rootSchema=" + rootSchema
        + ", namedTypes=" + namedTypes
        + ", externalTypes=" + externalTypes
        + ", externalImports=" + externalImports
        + ", references=" + references
        + ", namespace=" + namespace
        + ", defaultValues=" + defaultValues
        + '}';
  }
}
