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

package io.confluent.kafka.serializers.provenance;

import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Which writer field feeds each reader field, from joining two versions of a {@link
 * SchemaProvenance} on the provenance id. Paths are the endpoint's inlined index paths; names are
 * each location in the native schema's own names, with {@code null} for an unnamed step; kinds
 * are what each location's type is, as the endpoint spells it.
 */
public final class ProvenanceMapping {

  private final Map<List<Integer>, List<Integer>> readerToWriter;
  private final Map<List<Integer>, List<Integer>> writerToReader;
  private final Map<List<Integer>, List<String>> readerNames;
  private final Map<List<Integer>, List<String>> writerNames;
  private final Map<List<Integer>, String> readerKinds;
  private final Map<List<Integer>, String> writerKinds;
  private final String readerRootKind;
  private final String writerRootKind;
  private final Map<List<String>, List<Integer>> readerAt;
  private final Map<List<String>, List<Integer>> writerAt;
  private final List<List<Integer>> readerPaths;
  private final List<List<Integer>> writerPaths;
  private final int writerId;
  private final int readerId;

  private ProvenanceMapping(ProvenanceVersion writer, ProvenanceVersion reader) {
    writerId = writer.getId();
    readerId = reader.getId();
    Map<Integer, List<Integer>> writerByPid = new HashMap<>();
    writerNames = new HashMap<>();
    writerKinds = new HashMap<>();
    writerRootKind = writer.getKind();
    writerAt = new HashMap<>();
    List<List<Integer>> written = new ArrayList<>();
    Set<Integer> writerPids = new HashSet<>();
    for (ProvenanceField field : writer.getFields()) {
      requirePid(field, writerPids, writerId);
      written.add(field.getPath());
      writerByPid.put(field.getPid(), field.getPath());
      index(field, writerNames, writerAt);
      if (field.getKind() != null) {
        writerKinds.put(field.getPath(), field.getKind());
      }
    }
    writerPaths = Collections.unmodifiableList(written);
    readerToWriter = new HashMap<>();
    writerToReader = new HashMap<>();
    readerNames = new HashMap<>();
    readerKinds = new HashMap<>();
    readerRootKind = reader.getKind();
    readerAt = new HashMap<>();
    List<List<Integer>> paths = new ArrayList<>();
    Set<Integer> readerPids = new HashSet<>();
    for (ProvenanceField field : reader.getFields()) {
      requirePid(field, readerPids, readerId);
      paths.add(field.getPath());
      index(field, readerNames, readerAt);
      if (field.getKind() != null) {
        readerKinds.put(field.getPath(), field.getKind());
      }
      List<Integer> writerPath = writerByPid.get(field.getPid());
      if (writerPath != null) {
        readerToWriter.put(field.getPath(), writerPath);
        writerToReader.put(writerPath, field.getPath());
      }
    }
    readerPaths = Collections.unmodifiableList(paths);
  }

  /**
   * Fails unless {@code field} has a pid no other location of its version has: pairing is by pid,
   * so a missing or shared one would pair unrelated locations.
   */
  private static void requirePid(ProvenanceField field, Set<Integer> seen, int schemaId) {
    if (field.getPid() == null) {
      throw new SerializationException("Location " + field.getPath() + " of schema id "
          + schemaId + " has no provenance id");
    }
    if (!seen.add(field.getPid())) {
      throw new SerializationException("Location " + field.getPath() + " of schema id "
          + schemaId + " shares provenance id " + field.getPid() + " with another location");
    }
  }

  private static void index(ProvenanceField field, Map<List<Integer>, List<String>> names,
      Map<List<String>, List<Integer>> at) {
    if (field.getNames() != null) {
      names.put(field.getPath(), field.getNames());
      // The first location spelled so wins: JSON union branches and Protobuf oneofs share their
      // parents' names.
      at.putIfAbsent(field.getNames(), field.getPath());
    }
  }

  /**
   * Joins the versions of {@code provenance} with schema ids {@code writerId} and {@code
   * readerId}. Versions are found by schema id, never by position: the writer may be the newer.
   *
   * @throws SerializationException if either schema id is not among the versions
   */
  public static ProvenanceMapping join(SchemaProvenance provenance, int writerId, int readerId) {
    return new ProvenanceMapping(
        versionWithId(provenance, writerId), versionWithId(provenance, readerId));
  }

  /**
   * Joins the version {@code readerVersion} of {@code provenance}, the reader's, with its other
   * end, the writer's; null when the two ends are one version. Found by version: the writer's
   * schema id may also be the reader's, under an earlier version.
   *
   * @throws SerializationException if the reader's version is not among the versions, or more
   *     than one other version is
   */
  public static ProvenanceMapping joinToVersion(SchemaProvenance provenance, int readerVersion) {
    ProvenanceVersion reader = null;
    ProvenanceVersion writer = null;
    for (ProvenanceVersion version : provenance.getVersions()) {
      if (version.getVersion() != null && version.getVersion() == readerVersion) {
        reader = version;
      } else if (writer == null) {
        writer = version;
      } else {
        throw new SerializationException("The provenance response has more than one version "
            + "besides the reader's, version " + readerVersion);
      }
    }
    if (reader == null) {
      throw new SerializationException(
          "The provenance response has no version " + readerVersion);
    }
    return writer == null ? null : new ProvenanceMapping(writer, reader);
  }

  private static ProvenanceVersion versionWithId(SchemaProvenance provenance, int schemaId) {
    for (ProvenanceVersion version : provenance.getVersions()) {
      if (version.getId() != null && version.getId() == schemaId) {
        return version;
      }
    }
    throw new SerializationException(
        "The provenance response has no version with schema id " + schemaId);
  }

  /**
   * Fails unless every location on both sides carries names: a reader can only find what they
   * spell, and one it cannot find would silently escape provenance.
   *
   * @throws SerializationException naming the first location without names
   */
  public void requireNames() {
    requireNames(writerPaths, writerNames, writerId);
    requireNames(readerPaths, readerNames, readerId);
  }

  private static void requireNames(List<List<Integer>> paths,
      Map<List<Integer>, List<String>> names, int schemaId) {
    for (List<Integer> path : paths) {
      if (!names.containsKey(path)) {
        throw new SerializationException("The provenance response gives no names for location "
            + path + " of schema id " + schemaId);
      }
    }
  }

  /**
   * Fails unless every location on both sides, and both roots, carry a kind: a reader telling a
   * field from a branch by it would otherwise guess.
   *
   * @throws SerializationException naming the first location without a kind
   */
  public void requireKinds() {
    requireKinds(writerPaths, writerKinds, writerRootKind, writerId);
    requireKinds(readerPaths, readerKinds, readerRootKind, readerId);
  }

  private static void requireKinds(List<List<Integer>> paths, Map<List<Integer>, String> kinds,
      String rootKind, int schemaId) {
    if (rootKind == null) {
      throw new SerializationException(
          "The provenance response gives no kind for the root of schema id " + schemaId);
    }
    for (List<Integer> path : paths) {
      if (!kinds.containsKey(path)) {
        throw new SerializationException("The provenance response gives no kind for location "
            + path + " of schema id " + schemaId);
      }
    }
  }

  /**
   * The writer's schema id.
   */
  public int writerId() {
    return writerId;
  }

  /**
   * The reader's schema id.
   */
  public int readerId() {
    return readerId;
  }

  /**
   * The outermost kind of the reader's root as the response gives it ({@code STRUCT},
   * {@code UNION}, ...), or null when it gives none.
   */
  public String readerRootKindName() {
    return readerRootKind == null ? null : Kind.parse(readerRootKind).name;
  }

  /**
   * Every writer field's path, in path order.
   */
  public List<List<Integer>> writerPaths() {
    return writerPaths;
  }

  /**
   * The writer field feeding the reader field at {@code readerPath}, or null if none.
   */
  public List<Integer> writerPathOf(List<Integer> readerPath) {
    return readerToWriter.get(readerPath);
  }

  /**
   * The reader field fed by the writer field at {@code writerPath}, or null if none.
   */
  public List<Integer> readerPathOf(List<Integer> writerPath) {
    return writerToReader.get(writerPath);
  }

  /**
   * Every reader field's path, in path order.
   */
  public List<List<Integer>> readerPaths() {
    return readerPaths;
  }

  /**
   * The names along {@code readerPath}, or null when the response carried none.
   */
  public List<String> readerNamesOf(List<Integer> readerPath) {
    return readerNames.get(readerPath);
  }

  /**
   * The names along {@code writerPath}, or null when the response carried none.
   */
  public List<String> writerNamesOf(List<Integer> writerPath) {
    return writerNames.get(writerPath);
  }

  /**
   * The names of the nearest reader location enclosing {@code readerPath} — skipping steps into
   * an array or map, which are no locations — or an empty list at the root.
   */
  public List<String> enclosingReaderNamesOf(List<Integer> readerPath) {
    for (int k = readerPath.size() - 1; k > 0; k--) {
      List<String> names = readerNames.get(readerPath.subList(0, k));
      if (names != null) {
        return names;
      }
    }
    return Collections.emptyList();
  }

  /**
   * The kind of the reader location at {@code readerPath}, or null when the response carried none.
   */
  public String readerKindOf(List<Integer> readerPath) {
    return readerKinds.get(readerPath);
  }

  /**
   * The kind of the writer location at {@code writerPath}, or null when the response carried none.
   */
  public String writerKindOf(List<Integer> writerPath) {
    return writerKinds.get(writerPath);
  }

  /**
   * Whether the reader location at {@code readerPath} is a union branch rather than a field; false
   * for a path that is no location, which ends in a collection step.
   */
  public boolean isReaderBranch(List<Integer> readerPath) {
    return isBranch(readerPath, readerKinds, readerRootKind);
  }

  /**
   * Whether the writer location at {@code writerPath} is a union branch rather than a field; false
   * for a path that is no location, which ends in a collection step.
   */
  public boolean isWriterBranch(List<Integer> writerPath) {
    return isBranch(writerPath, writerKinds, writerRootKind);
  }

  // A location is a branch when the type it sits in is a union: the nearest enclosing location's,
  // or the root's, followed through the collection steps between.
  private static boolean isBranch(List<Integer> path, Map<List<Integer>, String> kinds,
      String rootKind) {
    int k = path.size() - 1;
    while (k > 0 && !kinds.containsKey(path.subList(0, k))) {
      k--;
    }
    String enclosing = k > 0 ? kinds.get(path.subList(0, k)) : rootKind;
    if (enclosing == null) {
      return false;
    }
    Kind reached = Kind.parse(enclosing).through(path.subList(k, path.size() - 1));
    return reached != null && reached.name.equals("UNION");
  }

  /**
   * A kind as the endpoint spells it: {@code SCALAR}, {@code STRUCT}, {@code UNION}, or
   * {@code ARRAY<k>}, {@code MULTISET<k>} or {@code MAP<k, k>} of what a collection holds.
   */
  private static final class Kind {
    final String name;
    final List<Kind> held;

    private Kind(String name, List<Kind> held) {
      this.name = name;
      this.held = held;
    }

    static Kind parse(String spelled) {
      int[] at = {0};
      Kind kind = parse(spelled, at);
      if (at[0] != spelled.length()) {
        throw malformed(spelled);
      }
      return kind;
    }

    private static Kind parse(String spelled, int[] at) {
      int start = at[0];
      while (at[0] < spelled.length() && Character.isLetter(spelled.charAt(at[0]))) {
        at[0]++;
      }
      String name = spelled.substring(start, at[0]);
      if (name.isEmpty()) {
        throw malformed(spelled);
      }
      List<Kind> held = new ArrayList<>();
      if (at[0] < spelled.length() && spelled.charAt(at[0]) == '<') {
        do {
          at[0]++;
          while (at[0] < spelled.length() && spelled.charAt(at[0]) == ' ') {
            at[0]++;
          }
          held.add(parse(spelled, at));
        } while (at[0] < spelled.length() && spelled.charAt(at[0]) == ',');
        if (at[0] >= spelled.length() || spelled.charAt(at[0]) != '>') {
          throw malformed(spelled);
        }
        at[0]++;
      }
      return new Kind(name, held);
    }

    private static SerializationException malformed(String spelled) {
      return new SerializationException("The provenance response gives a malformed kind: "
          + spelled);
    }

    // What this kind holds at the end of collection steps: an element 0, a map's key 0, value 1.
    Kind through(List<Integer> steps) {
      Kind kind = this;
      for (int step : steps) {
        if (step < 0 || step >= kind.held.size()) {
          return null;
        }
        kind = kind.held.get(step);
      }
      return kind;
    }
  }

  /**
   * The reader location spelled {@code names}, or null if there is none.
   */
  public List<Integer> readerPathAt(List<String> names) {
    return readerAt.get(names);
  }

  /**
   * The writer location spelled {@code names}, or null if there is none.
   */
  public List<Integer> writerPathAt(List<String> names) {
    return writerAt.get(names);
  }
}
