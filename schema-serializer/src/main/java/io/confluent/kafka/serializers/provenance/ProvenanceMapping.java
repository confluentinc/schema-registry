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
import java.util.List;
import java.util.Map;

/**
 * Which writer field feeds each reader field, from joining two versions of a {@link
 * SchemaProvenance} on the provenance id. Paths are the endpoint's inlined index paths; names are
 * each location in the native schema's own names, with {@code null} for an unnamed step.
 */
public final class ProvenanceMapping {

  private final Map<List<Integer>, List<Integer>> readerToWriter;
  private final Map<List<Integer>, List<Integer>> writerToReader;
  private final Map<List<Integer>, List<String>> readerNames;
  private final Map<List<Integer>, List<String>> writerNames;
  private final Map<List<String>, List<Integer>> readerAt;
  private final Map<List<String>, List<Integer>> writerAt;
  private final List<List<Integer>> readerPaths;

  private ProvenanceMapping(ProvenanceVersion writer, ProvenanceVersion reader) {
    Map<Integer, List<Integer>> writerByPid = new HashMap<>();
    writerNames = new HashMap<>();
    writerAt = new HashMap<>();
    for (ProvenanceField field : writer.getFields()) {
      writerByPid.put(field.getPid(), field.getPath());
      index(field, writerNames, writerAt);
    }
    readerToWriter = new HashMap<>();
    writerToReader = new HashMap<>();
    readerNames = new HashMap<>();
    readerAt = new HashMap<>();
    List<List<Integer>> paths = new ArrayList<>();
    for (ProvenanceField field : reader.getFields()) {
      paths.add(field.getPath());
      index(field, readerNames, readerAt);
      List<Integer> writerPath = writerByPid.get(field.getPid());
      if (writerPath != null) {
        readerToWriter.put(field.getPath(), writerPath);
        writerToReader.put(writerPath, field.getPath());
      }
    }
    readerPaths = Collections.unmodifiableList(paths);
  }

  private static void index(ProvenanceField field, Map<List<Integer>, List<String>> names,
      Map<List<String>, List<Integer>> at) {
    if (field.getNames() != null) {
      names.put(field.getPath(), field.getNames());
      // The first location spelled so wins: only JSON union branches share their names.
      at.putIfAbsent(field.getNames(), field.getPath());
    }
  }

  /**
   * Joins the versions of {@code provenance} with schema ids {@code writerId} and {@code
   * readerId}. Versions are found by schema id, never by position: the writer may be the newer.
   *
   * @throws ProvenanceUnavailableException if either schema id is not among the versions
   */
  public static ProvenanceMapping join(SchemaProvenance provenance, int writerId, int readerId) {
    return new ProvenanceMapping(
        versionWithId(provenance, writerId), versionWithId(provenance, readerId));
  }

  private static ProvenanceVersion versionWithId(SchemaProvenance provenance, int schemaId) {
    for (ProvenanceVersion version : provenance.getVersions()) {
      if (version.getId() != null && version.getId() == schemaId) {
        return version;
      }
    }
    throw new ProvenanceUnavailableException(
        "The provenance returned has no version with schema id " + schemaId);
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
