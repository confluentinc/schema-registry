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
 * the same paths in names.
 */
public final class ProvenanceMapping {

  private final Map<List<Integer>, List<Integer>> readerToWriter;
  private final Map<List<Integer>, List<Integer>> writerToReader;
  private final Map<List<Integer>, List<String>> readerNames;
  private final List<List<Integer>> readerPaths;

  private ProvenanceMapping(Map<List<Integer>, List<Integer>> readerToWriter,
      Map<List<Integer>, List<Integer>> writerToReader,
      Map<List<Integer>, List<String>> readerNames, List<List<Integer>> readerPaths) {
    this.readerToWriter = readerToWriter;
    this.writerToReader = writerToReader;
    this.readerNames = readerNames;
    this.readerPaths = readerPaths;
  }

  /**
   * Joins the versions of {@code provenance} with schema ids {@code writerId} and {@code
   * readerId}. Versions are found by schema id, never by position: the writer may be the newer.
   *
   * @throws ProvenanceUnavailableException if either schema id is not among the versions
   */
  public static ProvenanceMapping join(SchemaProvenance provenance, int writerId, int readerId) {
    ProvenanceVersion writer = versionWithId(provenance, writerId);
    ProvenanceVersion reader = versionWithId(provenance, readerId);
    Map<Integer, List<Integer>> writerByPid = new HashMap<>();
    for (ProvenanceField field : writer.getFields()) {
      writerByPid.put(field.getPid(), field.getPath());
    }
    Map<List<Integer>, List<Integer>> readerToWriter = new HashMap<>();
    Map<List<Integer>, List<Integer>> writerToReader = new HashMap<>();
    Map<List<Integer>, List<String>> readerNames = new HashMap<>();
    List<List<Integer>> readerPaths = new ArrayList<>();
    for (ProvenanceField field : reader.getFields()) {
      readerPaths.add(field.getPath());
      if (field.getNames() != null) {
        readerNames.put(field.getPath(), field.getNames());
      }
      List<Integer> writerPath = writerByPid.get(field.getPid());
      if (writerPath != null) {
        readerToWriter.put(field.getPath(), writerPath);
        writerToReader.put(writerPath, field.getPath());
      }
    }
    return new ProvenanceMapping(readerToWriter, writerToReader, readerNames,
        Collections.unmodifiableList(readerPaths));
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
   * The member name a names step stands for, or null if the step is a collection step ({@code
   * []}, {@code {key}} or {@code {value}}). A member name spelling one of those arrives escaped
   * with a leading {@code $$}.
   */
  public static String memberNameOf(String step) {
    if ("[]".equals(step) || "{key}".equals(step) || "{value}".equals(step)) {
      return null;
    }
    return step.startsWith("$$") ? step.substring(2) : step;
  }

  /**
   * The names along {@code readerPath}, or null when the response carried none.
   */
  public List<String> readerNamesOf(List<Integer> readerPath) {
    return readerNames.get(readerPath);
  }
}
