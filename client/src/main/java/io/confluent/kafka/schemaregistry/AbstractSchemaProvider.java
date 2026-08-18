/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry;

import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

public abstract class AbstractSchemaProvider implements SchemaProvider {

  public static final String REFERENCE_VERSIONS_STRICT_CONFIG = "reference.versions.strict";

  private SchemaVersionFetcher schemaVersionFetcher;
  private boolean referenceVersionsStrict = false;

  @Override
  public void configure(Map<String, ?> configs) {
    schemaVersionFetcher =
        (SchemaVersionFetcher) configs.get(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG);
    Object strict = configs.get(REFERENCE_VERSIONS_STRICT_CONFIG);
    if (strict instanceof Boolean) {
      referenceVersionsStrict = (Boolean) strict;
    } else if (strict instanceof String) {
      referenceVersionsStrict = Boolean.parseBoolean((String) strict);
    }
  }

  public SchemaVersionFetcher schemaVersionFetcher() {
    return schemaVersionFetcher;
  }

  protected Map<String, String> resolveReferences(Schema schema) {
    return resolveReferences(schema, false);
  }

  protected Map<String, String> resolveReferences(Schema schema, boolean validateAsNew) {
    return resolveReferences(
        schemaVersionFetcher(), schema.getSubject(), schema.getReferences(),
        validateAsNew, referenceVersionsStrict);
  }

  /**
   * Recursively resolves a schema's references into a map of reference name to schema text,
   * matching the behavior of the instance-level resolution: each unqualified reference subject is
   * qualified relative to {@code subject}'s parent context, and each referenced schema's own
   * references are resolved before it is added, so the map contains the full transitive closure.
   *
   * @param fetcher                 used to look up referenced schemas by subject and version
   * @param subject                 the referencing schema's subject, the parent for qualification
   * @param references              the referencing schema's references, or {@code null} for none
   * @param validateAsNew           when true, deleted referenced versions are not looked up
   * @param referenceVersionsStrict when true, a reference name bound to two different versions
   *                                across the graph is an error rather than being ignored
   */
  public static Map<String, String> resolveReferences(
      SchemaVersionFetcher fetcher, String subject, List<SchemaReference> references,
      boolean validateAsNew, boolean referenceVersionsStrict) {
    if (references == null) {
      return Collections.emptyMap();
    }
    Map<String, String> result = new LinkedHashMap<>();
    Map<String, Integer> visited = new HashMap<>();
    resolveReferences(
        fetcher, subject, references, result, visited, validateAsNew, referenceVersionsStrict);
    return result;
  }

  private static void resolveReferences(
      SchemaVersionFetcher fetcher, String subject, List<SchemaReference> references,
      Map<String, String> schemas, Map<String, Integer> visited,
      boolean validateAsNew, boolean referenceVersionsStrict) {
    boolean lookupDeletedSchema = !validateAsNew;
    if (references == null) {
      return;
    }
    for (SchemaReference reference : references) {
      if (reference.getName() == null
          || reference.getSubject() == null
          || reference.getVersion() == null) {
        throw new IllegalArgumentException("Invalid reference: " + reference);
      }
      QualifiedSubject refSubject = QualifiedSubject.qualifySubjectWithParent(
              fetcher.tenant(), subject, reference.getSubject());
      Schema s = fetcher.getByVersion(refSubject.toQualifiedSubject(),
              reference.getVersion(), lookupDeletedSchema);
      if (s == null) {
        throw new IllegalArgumentException("No schema reference found for subject \""
                + refSubject
                + "\" and version "
                + reference.getVersion());
      }
      if (reference.getVersion() == -1) {
        // Update the version with the latest
        reference.setVersion(s.getVersion());
      }
      if (visited.containsKey(reference.getName())) {
        if (referenceVersionsStrict) {
          Integer previousVersion = visited.get(reference.getName());
          if (!previousVersion.equals(reference.getVersion())) {
            throw new IllegalStateException(
                "Conflicting reference versions for \"" + reference.getName()
                + "\": version " + previousVersion
                + " and version " + reference.getVersion());
          }
        }
        continue;
      } else {
        visited.put(reference.getName(), reference.getVersion());
      }
      if (!schemas.containsKey(reference.getName())) {
        resolveReferences(fetcher, s.getSubject(), s.getReferences(), schemas, visited,
            validateAsNew, referenceVersionsStrict);
        schemas.put(reference.getName(), s.getSchema());
      }
    }
  }

  // Parking this method and the following ones here instead of in ParsedSchema as interfaces can't
  // have private methods in Java 8.  Move these to ParsedSchema in 8.0.x
  protected static boolean canLookupIgnoringVersion(
      ParsedSchema current, ParsedSchema prev) {
    Integer schemaVer = getConfluentVersionNumber(current.metadata());
    Integer prevVer = getConfluentVersionNumber(prev.metadata());
    if (schemaVer == null && prevVer != null) {
      ParsedSchema newSchema = current.metadata() != null
          ? current
          : current.copy(new Metadata(null, null, null), current.ruleSet());
      ParsedSchema newPrev = prev.copy(
          Metadata.removeConfluentVersion(prev.metadata()), prev.ruleSet());
      // This handles the case where current schema is without confluent:version
      return newSchema.equivalent(newPrev);
    } else if (schemaVer != null && prevVer == null) {
      if (!schemaVer.equals(prev.version())) {
        // The incoming confluent:version must match the actual version of the prev schema
        return false;
      }
      ParsedSchema newPrev = prev.metadata() != null
          ? prev
          : prev.copy(new Metadata(null, null, null), prev.ruleSet());
      ParsedSchema newSchema = current.copy(
          Metadata.removeConfluentVersion(current.metadata()), current.ruleSet());
      // This handles the case where prev schema is without confluent:version
      return newSchema.equivalent(newPrev);
    } else {
      return current.equivalent(prev);
    }
  }

  protected static boolean hasLatestVersion(List<SchemaReference> refs) {
    return refs.stream().anyMatch(e -> e.getVersion() == -1);
  }

  protected static List<SchemaReference> replaceLatestVersion(
      List<SchemaReference> refs, SchemaVersionFetcher fetcher) {
    List<SchemaReference> result = new ArrayList<>();
    for (SchemaReference ref : refs) {
      if (ref.getVersion() == -1) {
        Schema s = fetcher.getByVersion(ref.getSubject(), -1, false);
        result.add(new SchemaReference(ref.getName(), ref.getSubject(), s.getVersion()));
      } else {
        result.add(ref);
      }
    }
    return result;
  }

  protected static Integer getConfluentVersionNumber(Metadata metadata) {
    return metadata != null ? metadata.getConfluentVersionNumber() : null;
  }

  protected static String getConfluentVersion(Metadata metadata) {
    return metadata != null ? metadata.getConfluentVersion() : null;
  }
}
