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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.Set;

public abstract class AbstractSchemaProvider implements SchemaProvider {

  private SchemaVersionFetcher schemaVersionFetcher;

  @Override
  public void configure(Map<String, ?> configs) {
    schemaVersionFetcher =
        (SchemaVersionFetcher) configs.get(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG);
  }

  public SchemaVersionFetcher schemaVersionFetcher() {
    return schemaVersionFetcher;
  }

  protected Map<String, String> resolveReferences(Schema schema) {
    List<SchemaReference> references = schema.getReferences();
    if (references == null) {
      return Collections.emptyMap();
    }
    Map<String, String> result = new LinkedHashMap<>();
    Set<String> visited = new HashSet<>();
    resolveReferences(schema, result, visited);
    return result;
  }

  private void resolveReferences(Schema schema, Map<String, String> schemas, Set<String> visited) {
    List<SchemaReference> references = schema.getReferences();
    for (SchemaReference reference : references) {
      if (reference.getName() == null
          || reference.getSubject() == null
          || reference.getVersion() == null) {
        throw new IllegalStateException("Invalid reference: " + reference);
      }
      if (visited.contains(reference.getName())) {
        continue;
      } else {
        visited.add(reference.getName());
      }
      if (!schemas.containsKey(reference.getName())) {
        QualifiedSubject refSubject = QualifiedSubject.qualifySubjectWithParent(
            schemaVersionFetcher().tenant(), schema.getSubject(), reference.getSubject());
        Schema s = schemaVersionFetcher().getByVersion(refSubject.toQualifiedSubject(),
            reference.getVersion(), true);
        if (s == null) {
          throw new IllegalStateException("No schema reference found for subject \""
              + refSubject
              + "\" and version "
              + reference.getVersion());
        }
        if (reference.getVersion() == -1) {
          // Update the version with the latest
          reference.setVersion(s.getVersion());
        }
        resolveReferences(s, schemas, visited);
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
