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

import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
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
      if (visited.contains(reference.getName())) {
        continue;
      } else {
        visited.add(reference.getName());
      }
      if (!schemas.containsKey(reference.getName())) {
        resolveReferences(s, schemas, visited);
        schemas.put(reference.getName(), s.getSchema());
      }
    }
  }
}
