/**
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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

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

  protected Map<String, String> resolveReferences(List<SchemaReference> references) {
    if (references == null) {
      return Collections.emptyMap();
    }
    Map<String, String> result = new LinkedHashMap<>();
    resolveReferences(references, result);
    return result;
  }

  private void resolveReferences(List<SchemaReference> references, Map<String, String> schemas) {
    for (SchemaReference reference : references) {
      String subject = reference.getSubject();
      Schema schema = schemaVersionFetcher().getByVersion(subject, reference.getVersion(), true);
      if (schema == null) {
        throw new IllegalStateException("No schema reference found for subject \"" + subject
                + "\" and version " + reference.getVersion());
      }
      resolveReferences(schema.getReferences(), schemas);
      schemas.put(reference.getName(), schema.getSchema());
    }
  }
}
