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

package io.confluent.kafka.serializers;

import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.util.Objects;

// Cache key for a schema looked up by id. Ids are only unique within a context, and the server
// may resolve an unqualified subject in any context, so the id is paired with the subject used
// for the lookup (the same granularity as the registry client's own id cache).
final class SubjectSchemaId {
  private final String subject;
  private final SchemaId schemaId;

  SubjectSchemaId(String subject, SchemaId schemaId) {
    this.subject = subject;
    this.schemaId = schemaId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SubjectSchemaId that = (SubjectSchemaId) o;
    return Objects.equals(subject, that.subject) && Objects.equals(schemaId, that.schemaId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, schemaId);
  }

  @Override
  public String toString() {
    return "SubjectSchemaId{"
        + "subject='" + subject + '\''
        + ", schemaId=" + schemaId
        + '}';
  }
}
