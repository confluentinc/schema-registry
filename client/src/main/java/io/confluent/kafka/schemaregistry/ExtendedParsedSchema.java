/*
 * Copyright 2025 Confluent Inc.
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

import java.util.Objects;

/**
 * Extended schema that includes metadata such as subject, id, version, and guid
 * alongside the parsed schema.
 */
public class ExtendedParsedSchema {
  private final String subject;
  private final Integer version;
  private final Integer id;
  private final String guid;
  private final ParsedSchema schema;

  public ExtendedParsedSchema(
      String subject, Integer version, Integer id, String guid, ParsedSchema schema) {
    this.subject = subject;
    this.version = version;
    this.id = id;
    this.guid = guid;
    this.schema = schema;
  }

  public String getSubject() {
    return subject;
  }

  public Integer getVersion() {
    return version;
  }

  public Integer getId() {
    return id;
  }

  public String getGuid() {
    return guid;
  }

  public ParsedSchema getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExtendedParsedSchema that = (ExtendedParsedSchema) o;
    return Objects.equals(subject, that.subject)
        && version.equals(that.version)
        && id.equals(that.id)
        && Objects.equals(guid, that.guid)
        && schema.equals(that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, version, id, guid, schema);
  }
}

