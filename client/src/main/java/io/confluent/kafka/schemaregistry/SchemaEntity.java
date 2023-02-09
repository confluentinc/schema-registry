/*
 * Copyright 2023 Confluent Inc.
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

public class SchemaEntity {

  private final String entityPath;
  private final EntityType entityType;

  public SchemaEntity(String entityPath, EntityType entityType) {
    this.entityPath = entityPath;
    this.entityType = entityType;
  }

  public String getEntityPath() {
    return entityPath;
  }

  public EntityType getEntityType() {
    return entityType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaEntity other = (SchemaEntity) o;
    return Objects.equals(this.entityPath, other.getEntityPath())
      && Objects.equals(this.entityType, other.getEntityType());
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(entityPath);
    result = 31 * result + Objects.hashCode(entityType);
    return result;
  }

  public enum EntityType {
    SR_RECORD,
    SR_FIELD;

    public static EntityType forName(String name) {
      if (name == null) {
        return null;
      }

      name = name.toUpperCase();
      if (SR_RECORD.name().equals(name)) {
        return SR_RECORD;
      } else if (SR_FIELD.name().equals(name)) {
        return SR_FIELD;
      } else {
        return null;
      }
    }
  }
}
