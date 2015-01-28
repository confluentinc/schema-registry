/*
 * Copyright 2015 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.resources;

import java.util.HashMap;
import java.util.Map;

public class SchemaIdAndSubjects {

  private int id;
  private Map<String, Integer> subjectsAndVersions;

  public SchemaIdAndSubjects(int id) {
    this.subjectsAndVersions = new HashMap<String, Integer>();
    this.id = id;
  }

  public void addSubjectAndVersion(String subject, int version) {
    this.subjectsAndVersions.put(subject, version);
  }

  public boolean hasSubject(String subject) {
    return this.subjectsAndVersions.keySet().contains(subject);
  }

  public int getVersion(String subject) {
    return this.subjectsAndVersions.get(subject);
  }

  public int getSchemaId() {
    return this.id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaIdAndSubjects that = (SchemaIdAndSubjects) o;

    if (this.id != that.id) {
      return false;
    }
    if (!this.subjectsAndVersions.equals(that.subjectsAndVersions)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = 31 * this.id;
    result = 31 * result + subjectsAndVersions.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{id=" + this.id + ",");
    sb.append("subjectsAndVersions=" + this.subjectsAndVersions.toString() + "}");
    return sb.toString();
  }

}
