/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Kafka schema registry maintains a few in memory indices to facilitate schema lookups. One such
 * index is the md5 index that maps MD5 -&gt; SchemaIdAndSubjects.
 * This index is used to do 2 things.
 * Firstly, to prevent the same schema string from being registered multiple times. So, if the MD5
 * of the canonicalized schema is present in the registry, we simply return the id. However, the
 * same schema string can be registered under multiple subjects. And if so, it may be assigned
 * different version ids per subject that it registers under
 */
public class SchemaIdAndSubjects {

  private int id;
  private Map<String, Integer> subjectsAndVersions;

  public SchemaIdAndSubjects(int id) {
    this.subjectsAndVersions = new ConcurrentHashMap<>();
    this.id = id;
  }

  public SchemaIdAndSubjects(int id, Map<String, Integer> subjectsAndVersions) {
    this.subjectsAndVersions = subjectsAndVersions;
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

  public boolean isEmpty() {
    return subjectsAndVersions.isEmpty();
  }

  public SchemaKey findAny(Predicate<SchemaKey> filter) {
    return subjectsAndVersions.entrySet().stream()
        .map(e -> new SchemaKey(e.getKey(), e.getValue()))
        .filter(key -> filter.test(key))
        .findAny()
        .orElse(null);
  }

  public Set<String> allSubjects() {
    return subjectsAndVersions.keySet();
  }

  public Map<String, Integer> allSubjectVersions() {
    return subjectsAndVersions;
  }

  public void removeIf(Predicate<SchemaKey> filter) {
    subjectsAndVersions.entrySet().removeIf(e ->
        filter.test(new SchemaKey(e.getKey(), e.getValue())));
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
