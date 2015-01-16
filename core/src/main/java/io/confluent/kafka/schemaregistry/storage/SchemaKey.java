/*
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

@JsonPropertyOrder(value = {"keytype", "subject", "version", "magic"})
public class SchemaKey extends SchemaRegistryKey {

  private static final int MAGIC_BYTE = 0;
  @NotEmpty
  private String subject;
  @Min(1)
  @NotEmpty
  private Integer version;

  public SchemaKey(@JsonProperty("subject") String subject,
                   @JsonProperty("version") int version) {
    super(SchemaRegistryKeyType.SCHEMA);
    this.magicByte = MAGIC_BYTE;
    this.subject = subject;
    this.version = version;
  }

  @JsonProperty("subject")
  public String getSubject() {
    return this.subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @JsonProperty("version")
  public int getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaKey that = (SchemaKey) o;

    if (!super.equals(o)) {
      return false;
    }
    if (!subject.equals(that.subject)) {
      return false;
    }
    if (version != that.version) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + subject.hashCode();
    result = 31 * result + version;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{magic=" + this.magicByte + ",");
    sb.append("keytype=" + this.keyType.keyType + ",");
    sb.append("subject=" + this.subject + ",");
    sb.append("version=" + this.version + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(SchemaRegistryKey o) {
    int compare = super.compareTo(o);
    if (compare == 0) {
      SchemaKey otherKey = (SchemaKey) o;
      int subjectComp = this.subject.compareTo(otherKey.subject);
      return subjectComp == 0 ? this.version - otherKey.version : subjectComp;
    } else {
      return compare;
    }
  }
}
