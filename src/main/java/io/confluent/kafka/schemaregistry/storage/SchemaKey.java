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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class SchemaKey implements Comparable<SchemaKey> {
  private static final int MAGIC_BYTE = 0;
  @Min(0)
  private int magicByte;
  @NotEmpty
  private String subject;
  @Min(1)
  private Integer version;

  public SchemaKey(@JsonProperty("subject") String subject,
                   @JsonProperty("version") int version) {
    this.magicByte = MAGIC_BYTE;
    this.subject = subject;
    this.version = version;
  }

  @JsonProperty("magic")
  public int getMagicByte() {
    return this.magicByte;
  }

  @JsonProperty("magic")
  public void setMagicByte(int magicByte) {
    this.magicByte = magicByte;
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

    if (this.magicByte != that.magicByte) {
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
    int result = this.magicByte;
    result = 31 * result + subject.hashCode();
    result = 31 * result + version;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{magic_byte=" + this.magicByte + ",");
    sb.append("subject=" + this.subject + ",");
    sb.append("version=" + this.version + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(SchemaKey otherKey) {
    if (this.magicByte == otherKey.magicByte) {
      int compare = this.subject.compareTo(otherKey.subject);
      return compare == 0 ? this.version - otherKey.version : compare;
    } else {
      return this.magicByte - otherKey.magicByte;
    }
  }
}
