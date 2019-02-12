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

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

import java.util.Objects;

@JsonPropertyOrder(value = {"keytype", "subject", "magic"})
public class ModeKey extends SchemaRegistryKey {

  private static final int MAGIC_BYTE = 0;
  private String subject;

  public ModeKey(@JsonProperty("subject") String subject) {
    super(SchemaRegistryKeyType.MODE);
    this.subject = subject;
    this.magicByte = MAGIC_BYTE;
  }

  @JsonProperty("subject")
  public String getSubject() {
    return this.subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @JsonIgnore
  public boolean isPrefix() {
    return getPrefix() != null;
  }

  /**
   * Get the prefix for this ModeKey, or null if is not a prefix.
   * @return the prefix, or null
   */
  @JsonIgnore
  public String getPrefix() {
    return getPrefix(subject);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ModeKey modeKey = (ModeKey) o;
    return Objects.equals(subject, modeKey.subject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), subject);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{magic=" + this.magicByte + ",");
    sb.append("keytype=" + this.keyType.keyType + ",");
    sb.append("subject=" + this.subject + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(SchemaRegistryKey that) {
    int compare = super.compareTo(that);
    if (compare != 0) {
      return compare;
    }

    ModeKey otherKey = (ModeKey) that;

    // Sort by length of subject (or prefix) in descending order
    int lenComp = otherKey.subject.length() - this.subject.length();
    return lenComp == 0 ? subject.compareTo(otherKey.subject) : lenComp;
  }
}
