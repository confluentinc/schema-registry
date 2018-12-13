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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

import java.util.Objects;

@JsonPropertyOrder(value = {"keytype", "subject", "prefix", "magic"})
public class ModeKey extends SchemaRegistryKey {

  private static final int MAGIC_BYTE = 0;
  private String subject;
  private boolean prefix;

  public ModeKey(@JsonProperty("subject") String subject,
                 @JsonProperty("prefix") boolean prefix) {
    super(SchemaRegistryKeyType.MODE);
    this.subject = subject;
    this.prefix = prefix;
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

  @JsonProperty("prefix")
  public boolean isPrefix() {
    return this.prefix;
  }

  @JsonProperty("prefix")
  public void setPrefix(boolean prefix) {
    this.prefix = prefix;
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
    return prefix == modeKey.prefix && Objects.equals(subject, modeKey.subject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), subject, prefix);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{magic=" + this.magicByte + ",");
    sb.append("keytype=" + this.keyType.keyType + ",");
    sb.append("subject=" + this.subject + ",");
    sb.append("prefix=" + this.prefix + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(SchemaRegistryKey that) {
    int compare = super.compareTo(that);
    if (compare == 0) {
      ModeKey otherKey = (ModeKey) that;

      // All non-prefixes come before prefixes
      if (!this.prefix && otherKey.prefix) {
        return -1;
      } else if (this.prefix && !otherKey.prefix) {
        return 1;
      }

      // Sort by length of subject in descending order
      int len = otherKey.subject.length() - this.subject.length();
      if (len != 0) {
        return len;
      }

      return this.subject.compareTo(otherKey.subject);
    } else {
      return compare;
    }
  }
}
