/*
 * Copyright 2019 Confluent Inc.
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
  public int compareTo(SchemaRegistryKey o) {
    int compare = super.compareTo(o);
    if (compare == 0) {
      ModeKey otherKey = (ModeKey) o;
      if (this.subject == null && otherKey.subject == null) {
        return 0;
      } else {
        if (this.subject == null) {
          return -1;
        }
        if (otherKey.subject == null) {
          return 1;
        }
        return this.subject.compareTo(otherKey.subject);
      }
    } else {
      return compare;
    }
  }
}
