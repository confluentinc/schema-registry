/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.codehaus.jackson.annotate.JsonPropertyOrder;

@JsonPropertyOrder(value = {"keytype", "subject", "magic"})
public class ConfigKey extends SchemaRegistryKey {

  private static final int MAGIC_BYTE = 0;
  private String subject;

  public ConfigKey(@JsonProperty("subject") String subject) {
    super(SchemaRegistryKeyType.CONFIG);
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
   * Get the prefix for this ConfigKey, or null if is not a prefix.
   * @return the prefix, or null
   */
  @JsonIgnore
  public String getPrefix() {
    return getPrefix(subject);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }

    ConfigKey that = (ConfigKey) o;
    if (this.subject != null && that.subject != null) {
      if (!subject.equals(that.subject)) {
        return false;
      }
    } else if (this.subject == null && that.subject == null) {
      return true;
    } else {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    if (this.subject != null) {
      result = 31 * result + subject.hashCode();
    }
    return result;
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
      ConfigKey otherKey = (ConfigKey) o;
      if (this.subject == null && otherKey.subject == null) {
        return 0;
      } else {
        if (this.subject == null) {
          return -1;
        }
        if (otherKey.subject == null) {
          return 1;
        }
        // Sort by length of subject (or prefix) in descending order
        int lenComp = otherKey.subject.length() - this.subject.length();
        return lenComp == 0 ? subject.compareTo(otherKey.subject) : lenComp;
      }
    } else {
      return compare;
    }
  }
}
