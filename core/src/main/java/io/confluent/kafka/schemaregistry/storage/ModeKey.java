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

@JsonPropertyOrder(value = {"keytype", "prefix", "magic"})
public class ModeKey extends SchemaRegistryKey {

  private static final int MAGIC_BYTE = 0;
  private String prefix;

  public ModeKey(@JsonProperty("prefix") String prefix) {
    super(SchemaRegistryKeyType.MODE);
    this.prefix = prefix;
    this.magicByte = MAGIC_BYTE;
  }

  @JsonProperty("prefix")
  public String getPrefix() {
    return this.prefix;
  }

  @JsonProperty("prefix")
  public void setPrefix(String prefix) {
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
    return Objects.equals(prefix, modeKey.prefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), prefix);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{magic=" + this.magicByte + ",");
    sb.append("keytype=" + this.keyType.keyType + ",");
    sb.append("prefix=" + this.prefix + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(SchemaRegistryKey that) {
    int compare = super.compareTo(that);
    if (compare == 0) {
      ModeKey otherKey = (ModeKey) that;

      // Sort by length of prefix in descending order
      return otherKey.prefix.length() - this.prefix.length();
    } else {
      return compare;
    }
  }
}
