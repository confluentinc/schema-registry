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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;

import jakarta.validation.constraints.Min;

@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder(value = {"keytype", "subject", "version", "magic"})
public class SchemaKey extends SubjectKey {

  private static final int MAGIC_BYTE = 1;
  @Min(1)
  @NotEmpty
  private Integer version;

  public SchemaKey(@JsonProperty("subject") String subject,
                   @JsonProperty("version") int version) {
    super(SchemaRegistryKeyType.SCHEMA, subject);
    this.magicByte = MAGIC_BYTE;
    this.version = version;
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
    if (!super.equals(o)) {
      return false;
    }

    SchemaKey that = (SchemaKey) o;
    return version.equals(that.version);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + version;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{magic=" + this.magicByte + ",");
    sb.append("keytype=" + this.keyType.keyType + ",");
    sb.append("subject=" + this.getSubject() + ",");
    sb.append("version=" + this.version + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(SchemaRegistryKey o) {
    int compare = super.compareTo(o);
    if (compare == 0) {
      SchemaKey otherKey = (SchemaKey) o;
      return this.version - otherKey.version;
    } else {
      return compare;
    }
  }
}
