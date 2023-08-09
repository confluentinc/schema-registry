/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dekregistry.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.dekregistry.client.rest.entities.KeyType;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataEncryptionKeyId extends EncryptionKeyId {

  private final String kekName;
  private final String scope;
  private final DekFormat algorithm;

  @JsonCreator
  public DataEncryptionKeyId(
      @JsonProperty("tenant") String tenant,
      @JsonProperty("kekName") String kekName,
      @JsonProperty("scope") String scope,
      @JsonProperty("algorithm") DekFormat algorithm
  ) {
    super(tenant, KeyType.DEK);
    this.kekName = kekName;
    this.scope = scope;
    this.algorithm = algorithm;
  }

  @JsonProperty("kekName")
  public String getKekName() {
    return this.kekName;
  }

  @JsonProperty("scope")
  public String getScope() {
    return this.scope;
  }

  @JsonProperty("algorithm")
  public DekFormat getAlgorithm() {
    return this.algorithm;
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
    DataEncryptionKeyId that = (DataEncryptionKeyId) o;
    return Objects.equals(kekName, that.kekName)
        && Objects.equals(scope, that.scope)
        && algorithm == that.algorithm;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), kekName, scope, algorithm);
  }

  @Override
  public int compareTo(EncryptionKeyId o) {
    int compare = super.compareTo(o);
    if (compare != 0) {
      return compare;
    }

    DataEncryptionKeyId that = (DataEncryptionKeyId) o;
    if (this.getKekName() == null && that.getKekName() == null) {
      // pass
    } else if (this.getKekName() == null) {
      return -1;
    } else if (that.getKekName() == null) {
      return 1;
    } else {
      int kmsTypeComparison = this.getKekName().compareTo(that.getKekName());
      if (kmsTypeComparison != 0) {
        return kmsTypeComparison < 0 ? -1 : 1;
      }
    }

    if (this.getKekName() == null && that.getKekName() == null) {
      // pass
    } else if (this.getKekName() == null) {
      return -1;
    } else if (that.getKekName() == null) {
      return 1;
    } else {
      int kmsKeyIdComparison = this.getKekName().compareTo(that.getKekName());
      if (kmsKeyIdComparison != 0) {
        return kmsKeyIdComparison < 0 ? -1 : 1;
      }
    }

    if (this.getScope() == null && that.getScope() == null) {
      // pass
    } else if (this.getScope() == null) {
      return -1;
    } else if (that.getScope() == null) {
      return 1;
    } else {
      int scopeComparison = this.getScope().compareTo(that.getScope());
      if (scopeComparison != 0) {
        return scopeComparison < 0 ? -1 : 1;
      }
    }

    if (this.getAlgorithm() == null && that.getAlgorithm() == null) {
      return 0;
    } else if (this.getAlgorithm() == null) {
      return -1;
    } else if (that.getAlgorithm() == null) {
      return 1;
    } else {
      return this.getAlgorithm().compareTo(that.getAlgorithm());
    }
  }
}
