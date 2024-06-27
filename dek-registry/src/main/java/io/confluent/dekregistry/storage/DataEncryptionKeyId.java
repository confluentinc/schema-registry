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
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataEncryptionKeyId extends EncryptionKeyId {

  private final String kekName;
  private final String subject;
  private final DekFormat algorithm;
  private final int version;

  @JsonCreator
  public DataEncryptionKeyId(
      @JsonProperty("tenant") String tenant,
      @JsonProperty("kekName") String kekName,
      @JsonProperty("subject") String subject,
      @JsonProperty("algorithm") DekFormat algorithm,
      @JsonProperty("version") int version
  ) {
    super(tenant, KeyType.DEK);
    this.kekName = kekName;
    this.subject = subject;
    this.algorithm = algorithm;
    this.version = version;
  }

  @JsonProperty("kekName")
  public String getKekName() {
    return this.kekName;
  }

  @JsonProperty("subject")
  public String getSubject() {
    return this.subject;
  }

  @JsonProperty("algorithm")
  public DekFormat getAlgorithm() {
    return this.algorithm;
  }

  @JsonProperty("version")
  public int getVersion() {
    return this.version;
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
        && Objects.equals(subject, that.subject)
        && algorithm == that.algorithm
        && version == that.version;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), kekName, subject, algorithm, version);
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
      int kekNameComparison = this.getKekName().compareTo(that.getKekName());
      if (kekNameComparison != 0) {
        return kekNameComparison < 0 ? -1 : 1;
      }
    }

    if (this.getSubject() == null && that.getSubject() == null) {
      // pass
    } else if (this.getSubject() == null) {
      return -1;
    } else if (that.getSubject() == null) {
      return 1;
    } else {
      QualifiedSubject qs1 = QualifiedSubject.createFromUnqualified(
          this.getTenant(), this.getSubject());
      QualifiedSubject qs2 = QualifiedSubject.createFromUnqualified(
          that.getTenant(), that.getSubject());
      int subjectComparison = qs1.compareTo(qs2);
      if (subjectComparison != 0) {
        return subjectComparison < 0 ? -1 : 1;
      }
    }

    if (this.getAlgorithm() == null && that.getAlgorithm() == null) {
      // pass
    } else if (this.getAlgorithm() == null) {
      return -1;
    } else if (that.getAlgorithm() == null) {
      return 1;
    } else {
      int algorithmComparison = this.getAlgorithm().compareTo(that.getAlgorithm());
      if (algorithmComparison != 0) {
        return algorithmComparison < 0 ? -1 : 1;
      }
    }

    if (this.getVersion() != that.getVersion()) {
      return (this.getVersion() < that.getVersion() ? -1 : 1);
    }

    return 0;
  }
}
