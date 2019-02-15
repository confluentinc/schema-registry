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

import com.fasterxml.jackson.annotation.JsonProperty;

import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.hibernate.validator.constraints.NotEmpty;

@JsonPropertyOrder(value = {"keytype", "subject", "magic"})
public class DeleteSubjectKey extends SchemaRegistryKey {

  private static final int MAGIC_BYTE = 0;
  @NotEmpty
  private String subject;

  public DeleteSubjectKey(@JsonProperty("subject") String subject) {
    super(SchemaRegistryKeyType.DELETE_SUBJECT);
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

    DeleteSubjectKey that = (DeleteSubjectKey) o;

    return subject.equals(that.subject);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + subject.hashCode();
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
}
