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

import javax.validation.constraints.Min;

@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeleteSubjectValue extends SubjectValue {

  @Min(1)
  private Integer version;

  public DeleteSubjectValue(@JsonProperty("subject") String subject,
                            @JsonProperty("version") Integer version) {
    super(subject);
    this.version = version;
  }

  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
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

    DeleteSubjectValue that = (DeleteSubjectValue) o;

    return version.equals(that.version);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + version.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{subject=" + this.getSubject() + ",");
    sb.append("version=" + this.version + "}");
    return sb.toString();
  }

  @Override
  public DeleteSubjectKey toKey() {
    return new DeleteSubjectKey(getSubject());
  }
}
