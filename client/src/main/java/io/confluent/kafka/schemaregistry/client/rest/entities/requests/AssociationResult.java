/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationResult {

  private ErrorMessage error;
  private AssociationResponse result;

  @JsonCreator
  public AssociationResult(
      @JsonProperty("error") ErrorMessage error,
      @JsonProperty("result") AssociationResponse result) {
    this.error = error;
    this.result = result;
  }

  @JsonProperty("error")
  public ErrorMessage getError() {
    return error;
  }

  @JsonProperty("error")
  public void setError(ErrorMessage error) {
    this.error = error;
  }

  @JsonProperty("result")
  public AssociationResponse getResult() {
    return result;
  }

  @JsonProperty("result")
  public void setResult(AssociationResponse result) {
    this.result = result;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssociationResult that = (AssociationResult) o;
    return Objects.equals(error, that.error)
        && Objects.equals(result, that.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(error, result);
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }
}