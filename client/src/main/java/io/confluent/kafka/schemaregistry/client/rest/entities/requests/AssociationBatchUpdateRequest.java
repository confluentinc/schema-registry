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
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationBatchUpdateRequest {

  private List<AssociationUpdateRequest> requests;

  @JsonCreator
  public AssociationBatchUpdateRequest(
      @JsonProperty("requests") List<AssociationUpdateRequest> requests) {
    this.requests = requests;
  }

  @JsonProperty("requests")
  public List<AssociationUpdateRequest> getRequests() {
    return requests;
  }

  @JsonProperty("requests")
  public void setRequests(List<AssociationUpdateRequest> requests) {
    this.requests = requests;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssociationBatchUpdateRequest that = (AssociationBatchUpdateRequest) o;
    return Objects.equals(requests, that.requests);
  }

  @Override
  public int hashCode() {
    return Objects.hash(requests);
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }
}