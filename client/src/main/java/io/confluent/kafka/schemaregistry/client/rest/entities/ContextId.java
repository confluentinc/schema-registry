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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Context ID pair")
public class ContextId implements Comparable<ContextId> {

  private String context;
  private Integer id;

  @JsonCreator
  public ContextId(@JsonProperty("context") String context,
                   @JsonProperty("id") Integer id) {
    this.context = context;
    this.id = id;
  }

  @JsonProperty("context")
  public String getContext() {
    return context;
  }

  @JsonProperty("context")
  public void setContext(String context) {
    this.context = context;
  }

  @JsonProperty("id")
  public Integer getId() {
    return this.id;
  }

  @JsonProperty("id")
  public void setId(Integer id) {
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContextId schema1 = (ContextId) o;
    return Objects.equals(context, schema1.context)
        && Objects.equals(id, schema1.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(context, id);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{context=" + this.context + ",");
    sb.append("id=" + this.id + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(ContextId that) {
    int result = this.context.compareTo(that.context);
    if (result != 0) {
      return result;
    }
    result = this.id - that.id;
    return result;
  }
}
