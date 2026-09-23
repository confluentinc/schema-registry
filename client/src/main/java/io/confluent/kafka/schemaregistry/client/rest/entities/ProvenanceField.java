/*
 * Copyright 2026 Confluent Inc.
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

import java.util.List;
import java.util.Objects;

/**
 * One field of one version: where it sits, what it is called, and which provenance id it carries.
 * A field is any addressable position in the inlined schema: a struct field or a union branch.
 *
 * <p>The {@code pid} identifies the field <em>at a location</em> in the fully inlined schema. A
 * named type used by two fields gives each of its fields two ids, one per use site, so a consumer
 * can join two versions on the pid alone without ever pairing the two uses. A rename keeps the
 * pid; a drop retires it; a field re-added under an old name takes a new one.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "One field's provenance")
public class ProvenanceField {

  private List<Integer> path;
  private List<String> names;
  private Integer pid;

  @JsonCreator
  public ProvenanceField(@JsonProperty("path") List<Integer> path,
                          @JsonProperty("names") List<String> names,
                          @JsonProperty("pid") Integer pid) {
    this.path = path;
    this.names = names;
    this.pid = pid;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "The inlined index path from the "
      + "schema root. A struct field or union branch appends its index, an array element 0, a map "
      + "key 0 and a map value 1", example = "[1, 0]")
  @JsonProperty("path")
  public List<Integer> getPath() {
    return path;
  }

  @JsonProperty("path")
  public void setPath(List<Integer> path) {
    this.path = path;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "The same path in names, with [], "
      + "{key} and {value} for collection steps. Informational only; returned when verbose",
      example = "[\"home\", \"city\"]")
  @JsonProperty("names")
  public List<String> getNames() {
    return names;
  }

  @JsonProperty("names")
  public void setNames(List<String> names) {
    this.names = names;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "The provenance id: unique per "
      + "location, stable across versions while the field keeps its identity", example = "3")
  @JsonProperty("pid")
  public Integer getPid() {
    return pid;
  }

  @JsonProperty("pid")
  public void setPid(Integer pid) {
    this.pid = pid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProvenanceField that = (ProvenanceField) o;
    return Objects.equals(path, that.path)
        && Objects.equals(names, that.names)
        && Objects.equals(pid, that.pid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, names, pid);
  }

  @Override
  public String toString() {
    return "{path=" + path + ",names=" + names + ",pid=" + pid + "}";
  }
}
