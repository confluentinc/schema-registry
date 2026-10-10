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
 * Its {@code kind} tells which: a location directly under a {@code STRUCT} is a field, and one
 * directly under a {@code UNION} a branch, following collection steps through the kind they pass.
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
  private String kind;
  private Integer pid;

  @JsonCreator
  public ProvenanceField(@JsonProperty("path") List<Integer> path,
                          @JsonProperty("names") List<String> names,
                          @JsonProperty("kind") String kind,
                          @JsonProperty("pid") Integer pid) {
    this.path = path;
    this.names = names;
    this.kind = kind;
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

  @io.swagger.v3.oas.annotations.media.Schema(description = "The location in the native schema's "
      + "or document's own names, one per native step: a field, property or union branch name, "
      + "or null for an Avro or JSON array element or map value (a Protobuf map value's step is "
      + "the entry's value field). Steps the logical type hides are included "
      + "and steps with no native form left out, so it need not match path step for step",
      example = "[\"items\", null, \"sku\"]")
  // An empty list is a location with no native step of its own, as a oneof or a root union's
  // branch: unlike null, it must reach the client.
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("names")
  public List<String> getNames() {
    return names;
  }

  @JsonProperty("names")
  public void setNames(List<String> names) {
    this.names = names;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "What a location's type is, references "
      + "resolved: SCALAR (a type with no locations under it: a primitive, an enum, a fixed or "
      + "a variant), STRUCT, UNION, or ARRAY<k>, MULTISET<k> or MAP<k, k> of the kinds they hold, "
      + "nested, a map's key kind first and the two separated by a comma and a space. A location "
      + "whose kind changes takes a new pid, as does everything under it",
      example = "MAP<SCALAR, STRUCT>")
  @JsonProperty("kind")
  public String getKind() {
    return kind;
  }

  @JsonProperty("kind")
  public void setKind(String kind) {
    this.kind = kind;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "The provenance id: unique per "
      + "location within a version, and equal across the response's versions exactly where the "
      + "location continues. Ids are numbered per response, so compare them only within one; a "
      + "metastore derives persistent column ids from them", example = "3")
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
        && Objects.equals(kind, that.kind)
        && Objects.equals(pid, that.pid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, names, kind, pid);
  }

  @Override
  public String toString() {
    return "{path=" + path + ",names=" + names + ",kind=" + kind + ",pid=" + pid + "}";
  }
}
