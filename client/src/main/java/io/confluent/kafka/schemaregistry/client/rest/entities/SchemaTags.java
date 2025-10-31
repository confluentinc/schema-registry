/*
 * Copyright 2023 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import java.util.List;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY,
    fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaTags {

  private SchemaEntity schemaEntity;
  private List<String> tags;

  @JsonCreator
  public SchemaTags(@JsonProperty("schemaEntity") SchemaEntity schemaEntity,
                    @JsonProperty("tags") List<String> tags) {
    this.schemaEntity = schemaEntity;
    this.tags = tags;
  }

  @JsonProperty("schemaEntity")
  public SchemaEntity getSchemaEntity() {
    return schemaEntity;
  }

  @JsonProperty("schemaEntity")
  public void setSchemaEntity(SchemaEntity schemaEntity) {
    this.schemaEntity = schemaEntity;
  }

  @JsonProperty("tags")
  public List<String> getTags() {
    return tags;
  }

  @JsonProperty("tags")
  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public String toJson() throws JsonProcessingException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaTags other = (SchemaTags) o;
    return Objects.equals(this.schemaEntity, other.schemaEntity)
        && Objects.equals(this.tags, other.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaEntity, tags);
  }

  @Override
  public String toString() {
    return "SchemaTags{"
        + "schemaEntity=" + schemaEntity
        + ", tags=" + tags
        + '}';
  }
}
