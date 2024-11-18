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

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTags;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Schema tags register request")
public class TagSchemaRequest {

  private static final String newVersionDescription =
      "The new version should be the latest version in the subject + 1."
          + " If set, the new version will be encoded to the schema metadata.";
  private Integer newVersion;
  private List<SchemaTags> tagsToAdd;
  private List<SchemaTags> tagsToRemove;
  private Metadata metadata;
  private RuleSet ruleSet;
  private RuleSet rulesToMerge;
  private List<String> rulesToRemove;

  public TagSchemaRequest() {
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = newVersionDescription)
  @JsonProperty("newVersion")
  public Integer getNewVersion() {
    return newVersion;
  }

  @JsonProperty("newVersion")
  public void setNewVersion(Integer newVersion) {
    this.newVersion = newVersion;
  }

  @JsonProperty("tagsToAdd")
  public List<SchemaTags> getTagsToAdd() {
    return tagsToAdd;
  }

  @JsonProperty("tagsToAdd")
  public void setTagsToAdd(List<SchemaTags> tagsToAdd) {
    this.tagsToAdd = tagsToAdd;
  }

  @JsonProperty("tagsToRemove")
  public List<SchemaTags> getTagsToRemove() {
    return tagsToRemove;
  }

  @JsonProperty("tagsToRemove")
  public void setTagsToRemove(List<SchemaTags> tagsToRemove) {
    this.tagsToRemove = tagsToRemove;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.METADATA_DESC)
  @JsonProperty("metadata")
  public Metadata getMetadata() {
    return metadata;
  }

  @JsonProperty("metadata")
  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = Schema.RULESET_DESC)
  @JsonProperty("ruleSet")
  public RuleSet getRuleSet() {
    return ruleSet;
  }

  @JsonProperty("ruleSet")
  public void setRuleSet(RuleSet ruleSet) {
    this.ruleSet = ruleSet;
  }

  @JsonProperty("rulesToMerge")
  public RuleSet getRulesToMerge() {
    return rulesToMerge;
  }

  @JsonProperty("rulesToMerge")
  public void setRulesToMerge(RuleSet rulesToMerge) {
    this.rulesToMerge = rulesToMerge;
  }

  @JsonProperty("rulesToRemove")
  public List<String> getRulesToRemove() {
    return rulesToRemove;
  }

  @JsonProperty("rulesToRemove")
  public void setRulesToRemove(List<String> rulesToRemove) {
    this.rulesToRemove = rulesToRemove;
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
    TagSchemaRequest tagSchemaRequest = (TagSchemaRequest) o;
    return Objects.equals(newVersion, tagSchemaRequest.newVersion)
        && Objects.equals(tagsToAdd, tagSchemaRequest.tagsToAdd)
        && Objects.equals(tagsToRemove, tagSchemaRequest.tagsToRemove)
        && Objects.equals(metadata, tagSchemaRequest.metadata)
        && Objects.equals(ruleSet, tagSchemaRequest.ruleSet)
        && Objects.equals(rulesToMerge, tagSchemaRequest.rulesToMerge)
        && Objects.equals(rulesToRemove, tagSchemaRequest.rulesToRemove);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newVersion, tagsToAdd, tagsToRemove, metadata,
        ruleSet, rulesToMerge, rulesToRemove);
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }

  public static Map<SchemaEntity, Set<String>> schemaTagsListToMap(List<SchemaTags> schemaTags) {
    if (schemaTags == null || schemaTags.isEmpty()) {
      return Collections.emptyMap();
    }
    return schemaTags
        .stream()
        .collect(Collectors.toMap(SchemaTags::getSchemaEntity,
            entry -> new LinkedHashSet<>(entry.getTags()),
            (v1, v2) -> {
              v1.addAll(v2);
              return v1;
            },
            LinkedHashMap::new));
  }
}
