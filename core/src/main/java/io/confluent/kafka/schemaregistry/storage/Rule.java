/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Rule {

  private final String name;
  private final String doc;
  private final RuleKind kind;
  private final RuleMode mode;
  private final String type;
  private final SortedSet<String> tags;
  @JsonPropertyOrder(alphabetic = true)
  private final SortedMap<String, String> params;
  private final String expr;
  private final String onSuccess;
  private final String onFailure;
  private final boolean disabled;

  @JsonCreator
  public Rule(@JsonProperty("name") String name,
              @JsonProperty("doc") String doc,
              @JsonProperty("kind") RuleKind kind,
              @JsonProperty("mode") RuleMode mode,
              @JsonProperty("type") String type,
              @JsonProperty("tags") SortedSet<String> tags,
              @JsonProperty("params") SortedMap<String, String> params,
              @JsonProperty("expr") String expr,
              @JsonProperty("onSuccess") String onSuccess,
              @JsonProperty("onFailure") String onFailure,
              @JsonProperty("disabled") boolean disabled) {
    this.name = name;
    this.doc = doc;
    this.kind = kind;
    this.mode = mode;
    this.type = type;
    this.tags = tags != null ? Collections.unmodifiableSortedSet(tags) : null;
    this.params = params != null ? Collections.unmodifiableSortedMap(params) : null;
    this.expr = expr;
    this.onSuccess = onSuccess;
    this.onFailure = onFailure;
    this.disabled = disabled;
  }

  public Rule(io.confluent.kafka.schemaregistry.client.rest.entities.Rule rule) {
    this.name = rule.getName();
    this.doc = rule.getDoc();
    this.kind = RuleKind.fromEntity(rule.getKind());
    this.mode = RuleMode.fromEntity(rule.getMode());
    this.type = rule.getType();
    this.tags = Collections.unmodifiableSortedSet(rule.getTags());
    this.params = Collections.unmodifiableSortedMap(rule.getParams());
    this.expr = rule.getExpr();
    this.onSuccess = rule.getOnSuccess();
    this.onFailure = rule.getOnFailure();
    this.disabled = rule.isDisabled();
  }

  @Schema(description = "Rule name")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @Schema(description = "Rule doc")
  @JsonProperty("doc")
  public String getDoc() {
    return doc;
  }

  @Schema(description = "Rule kind")
  @JsonProperty("kind")
  public RuleKind getKind() {
    return kind;
  }

  @Schema(description = "Rule mode")
  @JsonProperty("mode")
  public RuleMode getMode() {
    return mode;
  }

  @Schema(description = "Rule type")
  @JsonProperty("type")
  public String getType() {
    return this.type;
  }

  @Schema(description = "The tags to which this rule applies")
  @JsonProperty("tags")
  public SortedSet<String> getTags() {
    return tags;
  }

  @Schema(description = "The optional params for the rule")
  @JsonProperty("params")
  public SortedMap<String, String> getParams() {
    return params;
  }

  @Schema(description = "Rule expression")
  @JsonProperty("expr")
  public String getExpr() {
    return expr;
  }

  @Schema(description = "Rule action on success")
  @JsonProperty("onSuccess")
  public String getOnSuccess() {
    return onSuccess;
  }

  @Schema(description = "Rule action on failure")
  @JsonProperty("onFailure")
  public String getOnFailure() {
    return onFailure;
  }

  @Schema(description = "Whether the rule is disabled")
  @JsonProperty("disabled")
  public boolean isDisabled() {
    return disabled;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Rule rule = (Rule) o;
    return Objects.equals(name, rule.name)
        && Objects.equals(doc, rule.doc)
        && kind == rule.kind
        && mode == rule.mode
        && Objects.equals(type, rule.type)
        && Objects.equals(tags, rule.tags)
        && Objects.equals(params, rule.params)
        && Objects.equals(expr, rule.expr)
        && Objects.equals(onSuccess, rule.onSuccess)
        && Objects.equals(onFailure, rule.onFailure)
        && disabled == rule.disabled;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, doc, kind, mode, type, tags, params,
        expr, onSuccess, onFailure, disabled);
  }

  @Override
  public String toString() {
    return "Rule{"
        + "name=" + name
        + ", doc=" + doc
        + ", kind=" + kind
        + ", mode=" + mode
        + ", type='" + type + '\''
        + ", tags='" + tags + '\''
        + ", params='" + params + '\''
        + ", expr='" + expr + '\''
        + ", onSuccess='" + onSuccess + '\''
        + ", onFailure='" + onFailure + '\''
        + ", disabled='" + disabled + '\''
        + '}';
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.Rule toRuleEntity() {
    return new io.confluent.kafka.schemaregistry.client.rest.entities.Rule(
        getName(),
        getDoc(),
        getKind().toEntity(),
        getMode().toEntity(),
        getType(),
        getTags(),
        getParams(),
        getExpr(),
        getOnSuccess(),
        getOnFailure(),
        isDisabled()
    );
  }
}
