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
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Objects;
import java.util.SortedSet;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Rule {

  private String name;
  private RuleKind kind;
  private RuleMode mode;
  private String type;
  private SortedSet<String> annotations;
  private String expr;
  private String onSuccess;
  private String onFailure;

  @JsonCreator
  public Rule(@JsonProperty("name") String name,
              @JsonProperty("kind") RuleKind kind,
              @JsonProperty("mode") RuleMode mode,
              @JsonProperty("type") String type,
              @JsonProperty("annotations") SortedSet<String> annotations,
              @JsonProperty("expr") String expr,
              @JsonProperty("onSuccess") String onSuccess,
              @JsonProperty("onFailure") String onFailure) {
    this.name = name;
    this.kind = kind;
    this.mode = mode;
    this.type = type;
    this.annotations = annotations;
    this.expr = expr;
    this.onSuccess = onSuccess;
    this.onFailure = onFailure;
  }

  public Rule(io.confluent.kafka.schemaregistry.client.rest.entities.Rule rule) {
    this.name = rule.getName();
    this.kind = RuleKind.fromEntity(rule.getKind());
    this.mode = RuleMode.fromEntity(rule.getMode());
    this.type = rule.getType();
    this.annotations = rule.getAnnotations();
    this.expr = rule.getExpr();
    this.onSuccess = rule.getOnSuccess();
    this.onFailure = rule.getOnFailure();
  }

  @Schema(description = "Rule name")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @Schema(description = "Rule kind")
  @JsonProperty("kind")
  public RuleKind getKind() {
    return kind;
  }

  @JsonProperty("kind")
  public void setKind(RuleKind kind) {
    this.kind = kind;
  }

  @Schema(description = "Rule mode")
  @JsonProperty("mode")
  public RuleMode getMode() {
    return mode;
  }

  @JsonProperty("mode")
  public void setMode(RuleMode mode) {
    this.mode = mode;
  }

  @Schema(description = "Rule type")
  @JsonProperty("type")
  public String getType() {
    return this.type;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  @Schema(description = "The annotations to which this rule applies")
  @JsonProperty("annotations")
  public SortedSet<String> getAnnotations() {
    return annotations;
  }

  @JsonProperty("annotations")
  public void setAnnotations(SortedSet<String> annotations) {
    this.annotations = annotations;
  }

  @Schema(description = "Rule expression")
  @JsonProperty("expr")
  public String getExpr() {
    return expr;
  }

  @JsonProperty("expr")
  public void setExpr(String expr) {
    this.expr = expr;
  }

  @Schema(description = "Rule action on success")
  @JsonProperty("onSuccess")
  public String getOnSuccess() {
    return onSuccess;
  }

  @JsonProperty("onSuccess")
  public void setOnSuccess(String onSuccess) {
    this.onSuccess = onSuccess;
  }

  @Schema(description = "Rule action on failure")
  @JsonProperty("onFailure")
  public String getOnFailure() {
    return onFailure;
  }

  @JsonProperty("onFailure")
  public void setOnFailure(String onFailure) {
    this.onFailure = onFailure;
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
        && kind == rule.kind
        && mode == rule.mode
        && Objects.equals(type, rule.type)
        && Objects.equals(annotations, rule.annotations)
        && Objects.equals(expr, rule.expr)
        && Objects.equals(onSuccess, rule.onSuccess)
        && Objects.equals(onFailure, rule.onFailure);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, kind, mode, type, annotations, expr, onSuccess, onFailure);
  }

  @Override
  public String toString() {
    return "Rule{"
        + "name=" + name
        + ", kind=" + kind
        + ", mode=" + mode
        + ", type='" + type + '\''
        + ", annotations='" + annotations + '\''
        + ", expr='" + expr + '\''
        + ", onSuccess='" + onSuccess + '\''
        + ", onFailure='" + onFailure + '\''
        + '}';
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.Rule toRuleEntity() {
    return new io.confluent.kafka.schemaregistry.client.rest.entities.Rule(
        getName(),
        getKind().toEntity(),
        getMode().toEntity(),
        getType(),
        getAnnotations(),
        getExpr(),
        getOnSuccess(),
        getOnFailure()
    );
  }
}
