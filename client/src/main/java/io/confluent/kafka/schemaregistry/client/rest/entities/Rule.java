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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Rule")
public class Rule {

  private String name;
  private RuleKind kind;
  private RuleMode mode;
  private String type;
  private SortedSet<String> tags;
  private String expr;
  private String onSuccess;
  private String onFailure;
  private boolean disabled;

  @JsonCreator
  public Rule(@JsonProperty("name") String name,
              @JsonProperty("kind") RuleKind kind,
              @JsonProperty("mode") RuleMode mode,
              @JsonProperty("type") String type,
              @JsonProperty("tags") Set<String> tags,
              @JsonProperty("expr") String expr,
              @JsonProperty("onSuccess") String onSuccess,
              @JsonProperty("onFailure") String onFailure,
              @JsonProperty("disabled") boolean disabled) {
    this.name = name;
    this.kind = kind != null ? kind : RuleKind.TRANSFORM;
    this.mode = mode != null ? mode : RuleMode.WRITEREAD;
    this.type = type;
    SortedSet<String> sortedTags = tags != null
        ? tags.stream()
        .sorted()
        .collect(Collectors.toCollection(TreeSet::new))
        : Collections.emptySortedSet();
    this.tags = sortedTags;
    this.expr = expr;
    this.onSuccess = onSuccess;
    this.onFailure = onFailure;
    this.disabled = disabled;
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

  @Schema(description = "The tags to which this rule applies")
  @JsonProperty("tags")
  public SortedSet<String> getTags() {
    return tags;
  }

  @JsonProperty("tags")
  public void setTags(SortedSet<String> tags) {
    this.tags = tags;
  }

  @Schema(description = "Rule expression")
  @JsonProperty("expr")
  public String getExpr() {
    return expr;
  }

  @JsonProperty("expr")
  public void setExpression(String expr) {
    this.expr = expr;
  }

  /**
   * If the mode is WRITEREAD or UPDOWN, the on-success action can be a comma separated
   * pair of actions, such as "none,error".  The first action applies to WRITE (or UP)
   * and the second action applies to READ (or DOWN).
   */
  @Schema(description = "Rule action on success")
  @JsonProperty("onSuccess")
  public String getOnSuccess() {
    return onSuccess;
  }

  @JsonProperty("onSuccess")
  public void setOnSuccess(String onSuccess) {
    this.onSuccess = onSuccess;
  }

  /**
   * If the mode is WRITEREAD or UPDOWN, the on-failure action can be a comma separated
   * pair of actions, such as "none,error".  The first action applies to WRITE (or UP)
   * and the second action applies to READ (or DOWN).
   */
  @Schema(description = "Rule action on failure")
  @JsonProperty("onFailure")
  public String getOnFailure() {
    return onFailure;
  }

  @JsonProperty("onFailure")
  public void setOnFailure(String onFailure) {
    this.onFailure = onFailure;
  }

  @Schema(description = "Whether the rule is disabled")
  @JsonProperty("disabled")
  public boolean isDisabled() {
    return disabled;
  }

  @JsonProperty("disabled")
  public void setDisabled(boolean disabled) {
    this.disabled = disabled;
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
        && Objects.equals(tags, rule.tags)
        && Objects.equals(expr, rule.expr)
        && Objects.equals(onSuccess, rule.onSuccess)
        && Objects.equals(onFailure, rule.onFailure)
        && disabled == rule.disabled;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, kind, mode, type, tags, expr, onSuccess, onFailure, disabled);
  }

  @Override
  public String toString() {
    return "Rule{"
        + "name=" + name
        + ", kind=" + kind
        + ", mode=" + mode
        + ", type='" + type + '\''
        + ", tags='" + tags + '\''
        + ", expr='" + expr + '\''
        + ", onSuccess='" + onSuccess + '\''
        + ", onFailure='" + onFailure + '\''
        + ", disabled='" + disabled + '\''
        + '}';
  }

  public void updateHash(MessageDigest md) {
    if (name != null) {
      md.update(name.getBytes(StandardCharsets.UTF_8));
    }
    if (kind != null) {
      md.update(kind.name().getBytes(StandardCharsets.UTF_8));
    }
    if (mode != null) {
      md.update(mode.name().getBytes(StandardCharsets.UTF_8));
    }
    if (type != null) {
      md.update(type.getBytes(StandardCharsets.UTF_8));
    }
    if (tags != null) {
      tags.forEach(s -> md.update(s.getBytes(StandardCharsets.UTF_8)));
    }
    if (expr != null) {
      md.update(expr.getBytes(StandardCharsets.UTF_8));
    }
    if (onSuccess != null) {
      md.update(onSuccess.getBytes(StandardCharsets.UTF_8));
    }
    if (onFailure != null) {
      md.update(onFailure.getBytes(StandardCharsets.UTF_8));
    }
    if (disabled) {
      md.update((byte) 1);
    }
  }
}
