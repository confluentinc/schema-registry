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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.swagger.v3.oas.annotations.media.Schema;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Rule")
public class Rule {

  public static final int NAME_MAX_LENGTH = 64;

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
              @JsonProperty("tags") Set<String> tags,
              @JsonProperty("params") Map<String, String> params,
              @JsonProperty("expr") String expr,
              @JsonProperty("onSuccess") String onSuccess,
              @JsonProperty("onFailure") String onFailure,
              @JsonProperty("disabled") boolean disabled) {
    this.name = name;
    this.doc = doc;
    this.kind = kind != null ? kind : RuleKind.TRANSFORM;
    this.mode = mode != null ? mode : RuleMode.WRITEREAD;
    this.type = type;
    SortedSet<String> sortedTags = tags != null
        ? tags.stream()
        .sorted()
        .collect(Collectors.toCollection(TreeSet::new))
        : Collections.emptySortedSet();
    SortedMap<String, String> sortedParams = params != null
        ? new TreeMap<>(params)
        : Collections.emptySortedMap();
    this.tags = Collections.unmodifiableSortedSet(sortedTags);
    this.params = Collections.unmodifiableSortedMap(sortedParams);
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

  @Schema(description = "Optional params for the rule")
  @JsonProperty("params")
  public SortedMap<String, String> getParams() {
    return params;
  }

  @Schema(description = "Rule expression")
  @JsonProperty("expr")
  public String getExpr() {
    return expr;
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

  public void updateHash(MessageDigest md) {
    if (name != null) {
      md.update(name.getBytes(StandardCharsets.UTF_8));
    }
    if (doc != null) {
      md.update(doc.getBytes(StandardCharsets.UTF_8));
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
    if (params != null) {
      params.forEach((key, value) -> {
        md.update(key.getBytes(StandardCharsets.UTF_8));
        md.update(value.getBytes(StandardCharsets.UTF_8));
      });
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

  public void validate() throws RuleException {
    validateName(name);
    if (type == null) {
      throw new RuleException("Missing rule type");
    }
    validateAction(mode, onSuccess);
    validateAction(mode, onFailure);
  }

  private static void validateName(String name) throws RuleException {
    if (name == null) {
      throw new RuleException("Missing rule name");
    }
    int length = name.length();
    if (length == 0) {
      throw new RuleException("Empty rule name");
    }
    if (length > NAME_MAX_LENGTH) {
      throw new RuleException("Rule name too long");
    }
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      throw new RuleException("Illegal initial character in rule name: " + name);
    }
    for (int i = 1; i < length; i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_' || c == '-')) {
        throw new RuleException("Illegal character in rule name: " + name);
      }
    }
  }

  private static void validateAction(RuleMode mode, String action) throws RuleException {
    if (mode != null
        && mode != RuleMode.WRITEREAD
        && mode != RuleMode.UPDOWN
        && action != null
        && action.indexOf(',') >= 0) {
      throw new RuleException("Multiple actions only valid with WRITEREAD and UPDOWN");
    }
  }
}
