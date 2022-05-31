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
import java.util.Objects;
import java.util.SortedSet;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Rule")
public class Rule {

  private String name;
  private RuleKind kind;
  private RuleMode mode;
  private String type;
  private SortedSet<String> annotations;
  private String body;

  @JsonCreator
  public Rule(@JsonProperty("name") String name,
              @JsonProperty("kind") RuleKind kind,
              @JsonProperty("mode") RuleMode mode,
              @JsonProperty("type") String type,
              @JsonProperty("annotations") SortedSet<String> annotations,
              @JsonProperty("body") String body) {
    this.name = name;
    this.kind = kind;
    this.mode = mode;
    this.type = type;
    this.annotations = annotations;
    this.body = body;
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

  @Schema(description = "Rule body")
  @JsonProperty("body")
  public String getBody() {
    return body;
  }

  @JsonProperty("body")
  public void setBody(String body) {
    this.body = body;
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
        && Objects.equals(body, rule.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, kind, mode, type, annotations, body);
  }

  @Override
  public String toString() {
    return "Rule{"
        + "name=" + name
        + ", kind=" + kind
        + ", mode=" + mode
        + ", type='" + type + '\''
        + ", annotations='" + annotations + '\''
        + ", body='" + body + '\''
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
    if (annotations != null) {
      annotations.forEach(s -> md.update(s.getBytes(StandardCharsets.UTF_8)));
    }
    if (body != null) {
      md.update(body.getBytes(StandardCharsets.UTF_8));
    }
  }
}
