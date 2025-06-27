/*
 * Copyright 2018 Confluent Inc.
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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Rule set, which includes migration rules (for migrating between versions), domain rules
 * (for business logic), and encoding rules (for encoding logic).
 */

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleSet {

  private final List<Rule> migrationRules;
  private final List<Rule> domainRules;
  private final List<Rule> encodingRules;

  @JsonCreator
  public RuleSet(
      @JsonProperty("migrationRules") List<Rule> migrationRules,
      @JsonProperty("domainRules") List<Rule> domainRules,
      @JsonProperty("encodingRules") List<Rule> encodingRules
  ) {
    this.migrationRules = migrationRules != null
        ? Collections.unmodifiableList(migrationRules)
        : Collections.emptyList();
    this.domainRules = domainRules != null
        ? Collections.unmodifiableList(domainRules)
        : Collections.emptyList();
    this.encodingRules = encodingRules != null
        ? Collections.unmodifiableList(encodingRules)
        : Collections.emptyList();
  }

  public RuleSet(io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet ruleSet) {
    this.migrationRules = ruleSet.getMigrationRules().stream()
        .map(Rule::new)
        .collect(Collectors.toList());
    this.domainRules = ruleSet.getDomainRules().stream()
        .map(Rule::new)
        .collect(Collectors.toList());
    this.encodingRules = ruleSet.getEncodingRules().stream()
        .map(Rule::new)
        .collect(Collectors.toList());
  }

  public List<Rule> getMigrationRules() {
    return migrationRules;
  }

  public List<Rule> getDomainRules() {
    return domainRules;
  }

  public List<Rule> getEncodingRules() {
    return encodingRules;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RuleSet ruleSet = (RuleSet) o;
    return Objects.equals(migrationRules, ruleSet.migrationRules)
        && Objects.equals(domainRules, ruleSet.domainRules)
        && Objects.equals(encodingRules, ruleSet.encodingRules);
  }

  @Override
  public int hashCode() {
    return Objects.hash(migrationRules, domainRules, encodingRules);
  }

  @Override
  public String toString() {
    return "Rules{"
        + "migrationRules=" + migrationRules
        + ", domainRules=" + domainRules
        + ", encodingRules=" + encodingRules
        + '}';
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet toRuleSetEntity() {
    return new io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet(
        getMigrationRules().stream()
            .map(Rule::toRuleEntity)
            .collect(Collectors.toList()),
        getDomainRules().stream()
            .map(Rule::toRuleEntity)
            .collect(Collectors.toList()),
        getEncodingRules().stream()
            .map(Rule::toRuleEntity)
            .collect(Collectors.toList())
    );
  }
}
