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
import io.confluent.kafka.schemaregistry.rules.RuleException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rule set, which includes rules and a list of reference names for included rule sets.
 */

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleSet {

  private final List<Rule> migrationRules;
  private final List<Rule> domainRules;

  @JsonCreator
  public RuleSet(
      @JsonProperty("migrationRules") List<Rule> migrationRules,
      @JsonProperty("domainRules") List<Rule> domainRules
  ) {
    this.migrationRules = migrationRules != null
        ? Collections.unmodifiableList(migrationRules)
        : Collections.emptyList();
    this.domainRules = domainRules != null
        ? Collections.unmodifiableList(domainRules)
        : Collections.emptyList();
  }

  public List<Rule> getMigrationRules() {
    return migrationRules;
  }

  public List<Rule> getDomainRules() {
    return domainRules;
  }

  public boolean hasRules(RuleMode mode) {
    switch (mode) {
      case UPGRADE:
      case DOWNGRADE:
        return getMigrationRules().stream().anyMatch(r -> r.getMode() == mode
            || r.getMode() == RuleMode.UPDOWN);
      case UPDOWN:
        return getMigrationRules().stream().anyMatch(r -> r.getMode() == mode);
      case WRITE:
      case READ:
        return getDomainRules().stream().anyMatch(r -> r.getMode() == mode
            || r.getMode() == RuleMode.WRITEREAD);
      case WRITEREAD:
        return getDomainRules().stream().anyMatch(r -> r.getMode() == mode);
      default:
        return false;
    }
  }

  public boolean hasRulesWithType(String type) {
    return getDomainRules().stream().anyMatch(r -> type.equals(r.getType()))
        || getMigrationRules().stream().anyMatch(r -> type.equals(r.getType()));
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
        && Objects.equals(domainRules, ruleSet.domainRules);
  }

  @Override
  public int hashCode() {
    return Objects.hash(migrationRules, domainRules);
  }

  @Override
  public String toString() {
    return "Rules{"
        + "migrationRules=" + migrationRules
        + ", domainRules=" + domainRules
        + '}';
  }

  public void updateHash(MessageDigest md) {
    if (migrationRules != null) {
      migrationRules.forEach(r -> r.updateHash(md));
    }
    if (domainRules != null) {
      domainRules.forEach(r -> r.updateHash(md));
    }
  }

  public void validate() throws RuleException {
    Set<String> names = new HashSet<>();
    if (migrationRules != null) {
      for (Rule rule : migrationRules) {
        String name = rule.getName();
        if (names.contains(name)) {
          throw new RuleException("Found rule with duplicate name '" + name + "'");
        }
        names.add(name);
        rule.validate();
        if (!rule.getMode().isMigrationRule()) {
          throw new RuleException("Migration rules can only be UPGRADE, DOWNGRADE, UPDOWN");
        }
      }
    }
    if (domainRules != null) {
      for (Rule rule : domainRules) {
        String name = rule.getName();
        if (names.contains(name)) {
          throw new RuleException("Found rule with duplicate name '" + name + "'");
        }
        names.add(name);
        rule.validate();
        if (!rule.getMode().isDomainRule()) {
          throw new RuleException("Domain rules can only be WRITE, READ, WRITEREAD");
        }
      }
    }
  }

  public static RuleSet mergeRuleSets(RuleSet oldRuleSet, RuleSet newRuleSet) {
    if (oldRuleSet == null) {
      return newRuleSet;
    } else if (newRuleSet == null) {
      return oldRuleSet;
    } else {
      return new RuleSet(
          merge(oldRuleSet.migrationRules, newRuleSet.migrationRules),
          merge(oldRuleSet.domainRules, newRuleSet.domainRules)
      );
    }
  }

  private static List<Rule> merge(List<Rule> oldRules, List<Rule> newRules) {
    if (oldRules == null || oldRules.isEmpty()) {
      return newRules;
    } else if (newRules == null || newRules.isEmpty()) {
      return oldRules;
    } else {
      Set<String> newRuleNames = newRules.stream()
          .map(Rule::getName)
          .collect(Collectors.toSet());
      // Remove any old rules with the same name as one in new rules
      List<Rule> filteredOldRules = oldRules.stream()
          .filter(r -> !newRuleNames.contains(r.getName()))
          .collect(Collectors.toCollection(ArrayList::new));
      filteredOldRules.addAll(newRules);
      return filteredOldRules;
    }
  }
}
