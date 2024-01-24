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

package io.confluent.kafka.schemaregistry.maven;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.maven.plugins.annotations.Parameter;

public class RuleSet {

  // For mojo, cannot have any constructors besides default constructor

  @Parameter(required = false)
  protected List<Rule> migrationRules = new ArrayList<>();

  @Parameter(required = false)
  protected List<Rule> domainRules = new ArrayList<>();

  @Override
  public String toString() {
    return "RuleSet{"
        + "migrationRules=" + migrationRules
        + ", domainRules=" + domainRules
        + '}';
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet toRuleSetEntity() {
    return new io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet(
        migrationRules != null
            ? migrationRules.stream()
            .map(Rule::toRuleEntity)
            .collect(Collectors.toList())
            : null,
        domainRules != null
            ? domainRules.stream()
            .map(Rule::toRuleEntity)
            .collect(Collectors.toList())
            : null
    );
  }
}
