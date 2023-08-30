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

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.rest.handlers.UpdateRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleSetHandler implements UpdateRequestHandler {

  private static final Logger log = LoggerFactory.getLogger(RuleSetHandler.class);

  public RuleSetHandler() {
  }

  @Override
  public void handle(String subject, ConfigUpdateRequest request) {
    if (request.getDefaultRuleSet() != null || request.getOverrideRuleSet() != null) {
      log.warn("RuleSets are only supported by Confluent Enterprise and Confluent Cloud");
      request.setDefaultRuleSet(null);
      request.setOverrideRuleSet(null);
    }
  }

  @Override
  public void handle(String subject, boolean normalize, RegisterSchemaRequest request) {
    if (request.getRuleSet() != null) {
      log.warn("RuleSets are only supported by Confluent Enterprise and Confluent Cloud");
      request.setRuleSet(null);
    }
  }

  @Override
  public void handle(Schema schema, TagSchemaRequest request) {
    if (request.getRuleSet() != null) {
      log.warn("RuleSets are only supported by Confluent Enterprise and Confluent Cloud");
      request.setRuleSet(null);
    }
  }

  public RuleSet transform(io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet ruleSet) {
    return null;
  }
}
