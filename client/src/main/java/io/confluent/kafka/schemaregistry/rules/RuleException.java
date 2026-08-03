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

package io.confluent.kafka.schemaregistry.rules;

import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;

/**
 * A schema rule exception.
 */
public class RuleException extends Exception {

  private final Rule rule;

  public RuleException() {
    this.rule = null;
  }

  public RuleException(Throwable cause) {
    super(cause);
    this.rule = null;
  }

  public RuleException(String message) {
    super(message);
    this.rule = null;
  }

  public RuleException(String message, Throwable cause) {
    super(message, cause);
    this.rule = null;
  }

  public RuleException(Rule rule, Throwable cause) {
    super(cause);
    this.rule = rule;
  }

  public RuleException(Rule rule, String message) {
    super(message);
    this.rule = rule;
  }

  public RuleException(Rule rule, String message, Throwable cause) {
    super(message, cause);
    this.rule = rule;
  }

  /**
   * Returns the rule that caused this exception, or null if unknown.
   */
  public Rule getRule() {
    return rule;
  }
}
