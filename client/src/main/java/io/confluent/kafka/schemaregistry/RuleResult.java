/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry;

import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Result of a single rule's execution during deserialization, along with any
 * data the rule executor produced. One {@code RuleResult} is appended per
 * rule visited (including skipped rules) and surfaced via
 * {@link ParsedSchemaAndValue#getRuleResults()}.
 */
public final class RuleResult {

  /**
   * Result of a rule's execution.
   */
  public enum Result {
    /** The rule's transform ran without throwing. */
    SUCCESS,
    /** The rule's transform threw, or a CONDITION rule returned false. */
    FAILURE,
    /** The rule was skipped — disabled, environment mismatch, or its mode
     *  did not match the current phase. The transform did not run. */
    SKIPPED
  }

  private final Rule rule;
  private final Result result;
  private final String errorMessage;
  private final Map<String, Object> data;

  public RuleResult(
      Rule rule,
      Result result,
      String errorMessage,
      Map<String, Object> data) {
    this.rule = rule;
    this.result = result;
    this.errorMessage = errorMessage;
    this.data = data != null ? data : Collections.emptyMap();
  }

  public Rule rule() {
    return rule;
  }

  public Result result() {
    return result;
  }

  /**
   * The exception message when {@link #result()} is {@link Result#FAILURE};
   * {@code null} otherwise.
   */
  public String errorMessage() {
    return errorMessage;
  }

  /**
   * Per-rule data produced by the executor. The shape of this map is defined
   * by the executor that ran. For example, the ENCRYPT executor writes
   * {@code "fields" -> List<EncryptResult>}.
   *
   * <p>Always non-null; empty when the executor produced nothing.
   */
  public Map<String, Object> data() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RuleResult)) {
      return false;
    }
    RuleResult that = (RuleResult) o;
    return Objects.equals(rule, that.rule)
        && result == that.result
        && Objects.equals(errorMessage, that.errorMessage)
        && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rule, result, errorMessage, data);
  }

  @Override
  public String toString() {
    return "RuleResult{rule=" + (rule != null ? rule.getName() : null)
        + ", result=" + result
        + (errorMessage != null ? ", errorMessage=" + errorMessage : "")
        + (data.isEmpty() ? "" : ", data=" + data)
        + '}';
  }
}
