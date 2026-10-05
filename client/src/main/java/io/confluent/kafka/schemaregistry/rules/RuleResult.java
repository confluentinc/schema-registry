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

package io.confluent.kafka.schemaregistry.rules;

import io.confluent.kafka.schemaregistry.ParsedSchemaAndValue;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Outcome of a single rule's execution during deserialization, along with any
 * metadata the rule executor produced. One {@code RuleResult} is appended per
 * rule visited (including skipped rules) and surfaced via
 * {@link ParsedSchemaAndValue#getRuleResults()}.
 *
 * <p>Executor metadata is published in two parallel maps:
 * <ul>
 *   <li>{@link #messageMetadata()} — message-level key/value attributes
 *       (one map for the whole rule execution).</li>
 *   <li>{@link #fieldMetadata()} — per-field attributes, keyed by field
 *       path; each value is a key/value map specific to that field.</li>
 * </ul>
 *
 * <p>All values are {@code String}. Absent keys mean "not recorded" — null
 * values are never stored. Specific key conventions are documented by the
 * executor that produces them (for example, see
 * {@code EncryptionExecutor}).
 */
public final class RuleResult {

  /**
   * Outcome of a rule's execution.
   */
  public enum Result {
    /** The rule's transform ran, did not throw, and (for TRANSFORM rules)
     *  returned a non-null value. */
    SUCCESS,
    /** The rule failed. Any of:
     *  <ul>
     *    <li>the transform threw,</li>
     *    <li>a CONDITION rule returned false,</li>
     *    <li>a TRANSFORM rule returned null, or</li>
     *    <li>no executor was registered for the rule's type.</li>
     *  </ul>
     *  In all cases the rule's {@code onFailure} action runs; if
     *  {@link #errorMessage()} is set it reflects the underlying cause. */
    FAILURE,
    /** The rule was skipped — disabled, environment mismatch, or its mode
     *  did not match the current phase. The transform did not run. */
    SKIPPED
  }

  private final Rule rule;
  private final Result result;
  private final String errorMessage;
  private final Map<String, String> messageMetadata;
  private final Map<String, Map<String, String>> fieldMetadata;

  public RuleResult(
      Rule rule,
      Result result,
      String errorMessage,
      Map<String, String> messageMetadata,
      Map<String, Map<String, String>> fieldMetadata) {
    this.rule = rule;
    this.result = result;
    this.errorMessage = errorMessage;
    this.messageMetadata = messageMetadata != null ? messageMetadata : Collections.emptyMap();
    this.fieldMetadata = fieldMetadata != null ? fieldMetadata : Collections.emptyMap();
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
   * Message-level metadata produced by the executor. Always non-null;
   * empty when the executor produced nothing at the message level.
   */
  public Map<String, String> messageMetadata() {
    return messageMetadata;
  }

  /**
   * Per-field metadata produced by the executor, keyed by field path.
   * Always non-null; empty when the executor produced no field-level
   * metadata.
   */
  public Map<String, Map<String, String>> fieldMetadata() {
    return fieldMetadata;
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
        && Objects.equals(messageMetadata, that.messageMetadata)
        && Objects.equals(fieldMetadata, that.fieldMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rule, result, errorMessage, messageMetadata, fieldMetadata);
  }

  @Override
  public String toString() {
    return "RuleResult{rule=" + (rule != null ? rule.getName() : null)
        + ", result=" + result
        + (errorMessage != null ? ", errorMessage=" + errorMessage : "")
        + (messageMetadata.isEmpty() ? "" : ", messageMetadata=" + messageMetadata)
        + (fieldMetadata.isEmpty() ? "" : ", fieldMetadata=" + fieldMetadata)
        + '}';
  }
}
