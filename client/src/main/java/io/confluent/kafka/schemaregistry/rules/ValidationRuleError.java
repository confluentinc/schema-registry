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

import java.util.Objects;

public final class ValidationRuleError {
  private final ValidationRule rule;
  private final String fieldPath;
  private final String message;
  private final Throwable cause;

  public ValidationRuleError(
      ValidationRule rule, String fieldPath, String message, Throwable cause) {
    this.rule = rule;
    this.fieldPath = fieldPath;
    this.message = message;
    this.cause = cause;
  }

  public ValidationRule getRule() {
    return rule;
  }

  public String getFieldPath() {
    return fieldPath;
  }

  /**
   * Optional dynamic error message returned by the rule itself — non-null
   * when the rule expression returned a non-empty string explaining the
   * failure (e.g. {@code x > 0 ? '' : 'x must be positive'}). Null when
   * the failure was a plain {@code false} or a CEL evaluation error.
   */
  public String getMessage() {
    return message;
  }

  public Throwable getCause() {
    return cause;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(fieldPath == null || fieldPath.isEmpty() ? "<root>" : fieldPath);
    sb.append(": ");
    String name = rule != null ? rule.getName() : null;
    sb.append(name == null || name.isEmpty() ? "unnamed" : name);
    sb.append(": ");
    // Prefer the dynamic message returned by the rule itself; fall back to
    // the rule's authored doc / SQL / CEL expression in that order.
    String doc = rule != null ? rule.getDoc() : null;
    String sql = rule != null ? rule.getSql() : null;
    String expr = rule != null ? rule.getExpr() : null;
    if (message != null && !message.isEmpty()) {
      sb.append(message);
    } else if (doc != null && !doc.isEmpty()) {
      sb.append(doc);
    } else if (sql != null && !sql.isEmpty()) {
      sb.append(sql);
    } else {
      sb.append(expr);
    }
    if (cause != null) {
      sb.append(" (caused by: ").append(cause.getMessage()).append(")");
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ValidationRuleError)) {
      return false;
    }
    ValidationRuleError that = (ValidationRuleError) o;
    return Objects.equals(rule, that.rule)
        && Objects.equals(fieldPath, that.fieldPath)
        && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rule, fieldPath, message);
  }
}
