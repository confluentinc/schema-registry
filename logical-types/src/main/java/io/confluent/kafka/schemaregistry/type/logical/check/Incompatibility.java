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

package io.confluent.kafka.schemaregistry.type.logical.check;

import java.util.Objects;

/**
 * A single schema-evolution violation found by {@link LogicalTypeChecker#compare}.
 *
 * <p>Carries a machine-readable {@link Rule} plus the path of the offending field so callers can
 * surface the exact location rather than a whole-schema failure.
 *
 * <p>Every rule here concerns the <em>change</em> between two schemas. A schema that no consumer
 * can use regardless of what preceded it yields an {@link Invalidity} instead.
 *
 * <p>Path syntax: field names are dot-joined,
 * {@code []} denotes an array element, and <code>{}</code> denotes a map value. For example
 * {@code order.items[].price}.
 */
public class Incompatibility {

  /**
   * The evolution rule that was violated. Names mirror the exception types raised by the equivalent
   * Iceberg-schema checker, so findings can be cross-referenced between the two.
   */
  public enum Rule {
    /** A field was added that is neither nullable nor defaulted. */
    REQUIRED_FIELD_ADDED,
    /** A field present in the original schema is missing from the update. */
    FIELD_DELETED,
    /** Fields common to both schemas no longer appear in the same relative order. */
    FIELD_REORDERED,
    /** A nullable field was tightened to non-nullable. */
    NULLABLE_TO_NON_NULLABLE,
    /** A non-nullable field lost the default that made its absence readable. */
    NON_NULLABLE_DEFAULT_REMOVED,
    /** A map's key type changed. */
    MAP_KEY_TYPE_MISMATCH,
    /** The structural kind changed (e.g. STRUCT became a primitive, or ARRAY became MAP). */
    TYPE_MISMATCH,
    /** A primitive type changed in a way the target type system does not allow. */
    UNSUPPORTED_TYPE_CHANGE
  }

  private final Rule rule;
  private final String path;
  private final String message;

  public Incompatibility(Rule rule, String path, String message) {
    this.rule = Objects.requireNonNull(rule, "rule");
    this.path = path == null ? "" : path;
    this.message = Objects.requireNonNull(message, "message");
  }

  public Rule getRule() {
    return rule;
  }

  /** The dot-joined path of the offending field, or the empty string at the schema root. */
  public String getPath() {
    return path;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Incompatibility)) {
      return false;
    }
    Incompatibility that = (Incompatibility) o;
    return rule == that.rule
        && path.equals(that.path)
        && message.equals(that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rule, path, message);
  }

  @Override
  public String toString() {
    return path.isEmpty()
        ? rule + ": " + message
        : rule + " at '" + path + "': " + message;
  }
}
