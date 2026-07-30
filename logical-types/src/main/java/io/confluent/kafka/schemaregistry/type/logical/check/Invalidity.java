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
 * A single problem with one schema, found by {@link LogicalTypeChecker#validate}.
 *
 * <p>The counterpart of {@link Incompatibility}, which describes a problem with a <em>pair</em> of
 * schemas. The two are kept as separate types, with separate {@code Rule} enums, because the
 * questions differ: an {@link Incompatibility} says a change is unsafe, while an
 * {@link Invalidity} says a schema cannot be used by the consumer at all — whether or not anything
 * preceded it.
 *
 * <p>Path syntax matches {@link Incompatibility}: field names are dot-joined, {@code []} denotes an
 * array or multiset element, <code>{}</code> denotes a map value, and <code>{key}</code> denotes a
 * map key. For example {@code order.items[].price}.
 */
public class Invalidity {

  /** The validity rule that was violated. */
  public enum Rule {
    /** A struct or union has no fields, so it derives to a row type with no columns. */
    EMPTY_STRUCT,
    /** A named type refers to itself, directly or through other named types. */
    CYCLIC_TYPE,
    /** A named-type reference has no matching entry in the schema's named types. */
    UNRESOLVED_TYPE_REF,
    /** A precision falls outside the range the type permits. */
    PRECISION_OUT_OF_RANGE,
    /** A decimal scale is negative, or exceeds its own precision. */
    SCALE_OUT_OF_RANGE,
    /** A character-string or binary-string length falls outside the range the type permits. */
    LENGTH_OUT_OF_RANGE,
    /** The consumer's type system cannot represent the type at all. */
    UNREPRESENTABLE_TYPE
  }

  private final Rule rule;
  private final String path;
  private final String message;

  public Invalidity(Rule rule, String path, String message) {
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
    if (!(o instanceof Invalidity)) {
      return false;
    }
    Invalidity that = (Invalidity) o;
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
