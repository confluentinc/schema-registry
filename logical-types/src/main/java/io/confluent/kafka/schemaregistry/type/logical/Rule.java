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

package io.confluent.kafka.schemaregistry.type.logical;

import java.util.Objects;

/**
 * A validation rule attached to a {@link Schema} or {@link Schema.Field},
 * authored as a SQL-flavored {@code CHECK (...)} clause and translated to a
 * CEL expression for runtime evaluation.
 *
 * <p>Mirrors the {@code Meta.Rule} doc in {@code confluent/meta.proto} so
 * the wire form is uniform across Avro/JSON (as a {@code confluent:rules}
 * array property) and Protobuf (as a {@code repeated Rule rules} field on
 * {@code Meta}).
 *
 * <p>Field semantics:
 * <ul>
 *   <li>{@link #getName()} — optional machine identifier from
 *       {@code CONSTRAINT name CHECK (...)}; null if the user didn't supply
 *       one. Used in error reporting.</li>
 *   <li>{@link #getDoc()} — optional human-readable error text from
 *       {@code MESSAGE 'text'}; null if absent. Surfaced when the rule fails
 *       at runtime.</li>
 *   <li>{@link #getExpr()} — the translated CEL expression that the runtime
 *       evaluator executes. Always non-null.</li>
 *   <li>{@link #getSql()} — the original SQL CHECK expression text. Used
 *       by the DDL emitter to round-trip back to readable SQL. Always
 *       non-null. Named {@code sql} to match the existing convention in
 *       {@code Meta.Rule} that distinguishes "sql-form source" from the
 *       "expression form" stored in {@code expr}.</li>
 * </ul>
 *
 * <p>The {@code expr} field is the source of truth for runtime evaluation;
 * {@code sql} is the source of truth for DDL re-emission.
 */
public final class Rule {

  private final String name;
  private final String doc;
  private final String expr;
  private final String sql;

  public Rule(String name, String doc, String expr, String sql) {
    Objects.requireNonNull(expr, "expr");
    Objects.requireNonNull(sql, "sql");
    if (expr.isEmpty()) {
      throw new ValidationException("Rule expr must not be empty");
    }
    if (sql.isEmpty()) {
      throw new ValidationException("Rule sql must not be empty");
    }
    this.name = name;
    this.doc = doc;
    this.expr = expr;
    this.sql = sql;
  }

  public String getName() {
    return name;
  }

  public String getDoc() {
    return doc;
  }

  public String getExpr() {
    return expr;
  }

  public String getSql() {
    return sql;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Rule)) {
      return false;
    }
    Rule that = (Rule) o;
    return Objects.equals(name, that.name)
        && Objects.equals(doc, that.doc)
        && expr.equals(that.expr)
        && sql.equals(that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, doc, expr, sql);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Rule{");
    if (name != null) {
      sb.append("name=").append(name).append(", ");
    }
    sb.append("expr=").append(expr);
    if (doc != null) {
      sb.append(", doc=").append(doc);
    }
    sb.append("}");
    return sb.toString();
  }
}
