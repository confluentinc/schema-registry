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

public class ValidationRule {
  private final String name;
  private final String doc;
  private final String expr;
  private final String sql;

  public ValidationRule(String name, String doc, String expr, String sql) {
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
    if (!(o instanceof ValidationRule)) {
      return false;
    }
    ValidationRule that = (ValidationRule) o;
    return Objects.equals(name, that.name)
        && Objects.equals(doc, that.doc)
        && Objects.equals(expr, that.expr)
        && Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, doc, expr, sql);
  }

  @Override
  public String toString() {
    return "ValidationRule{name=" + name
        + ", doc=" + doc
        + ", expr=" + expr
        + ", sql=" + sql
        + "}";
  }
}
