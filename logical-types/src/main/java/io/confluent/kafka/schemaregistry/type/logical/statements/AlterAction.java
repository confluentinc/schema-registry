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

package io.confluent.kafka.schemaregistry.type.logical.statements;

import java.util.List;
import java.util.Map;

/**
 * One action of an {@code ALTER TYPE} statement, as a tool's parser reads it. Types, field
 * definitions and CHECK expressions are the statement's text, as written.
 */
public abstract class AlterAction {

  private AlterAction() {
  }

  /**
   * A CHECK constraint: its name or null, its SQL expression, and its message or null.
   */
  public static final class Check {

    private final String name;
    private final String sql;
    private final String message;

    public Check(String name, String sql, String message) {
      this.name = name;
      this.sql = sql;
      this.message = message;
    }

    public String name() {
      return name;
    }

    public String sql() {
      return sql;
    }

    public String message() {
      return message;
    }
  }

  /**
   * {@code ADD ATTRIBUTE name field}, where field is a type plus any field clauses.
   */
  public static final class AddAttribute extends AlterAction {

    private final String name;
    private final String field;

    public AddAttribute(String name, String field) {
      this.name = name;
      this.field = field;
    }

    public String name() {
      return name;
    }

    public String field() {
      return field;
    }
  }

  /**
   * {@code DROP ATTRIBUTE [IF EXISTS] name}.
   */
  public static final class DropAttribute extends AlterAction {

    private final String name;
    private final boolean ifExists;

    public DropAttribute(String name, boolean ifExists) {
      this.name = name;
      this.ifExists = ifExists;
    }

    public String name() {
      return name;
    }

    public boolean ifExists() {
      return ifExists;
    }
  }

  /**
   * {@code RENAME ATTRIBUTE from TO to}.
   */
  public static final class RenameAttribute extends AlterAction {

    private final String from;
    private final String to;

    public RenameAttribute(String from, String to) {
      this.from = from;
      this.to = to;
    }

    public String from() {
      return from;
    }

    public String to() {
      return to;
    }
  }

  /**
   * {@code ALTER ATTRIBUTE name [SET DATA] TYPE type}.
   */
  public static final class AlterAttributeType extends AlterAction {

    private final String name;
    private final String type;

    public AlterAttributeType(String name, String type) {
      this.name = name;
      this.type = type;
    }

    public String name() {
      return name;
    }

    public String type() {
      return type;
    }
  }

  /**
   * {@code SET DATA TYPE}, for a type whose root is not a struct.
   */
  public static final class SetDataType extends AlterAction {

    private final String type;

    public SetDataType(String type) {
      this.type = type;
    }

    public String type() {
      return type;
    }
  }

  /**
   * {@code ADD VALUE}, for an enum; before or after names an existing symbol.
   */
  public static final class AddValue extends AlterAction {

    private final String symbol;
    private final String before;
    private final String after;

    public AddValue(String symbol, String before, String after) {
      this.symbol = symbol;
      this.before = before;
      this.after = after;
    }

    public String symbol() {
      return symbol;
    }

    public String before() {
      return before;
    }

    public String after() {
      return after;
    }
  }

  /**
   * {@code SET WITH (...)}: the properties, with the format and policy among them.
   */
  public static final class SetWith extends AlterAction {

    private final Map<String, String> with;

    public SetWith(Map<String, String> with) {
      this.with = with;
    }

    public Map<String, String> with() {
      return with;
    }
  }

  /**
   * {@code ADD [CONSTRAINT name] CHECK (...)}, on the struct.
   */
  public static final class AddConstraint extends AlterAction {

    private final Check check;

    public AddConstraint(Check check) {
      this.check = check;
    }

    public Check check() {
      return check;
    }
  }

  /**
   * {@code DROP CONSTRAINT}, of the struct or of an attribute.
   */
  public static final class DropConstraint extends AlterAction {

    private final String name;
    private final boolean ifExists;

    public DropConstraint(String name, boolean ifExists) {
      this.name = name;
      this.ifExists = ifExists;
    }

    public String name() {
      return name;
    }

    public boolean ifExists() {
      return ifExists;
    }
  }

  /**
   * The type's comment; null drops it.
   */
  public static final class SetComment extends AlterAction {

    private final String comment;

    public SetComment(String comment) {
      this.comment = comment;
    }

    public String comment() {
      return comment;
    }
  }

  /**
   * The type's tags; empty drops them.
   */
  public static final class SetTags extends AlterAction {

    private final List<String> tags;

    public SetTags(List<String> tags) {
      this.tags = tags;
    }

    public List<String> tags() {
      return tags;
    }
  }

  /**
   * An attribute's comment; null drops it.
   */
  public static final class SetAttributeComment extends AlterAction {

    private final String name;
    private final String comment;

    public SetAttributeComment(String name, String comment) {
      this.name = name;
      this.comment = comment;
    }

    public String name() {
      return name;
    }

    public String comment() {
      return comment;
    }
  }

  /**
   * An attribute's tags; empty drops them.
   */
  public static final class SetAttributeTags extends AlterAction {

    private final String name;
    private final List<String> tags;

    public SetAttributeTags(String name, List<String> tags) {
      this.name = name;
      this.tags = tags;
    }

    public String name() {
      return name;
    }

    public List<String> tags() {
      return tags;
    }
  }

  /**
   * Parameters merged into an attribute's.
   */
  public static final class SetAttributeWith extends AlterAction {

    private final String name;
    private final Map<String, String> with;

    public SetAttributeWith(String name, Map<String, String> with) {
      this.name = name;
      this.with = with;
    }

    public String name() {
      return name;
    }

    public Map<String, String> with() {
      return with;
    }
  }

  /**
   * {@code ADD [CONSTRAINT name] CHECK (...)}, on an attribute.
   */
  public static final class AddAttributeCheck extends AlterAction {

    private final String name;
    private final Check check;

    public AddAttributeCheck(String name, Check check) {
      this.name = name;
      this.check = check;
    }

    public String name() {
      return name;
    }

    public Check check() {
      return check;
    }
  }
}
