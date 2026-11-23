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

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesParserFactory;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesSchemaVisitor;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.Token;

/**
 * Turns the raw text of type statements into logical types, by wrapping it in a logical types
 * script.
 */
public final class TypeScripts {

  /** How a created type's body was written, which decides the script it becomes. */
  public enum BodyKind {
    STRUCT, ENUM, OTHER
  }

  private static final String SCRATCH_STRUCT = "__scratch";

  private TypeScripts() {
  }

  /**
   * The script for a created type. A struct or enum is declared under its simple name; any other
   * type is the script's root type expression. Aliases map type names, as written, to the full
   * names their schemas declare.
   */
  public static String createScript(String simpleName, BodyKind kind, String body,
      Map<String, String> params, Map<String, String> aliases) {
    return createScript(simpleName, kind, body, "", params, aliases);
  }

  /** As above, with a struct's or enum's COMMENT and TAGS clauses, as written. */
  public static String createScript(String simpleName, BodyKind kind, String body,
      String clauses, Map<String, String> params, Map<String, String> aliases) {
    StringBuilder sb = new StringBuilder();
    aliases.forEach((name, target) -> sb.append("USING TYPE ").append(qualified(name))
        .append(" FOR ").append(qualified(target)).append(";\n"));
    switch (kind) {
      case STRUCT:
        sb.append("STRUCT ").append(quote(simpleName)).append(' ').append(body);
        break;
      case ENUM:
        sb.append("ENUM ").append(quote(simpleName)).append(' ').append(body);
        break;
      default:
        if (!params.isEmpty()) {
          throw new TypeRefusedException("WITH properties " + params.keySet()
              + " are only allowed on a struct or enum type");
        }
        sb.append("TYPE ").append(body);
        return sb.toString();
    }
    if (!clauses.isEmpty()) {
      sb.append(' ').append(clauses);
    }
    if (!params.isEmpty()) {
      sb.append(" WITH (").append(params.entrySet().stream()
          .map(e -> literal(e.getKey()) + " = " + literal(e.getValue()))
          .collect(Collectors.joining(", "))).append(')');
    }
    return sb.toString();
  }

  /** The script whose only field is the given field definition, for an added attribute. */
  public static String fieldScript(String name, String field, Map<String, String> aliases) {
    return createScript(SCRATCH_STRUCT, BodyKind.STRUCT, "(" + quote(name) + " " + field + ")",
        Map.of(), aliases);
  }

  /**
   * Whether an attribute's text is one field definition and nothing after it, as a parenthesis
   * that closes its struct and a statement of another type would be. Text that is no field
   * definition is left to the parse of its script, which says why.
   */
  public static boolean isOneField(String name, String field) {
    LogicalTypesParser parser = LogicalTypesParserFactory.newParser(quote(name) + " " + field);
    try {
      parser.fieldDef();
    } catch (RuntimeException e) {
      return true;
    }
    return parser.getCurrentToken().getType() == Token.EOF;
  }

  /** The script whose root is the given type expression. */
  public static String typeScript(String type, Map<String, String> aliases) {
    return createScript(SCRATCH_STRUCT, BodyKind.OTHER, type, Map.of(), aliases);
  }

  public static LogicalType parse(String script) {
    return parse(script, false);
  }

  /**
   * As {@link #parse(String)}; with severalMessages, a script with no TYPE and several structs
   * is rooted at all of them, as the registry shows a Protobuf file of several messages.
   */
  public static LogicalType parse(String script, boolean severalMessages) {
    try {
      LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor(severalMessages);
      visitor.visit(LogicalTypesParserFactory.parse(script));
      return visitor.toLogicalType();
    } catch (RuntimeException e) {
      throw new TypeRefusedException("Invalid type: " + e.getMessage(), e);
    }
  }

  public static LogicalType withNamespace(LogicalType lt, String namespace) {
    return new LogicalType(lt.getName(), namespace, lt.getRootSchema(), lt.getNamedTypes(),
        lt.getExternalTypes(), lt.getExternalImports(), lt.getReferences(),
        lt.getResolvedReferences(), lt.getDefaultValues());
  }

  public static LogicalType withRoot(LogicalType lt, Schema root,
      Map<String, Schema> namedTypes) {
    return new LogicalType(lt.getName(), lt.getNamespace(), root, namedTypes,
        lt.getExternalTypes(), lt.getExternalImports(), lt.getReferences(),
        lt.getResolvedReferences(), Map.of());
  }

  public static LogicalType withReferences(LogicalType lt, List<SchemaReference> references,
      Map<String, String> resolved) {
    return new LogicalType(lt.getName(), lt.getNamespace(), lt.getRootSchema(),
        lt.getNamedTypes(), lt.getExternalTypes(), lt.getExternalImports(), references,
        resolved, lt.getDefaultValues());
  }

  /** A name as the logical types DDL quotes one. */
  public static String quote(String name) {
    return "`" + name.replace("`", "``") + "`";
  }

  /** A CHECK clause of the logical types DDL; the name and the message may be null. */
  public static String checkClause(String name, String sql, String message) {
    return (name != null ? "CONSTRAINT " + quote(name) + " " : "")
        + "CHECK (" + sql + ")"
        + (message != null ? " MESSAGE " + literal(message) : "");
  }

  private static String qualified(String dotted) {
    return Arrays.stream(dotted.split("\\.")).map(TypeScripts::quote)
        .collect(Collectors.joining("."));
  }

  private static String literal(String s) {
    return "'" + s.replace("'", "''") + "'";
  }
}
