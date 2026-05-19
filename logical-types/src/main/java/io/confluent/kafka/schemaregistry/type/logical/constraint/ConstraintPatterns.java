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

package io.confluent.kafka.schemaregistry.type.logical.constraint;

import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

/**
 * Pure helpers for translating SQL string-shaped literals and patterns into
 * the forms CEL accepts. Stateless and dependency-free — extracted from
 * {@link ConstraintToCelTranslator} so the translator stays focused on
 * cascade-walking and emit decisions.
 *
 * <ul>
 *   <li>{@link #extractSqlStringContent(String)} — strip outer single quotes
 *       and resolve SQL's doubled-quote escape.</li>
 *   <li>{@link #translateStringLiteral(String)} — full SQL→CEL string-literal
 *       conversion (handles all the escapes CEL requires inside {@code '...'}).</li>
 *   <li>{@link #likePatternToRegex(String, char)} — SQL LIKE pattern →
 *       anchored regex string.</li>
 *   <li>{@link #escapeForCelStringLiteral(String)} — escape a regex (or any
 *       string) for embedding inside a CEL single-quoted literal.</li>
 *   <li>{@link #normalizeFloatLiteral(String)} — pad trailing/leading dot
 *       to satisfy CEL's float-literal grammar.</li>
 * </ul>
 */
final class ConstraintPatterns {

  private ConstraintPatterns() {
    // static utility
  }

  /**
   * Strip outer single quotes and resolve SQL's doubled-quote escape
   * ({@code ''} → {@code '}). Returns the raw string content.
   */
  static String extractSqlStringContent(String sqlText) {
    if (sqlText.length() < 2 || !sqlText.startsWith("'") || !sqlText.endsWith("'")) {
      throw new ValidationException(
          "Internal: malformed string literal token: " + sqlText);
    }
    String inner = sqlText.substring(1, sqlText.length() - 1);
    StringBuilder out = new StringBuilder(inner.length());
    for (int i = 0; i < inner.length(); i++) {
      char c = inner.charAt(i);
      if (c == '\'' && i + 1 < inner.length() && inner.charAt(i + 1) == '\'') {
        out.append('\'');
        i++;
      } else {
        out.append(c);
      }
    }
    return out.toString();
  }

  /**
   * Convert a SQL LIKE pattern to a regex string, anchored with {@code ^…$}.
   * {@code %}→{@code .*}, {@code _}→{@code .}; regex specials escaped;
   * {@code escapeChar} (followed by any char) emits the next char as a
   * regex literal.
   */
  static String likePatternToRegex(String pattern, char escapeChar) {
    StringBuilder out = new StringBuilder("^");
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (c == escapeChar && i + 1 < pattern.length()) {
        appendRegexLiteral(out, pattern.charAt(i + 1));
        i++;
      } else if (c == '%') {
        out.append(".*");
      } else if (c == '_') {
        out.append('.');
      } else {
        appendRegexLiteral(out, c);
      }
    }
    out.append('$');
    return out.toString();
  }

  /**
   * Append {@code c} to {@code sb}, escaping if it's a regex special.
   */
  private static void appendRegexLiteral(StringBuilder sb, char c) {
    if (".+*?[](){}^$|\\".indexOf(c) >= 0) {
      sb.append('\\').append(c);
    } else {
      sb.append(c);
    }
  }

  /**
   * Escape a regex string for embedding inside a CEL single-quoted string
   * literal. CEL string lexer rejects raw control characters (LF, CR, TAB,
   * etc.) inside {@code '...'}, so we must escape them in addition to
   * backslash and the single-quote delimiter.
   */
  static String escapeForCelStringLiteral(String regex) {
    StringBuilder out = new StringBuilder(regex.length());
    for (int i = 0; i < regex.length(); i++) {
      char c = regex.charAt(i);
      switch (c) {
        case '\\':
          out.append("\\\\");
          break;
        case '\'':
          out.append("\\'");
          break;
        case '\n':
          out.append("\\n");
          break;
        case '\r':
          out.append("\\r");
          break;
        case '\t':
          out.append("\\t");
          break;
        case '\b':
          out.append("\\b");
          break;
        case '\f':
          out.append("\\f");
          break;
        default:
          if (c < 0x20 || c == 0x7F) {
            out.append(String.format("\\x%02x", (int) c));
          } else {
            out.append(c);
          }
      }
    }
    return out.toString();
  }

  /**
   * SQL accepts the trailing-dot float form ({@code 1.}) and the
   * leading-dot form ({@code .5}). CEL's float literal grammar requires
   * digits on both sides of the dot, so {@code 1.} → CEL parse error and
   * {@code .5} → CEL parse error. Normalize both by adding a sentinel
   * digit on the absent side. Returns the input unchanged when the form
   * is already two-sided (e.g. {@code 1.5}, {@code 1.0e10}).
   */
  static String normalizeFloatLiteral(String text) {
    int dot = text.indexOf('.');
    if (dot < 0) {
      return text;  // shouldn't happen for FLOAT_LITERAL, defensive
    }
    int afterDot = dot + 1;
    boolean trailingDot = afterDot >= text.length()
        || !Character.isDigit(text.charAt(afterDot));
    boolean leadingDot = dot == 0
        || (dot == 1 && (text.charAt(0) == '-' || text.charAt(0) == '+'));
    if (!trailingDot && !leadingDot) {
      return text;
    }
    StringBuilder out = new StringBuilder(text.length() + 2);
    if (leadingDot) {
      // `.5` → `0.5`; `-.5` → `-0.5`
      if (dot > 0) {
        out.append(text, 0, dot);
      }
      out.append('0');
      out.append(text, dot, text.length());
    } else {
      out.append(text);
    }
    if (trailingDot) {
      // `1.` → `1.0`; `1.e10` → `1.0e10`
      out.insert(afterDot + (leadingDot ? 1 : 0), '0');
    }
    return out.toString();
  }

  /**
   * SQL string literal {@code 'foo''s bar'} → CEL string {@code 'foo\'s bar'}.
   * Replace SQL's doubled-quote escape with CEL's backslash escape; also
   * escape control chars CEL string literals reject.
   */
  static String translateStringLiteral(String sqlText) {
    if (sqlText.length() < 2 || !sqlText.startsWith("'") || !sqlText.endsWith("'")) {
      throw new ValidationException(
          "Internal: malformed string literal token: " + sqlText);
    }
    String inner = sqlText.substring(1, sqlText.length() - 1);
    StringBuilder out = new StringBuilder("'");
    for (int i = 0; i < inner.length(); i++) {
      char c = inner.charAt(i);
      if (c == '\'' && i + 1 < inner.length() && inner.charAt(i + 1) == '\'') {
        out.append("\\'");
        i++;
      } else if (c == '\\') {
        out.append("\\\\");
      } else if (c == '\n') {
        out.append("\\n");
      } else if (c == '\r') {
        out.append("\\r");
      } else if (c == '\t') {
        out.append("\\t");
      } else {
        out.append(c);
      }
    }
    out.append("'");
    return out.toString();
  }
}
