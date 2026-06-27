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

import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * Centralized factory for building lexer + parser instances configured with a
 * strict error listener that throws {@link ValidationException} on the first
 * syntax error.
 *
 * <p>ANTLR's default {@code ConsoleErrorListener} writes errors to stderr and
 * lets the parser silently recover by inserting/deleting tokens. For a DDL
 * grammar — and especially for {@code CHECK (...)} sub-expressions where
 * recovery can leak past the closing paren and corrupt subsequent fields —
 * silent recovery produces partially-formed parse trees that translate to
 * confusing downstream errors (or worse, to incorrect CEL).
 *
 * <p>This factory replaces the default listener with one that aborts on the
 * first syntax error, surfacing a single clear message with line and column.
 * All callers (production and tests) should route through {@link #parse(String)}
 * so the strict behavior is uniform.
 */
public final class LogicalTypesParserFactory {

  private LogicalTypesParserFactory() {}

  /**
   * Parse a DDL script and return its root {@code script} parse tree. Throws
   * {@link ValidationException} with line/column info on the first lex or parse
   * error encountered.
   */
  public static LogicalTypesParser.ScriptContext parse(String input) {
    return newParser(input).script();
  }

  /**
   * Build a {@link LogicalTypesParser} with the strict error listener wired in.
   * Use this when you need direct access to specific entry rules other than
   * {@code script()} (e.g., partial parses for tooling).
   */
  public static LogicalTypesParser newParser(String input) {
    LogicalTypesLexer lexer = new LogicalTypesLexer(CharStreams.fromString(input));
    lexer.removeErrorListeners();
    lexer.addErrorListener(STRICT_LISTENER);
    LogicalTypesParser parser = new LogicalTypesParser(new CommonTokenStream(lexer));
    parser.removeErrorListeners();
    parser.addErrorListener(STRICT_LISTENER);
    return parser;
  }

  /**
   * Throws a {@link ValidationException} on the first syntax error reported by
   * either the lexer or the parser. Stateless and shareable across recognizers.
   */
  private static final BaseErrorListener STRICT_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(
        Recognizer<?, ?> recognizer,
        Object offendingSymbol,
        int line,
        int charPositionInLine,
        String msg,
        RecognitionException e) {
      throw new ValidationException(
          "Parse error at line " + line + ", col " + (charPositionInLine + 1)
              + ": " + msg);
    }
  };
}
