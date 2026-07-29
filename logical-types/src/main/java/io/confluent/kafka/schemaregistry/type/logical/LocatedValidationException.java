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

/**
 * A {@link ValidationException} that carries the source position where the offending
 * input was parsed. Used by tools (e.g. the playground UI) to underline the exact
 * span of the error rather than reporting at the top of the document.
 *
 * <p>Position fields use ANTLR's convention: lines are 1-based, columns are 0-based.
 * {@code endColumn} is exclusive.
 */
public class LocatedValidationException extends ValidationException {

  private final int line;
  private final int column;
  private final int endLine;
  private final int endColumn;

  public LocatedValidationException(
      String message, int line, int column, int endLine, int endColumn) {
    super(message);
    this.line = line;
    this.column = column;
    this.endLine = endLine;
    this.endColumn = endColumn;
  }

  public LocatedValidationException(
      String message, int line, int column, int endLine, int endColumn, Throwable cause) {
    super(message, cause);
    this.line = line;
    this.column = column;
    this.endLine = endLine;
    this.endColumn = endColumn;
  }

  public int getLine() {
    return line;
  }

  public int getColumn() {
    return column;
  }

  public int getEndLine() {
    return endLine;
  }

  public int getEndColumn() {
    return endColumn;
  }
}
