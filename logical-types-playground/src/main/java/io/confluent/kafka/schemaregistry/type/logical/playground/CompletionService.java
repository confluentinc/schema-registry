/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.type.logical.playground;

import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.AND;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.ARRAY;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.AS;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.BETWEEN;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.BIGINT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.BINARY;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.BOOLEAN;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.BOTH;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.BYTES;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.CASE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.CAST;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.CHAR;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.CHARACTER;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.CHECK;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.CURRENT_TIMESTAMP;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.DATE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.DEC;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.DECIMAL;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.DEFAULT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.DOUBLE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.ELSE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.END;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.ENUM;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.ESCAPE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.EXTRACT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.FALSE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.FLOAT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.FOR;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.FROM;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.IN;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.INT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.INTEGER;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.IS;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.LEADING;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.LIKE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.LOCAL;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.MAP;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.MULTISET;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.NAMESPACE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.NOT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.NULL;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.NUMERIC;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.OR;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.POSITION;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.PRECISION;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.REAL;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.REFERENCE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.ROW;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.SMALLINT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.STRING;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.SUBSTRING;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.SYMMETRIC;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.THEN;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.TIME;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.TIMESTAMP;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.TIMESTAMP_LTZ;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.TINYINT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.TRAILING;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.TRIM;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.TRUE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.TYPE;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.UNION;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.VARBINARY;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.VARCHAR;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.VARIANT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.VARYING;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.WHEN;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.WITH;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.WITHOUT;
import static io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer.ZONE;

import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesLexer;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import io.confluent.kafka.schemaregistry.type.logical.playground.dto.CompleteResponse;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;

/**
 * Grammar-derived completion. Keyword set comes from the lexer vocabulary; the
 * follow-set table below is hand-curated against the grammar in LogicalTypes.g4.
 */
@ApplicationScoped
public class CompletionService {

  private static final int[] PRIMITIVES = {
      BOOLEAN, TINYINT, SMALLINT, INT, INTEGER, BIGINT, FLOAT, REAL, DOUBLE,
      DECIMAL, DEC, NUMERIC, CHARACTER, CHAR, VARCHAR, STRING,
      BINARY, VARBINARY, BYTES,
      DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ
  };

  private static final int[] TYPE_POSITION;

  static {
    int[] extra = {VARIANT, ROW, UNION, MAP, ARRAY, MULTISET};
    TYPE_POSITION = concat(PRIMITIVES, extra);
  }

  // Statement-leading keywords: `NAMESPACE`, `REFERENCE` (TYPE), `ROW`,
  // `ENUM`, and bare `TYPE` (trailing root-registration form).
  private static final int[] STMT_START = {NAMESPACE, REFERENCE, ROW, ENUM, TYPE};

  // Map from "previous keyword token type" → allowed next keyword token types.
  // Misses fall through to ALL_KEYWORDS, so the table only needs entries
  // wherever a tighter, demo-meaningful prediction beats the default.
  private static final Map<Integer, int[]> FOLLOW = new LinkedHashMap<>();

  static {
    // After NAMESPACE the user types a free-form qualifiedName; no keywords legal.
    FOLLOW.put(NAMESPACE, new int[0]);
    FOLLOW.put(REFERENCE, new int[] {TYPE});
    // `AS` mid-expression — appears in `REFERENCE TYPE x AS SYNONYM FOR ...`
    // (rare) and `CAST(x AS castType)`. Offer the primitive-type set, which
    // covers the CAST case; the user picks SYNONYM by typing.
    FOLLOW.put(AS, PRIMITIVES);
    FOLLOW.put(CHARACTER, new int[] {VARYING});
    FOLLOW.put(BINARY, new int[] {VARYING});
    FOLLOW.put(DOUBLE, new int[] {PRECISION});
    FOLLOW.put(TIME, new int[] {ZONE});
    FOLLOW.put(WITH, new int[] {LOCAL});
    FOLLOW.put(WITHOUT, new int[] {TIME});
    FOLLOW.put(LOCAL, new int[] {TIME});
    FOLLOW.put(NOT, new int[] {NULL});
    FOLLOW.put(DEFAULT, new int[] {TRUE, FALSE, NULL});
  }

  private static final int[] ALL_KEYWORDS;

  static {
    Set<Integer> all = new LinkedHashSet<>();
    for (int t = ARRAY; t <= ZONE; t++) {
      all.add(t);
    }
    ALL_KEYWORDS = all.stream().mapToInt(Integer::intValue).toArray();
  }

  // Keyword tokens valid inside a CHECK (...) expression. Includes operator
  // keywords (AND/OR/NOT/IS/IN/BETWEEN/LIKE/...), CASE control flow, special-
  // syntax function names that are themselves lexer tokens (CAST/EXTRACT/
  // SUBSTRING/POSITION/TRIM/CURRENT_TIMESTAMP), TRIM modifiers (BOTH/LEADING/
  // TRAILING — the latter two are accepted by the parser but rejected by the
  // translator; surfacing them anyway keeps the completion list grammar-faithful),
  // EXTRACT field names (YEAR/MONTH/.../EPOCH), CAST AS, and the literal
  // tokens NULL/TRUE/FALSE.
  private static final int[] CHECK_KEYWORDS = {
      AND, OR, NOT, IS, NULL, IN, BETWEEN, SYMMETRIC, LIKE, ESCAPE,
      CASE, WHEN, THEN, ELSE, END, AS, FROM, FOR, BOTH, LEADING, TRAILING,
      CAST, EXTRACT, SUBSTRING, POSITION, TRIM, CURRENT_TIMESTAMP,
      TRUE, FALSE,
  };

  // Function names that parse as plain identifiers (not lexer keywords) but
  // are recognized by the CHECK→CEL translator. Surface them as suggestions
  // when the caret is inside a CHECK clause. These mirror the closed
  // whitelist in ConstraintToCelTranslator.visitFuncApplication; the IS_*
  // validators come from the SR CEL extension at
  // io.confluent.kafka.schemaregistry.rules.cel.builtin.BuiltinDeclarations.
  private static final List<String> CHECK_FUNCTIONS = List.of(
      // Polymorphic / string-shape
      "LENGTH", "UPPER", "LOWER",
      "STARTS_WITH", "ENDS_WITH", "CONTAINS", "REPLACE", "MATCHES",
      // Conditional
      "COALESCE", "NULLIF", "GREATEST", "LEAST",
      // CEL macros (predicate-form)
      "EVERY", "ANY", "ONE",
      // Format validators (require SR BuiltinOverload extension)
      "IS_EMAIL", "IS_HOSTNAME", "IS_IPV4", "IS_IPV6",
      "IS_URI", "IS_URI_REF", "IS_UUID");

  // Macro function names — recognized in CHECK→CEL via emitMacro. Used to
  // detect when the caret is inside a macro call's body so the iter-var
  // can be added to the column list.
  private static final Set<String> CHECK_MACRO_NAMES = Set.of("EVERY", "ANY", "ONE");

  // ------------------------------------------------------------------------
  // Tier 2 narrowing: per-position keyword subsets. Each set covers one
  // syntactic position inside a CHECK expression. The dispatch in
  // narrowedCheckCandidates() picks one of these (or the full union for
  // unrecognized positions).
  // ------------------------------------------------------------------------

  /**
   * Tokens valid at the start of a fresh sub-expression.
   */
  private static final int[] CHECK_EXPRESSION_START_KEYWORDS = {
      NOT, CASE, CAST, EXTRACT, SUBSTRING, POSITION, TRIM, CURRENT_TIMESTAMP,
      TRUE, FALSE, NULL,
  };

  /**
   * Operators/predicates that may follow a value or column reference.
   */
  private static final int[] CHECK_AFTER_VALUE = {
      AND, OR, IS, IN, BETWEEN, NOT, LIKE,
  };

  /**
   * Tokens valid right after {@code IS}.
   */
  private static final int[] CHECK_AFTER_IS = {NOT, NULL};

  /**
   * Tokens valid right after {@code IS NOT}.
   */
  private static final int[] CHECK_AFTER_IS_NOT = {NULL};

  /**
   * Tokens valid right after {@code BETWEEN} — SYMMETRIC + expression-start.
   */
  private static final int[] CHECK_AFTER_BETWEEN =
      concat(new int[] {SYMMETRIC}, CHECK_EXPRESSION_START_KEYWORDS);

  /**
   * Postfix predicates that can follow {@code NOT} after a value.
   */
  private static final int[] CHECK_AFTER_NOT_POSTFIX = {IN, BETWEEN, LIKE};

  /**
   * EXTRACT field names — exactly the set the translator accepts. These
   * parse as plain identifiers in the grammar (the EXTRACT rule uses
   * {@code identifier} for the field), so they're emitted as strings
   * rather than via lexer-vocabulary lookup.
   */
  private static final List<String> CHECK_EXTRACT_FIELDS = List.of(
      "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "EPOCH");

  /**
   * Tokens valid right after a {@code WHEN <expr>}.
   */
  private static final int[] CHECK_AFTER_WHEN_VALUE = {THEN};

  /**
   * Tokens valid right after a {@code THEN <expr>}.
   */
  private static final int[] CHECK_AFTER_THEN_VALUE = {WHEN, ELSE, END};

  /**
   * Tokens valid right after an {@code ELSE <expr>}.
   */
  private static final int[] CHECK_AFTER_ELSE_VALUE = {END};

  /**
   * TRIM modifier tokens. We only suggest BOTH (LEADING/TRAILING are
   * parsed but the translator rejects them — no point steering users there).
   */
  private static final int[] CHECK_AFTER_TRIM_PAREN =
      concat(new int[] {BOTH}, CHECK_EXPRESSION_START_KEYWORDS);

  /**
   * Tokens valid right after a {@code LIKE 'pat'} pattern (then ESCAPE/op/...).
   */
  private static final int[] CHECK_AFTER_LIKE_PATTERN =
      concat(new int[] {ESCAPE}, CHECK_AFTER_VALUE);

  /**
   * Bundle of "what to offer at the caret" — enables the CHECK-mode
   * dispatch to return a single object capturing keyword tokens, function
   * inclusion, and column inclusion (with optional extras like macro
   * iter-var bindings or AFTER_DOT nested-field replacement).
   */
  private static final class CheckCandidates {
    int[] keywordTokens;
    boolean includeFunctions;
    boolean includeColumns;
    /**
     * Extra keyword-style names emitted as plain strings — used for tokens
     * that aren't in the lexer vocabulary (e.g. EXTRACT field names like
     * YEAR/MONTH/DAY which parse as plain identifiers).
     */
    List<String> extraKeywords = java.util.Collections.emptyList();
    /**
     * Extra column-style names (e.g. macro iter-vars) added on top of the scope.
     */
    List<String> extraColumns = java.util.Collections.emptyList();
    /**
     * When non-null, replaces the column list entirely (used for AFTER_DOT).
     */
    List<String> overrideColumns;

    static CheckCandidates of(int[] kws, boolean fns, boolean cols) {
      CheckCandidates c = new CheckCandidates();
      c.keywordTokens = kws;
      c.includeFunctions = fns;
      c.includeColumns = cols;
      return c;
    }
  }

  public CompleteResponse complete(String sql, int caretOffset) {
    CompleteResponse resp = new CompleteResponse();
    resp.items = new ArrayList<>();
    resp.prefix = "";
    if (sql == null) {
      sql = "";
    }
    if (caretOffset < 0) {
      caretOffset = 0;
    }
    if (caretOffset > sql.length()) {
      caretOffset = sql.length();
    }

    LogicalTypesLexer lexer = new LogicalTypesLexer(CharStreams.fromString(sql));
    lexer.removeErrorListeners();
    List<? extends Token> all = lexer.getAllTokens();

    // Identify the partial identifier the user is typing (chars left of caret).
    int prefixStart = caretOffset;
    while (prefixStart > 0 && isIdChar(sql.charAt(prefixStart - 1))) {
      prefixStart--;
    }
    String prefix = sql.substring(prefixStart, caretOffset);
    resp.prefix = prefix;

    // Last two non-WS tokens strictly before the partial word. The
    // second-to-last is needed to recognize the typeExpr position right after a
    // fieldName identifier (e.g. `(wrapper |` — `previous` is `wrapper`,
    // `previousPrev` is `(`, which is what makes this a type position rather
    // than just trailing identifier text).
    Token previous = null;
    Token previousPrev = null;
    for (Token t : all) {
      int type = t.getType();
      if (type == LogicalTypesLexer.WS
          || type == LogicalTypesLexer.LINE_COMMENT
          || type == LogicalTypesLexer.BLOCK_COMMENT) {
        continue;
      }
      if (t.getStopIndex() + 1 <= prefixStart) {
        previousPrev = previous;
        previous = t;
      } else {
        break;
      }
    }

    Vocabulary vocab = LogicalTypesParser.VOCABULARY;
    String upper = prefix.toUpperCase();
    Set<String> seen = new LinkedHashSet<>();

    // CHECK-expression mode: the caret sits between an unmatched `(` opened
    // by a CHECK keyword and its eventual `)`. Tier 2: dispatch on the
    // previous-token context to narrow what's offered (after `IS` → NULL/NOT,
    // after `EXTRACT (` → field names, etc.); fall back to the full union
    // when the position isn't recognized.
    List<String> columnsInScope = enclosingCheckScope(all, prefixStart);
    if (columnsInScope != null) {
      CheckCandidates candidates = narrowedCheckCandidates(
          previous, previousPrev, all, prefixStart);
      for (int tokenType : candidates.keywordTokens) {
        String name = vocab.getSymbolicName(tokenType);
        if (name == null || !seen.add(name)) {
          continue;
        }
        if (upper.isEmpty() || name.startsWith(upper)) {
          resp.items.add(new CompleteResponse.Item(name, "Keyword", name));
        }
      }
      for (String kw : candidates.extraKeywords) {
        if (!seen.add(kw)) {
          continue;
        }
        if (upper.isEmpty() || kw.startsWith(upper)) {
          resp.items.add(new CompleteResponse.Item(kw, "Keyword", kw));
        }
      }
      if (candidates.includeFunctions) {
        for (String fn : CHECK_FUNCTIONS) {
          if (!seen.add(fn)) {
            continue;
          }
          if (upper.isEmpty() || fn.startsWith(upper)) {
            resp.items.add(new CompleteResponse.Item(fn, "Function", fn));
          }
        }
      }
      List<String> cols = candidates.overrideColumns;
      if (cols == null && candidates.includeColumns) {
        cols = new ArrayList<>(columnsInScope);
        cols.addAll(candidates.extraColumns);
      }
      if (cols != null) {
        for (String col : cols) {
          if (!seen.add(col)) {
            continue;
          }
          if (prefix.isEmpty() || col.toUpperCase().startsWith(upper)) {
            resp.items.add(new CompleteResponse.Item(col, "Column", col));
          }
        }
      }
      return resp;
    }

    int[] candidates = candidatesFor(previous, previousPrev);

    for (int tokenType : candidates) {
      String name = vocab.getSymbolicName(tokenType);
      if (name == null || !seen.add(name)) {
        continue;
      }
      if (upper.isEmpty() || name.startsWith(upper)) {
        resp.items.add(new CompleteResponse.Item(name, "Keyword", name));
      }
    }

    if (atTypePosition(previous, previousPrev)) {
      for (String name : declaredTypeNames(all)) {
        if (prefix.isEmpty() || name.toUpperCase().startsWith(upper)) {
          resp.items.add(new CompleteResponse.Item(name, "Type", name));
        }
      }
    }
    return resp;
  }

  private int[] candidatesFor(Token previous, Token previousPrev) {
    if (previous == null) {
      return STMT_START;
    }
    int[] mapped = FOLLOW.get(previous.getType());
    if (mapped != null) {
      return mapped;
    }
    // Heuristic: directly after `(` or `,` inside a struct/row body or arg list,
    // we are usually at a type position; suggest type keywords first.
    String text = previous.getText();
    if ("(".equals(text) || ",".equals(text) || "<".equals(text)) {
      return TYPE_POSITION;
    }
    if (";".equals(text)) {
      return STMT_START;
    }
    if (".".equals(text)) {
      // Inside a qualifiedName the next token must be an identifier; no keywords.
      return new int[0];
    }
    if (isAfterFieldName(previous, previousPrev)) {
      return TYPE_POSITION;
    }
    return ALL_KEYWORDS;
  }

  private boolean atTypePosition(Token previous, Token previousPrev) {
    if (previous == null) {
      return false;
    }
    if (previous.getType() == AS) {
      return true;
    }
    String text = previous.getText();
    if ("(".equals(text) || ",".equals(text) || "<".equals(text)) {
      return true;
    }
    // Statement-leading bare `TYPE` opens the trailing root-registration form
    // `TYPE <typeExpr>` — a typeExpr is expected next. Detect by checking that
    // the token before `TYPE` is either nothing (doc start) or a `;`.
    // Same shape applies after `REFERENCE TYPE` (an external import name),
    // though the playground client today short-circuits that case with a
    // cross-tab-only completion path.
    if (previous.getType() == TYPE) {
      if (previousPrev == null) {
        return true;
      }
      if (previousPrev.getType() == REFERENCE) {
        return true;
      }
      return ";".equals(previousPrev.getText());
    }
    return isAfterFieldName(previous, previousPrev);
  }

  /**
   * True when {@code previous} is an identifier that opens a fieldDef in a
   * struct body — i.e. the identifier is immediately preceded by {@code (} or
   * {@code ,}. The grammar is {@code fieldName typeExpr ...}, so the next token
   * is a type.
   */
  private boolean isAfterFieldName(Token previous, Token previousPrev) {
    if (previous == null || previousPrev == null) {
      return false;
    }
    int t = previous.getType();
    if (t != LogicalTypesLexer.ID && t != LogicalTypesLexer.QUOTED_ID) {
      return false;
    }
    String prevPrevText = previousPrev.getText();
    return "(".equals(prevPrevText) || ",".equals(prevPrevText);
  }

  /**
   * If the caret sits inside a CHECK (...) expression, return the column
   * names that may be referenced from there: the single attached field for
   * a column-level CHECK, or every field of the surrounding struct for a
   * table-level CHECK. Returns null when the caret is not inside a CHECK
   * paren.
   *
   * <p>Detection: walk tokens left-to-right and track a stack of "open"
   * paren contexts. When a `(` is opened immediately after CHECK, push
   * "check"; otherwise push "other". Pop on `)`. If the stack's top at
   * caret-time is "check", we're inside.
   *
   * <p>Scope resolution (Tier 1 best-effort):
   * <ul>
   *   <li>Table-level: the most recent ROW declaration that contains the
   *     caret defines the field set.</li>
   *   <li>Column-level: the most recent ID before the CHECK keyword, when
   *     it appears immediately after `,` or `(`, names the attached field.
   *     Only that field is in scope.</li>
   * </ul>
   * Macro iter-vars (e.g., {@code EVERY(arr, t, |)}) are not resolved at
   * Tier 1 — defer to Tier 2.
   */
  /**
   * Tier 2 narrowing: pick the candidate set for the caret's position
   * inside a CHECK expression, based on the immediately-preceding tokens.
   * Falls back to the full Tier 1 union for positions we don't recognize.
   *
   * <p>The dispatch is intentionally previous/previousPrev-driven rather
   * than parse-tree-aware: the existing DDL completion path uses the same
   * pattern, so the rules stay simple and the cases easy to test. Anything
   * needing deeper structural awareness (typed nested fields, macro
   * iter-var binding) lives below in dedicated helpers.
   */
  private CheckCandidates narrowedCheckCandidates(
      Token previous, Token previousPrev,
      List<? extends Token> tokens, int prefixStart) {
    if (previous == null) {
      // Empty CHECK clause body — `CHECK (|`. Offer a fresh expression.
      return expressionStartCandidates();
    }
    int prevType = previous.getType();
    String prevText = previous.getText();

    // -------------------------------------------------------------------
    // Strict positions where exactly one keyword is expected next
    // -------------------------------------------------------------------
    if (prevType == IS) {
      if (previousPrev != null && previousPrev.getType() == IS) {
        // shouldn't normally happen, but guard.
        return CheckCandidates.of(CHECK_AFTER_IS, false, false);
      }
      return CheckCandidates.of(CHECK_AFTER_IS, false, false);
    }
    if (prevType == NOT && previousPrev != null && previousPrev.getType() == IS) {
      return CheckCandidates.of(CHECK_AFTER_IS_NOT, false, false);
    }

    // -------------------------------------------------------------------
    // EXTRACT (field FROM ts)
    // -------------------------------------------------------------------
    if ("(".equals(prevText) && previousPrev != null
        && previousPrev.getType() == EXTRACT) {
      CheckCandidates c = CheckCandidates.of(new int[0], false, false);
      c.extraKeywords = CHECK_EXTRACT_FIELDS;
      return c;
    }
    if (isExtractField(previous)) {
      return CheckCandidates.of(new int[] {FROM}, false, false);
    }

    // -------------------------------------------------------------------
    // CAST (<expr> AS <type>)
    // -------------------------------------------------------------------
    if (prevType == AS) {
      // The only AS inside a CHECK expression is CAST's. Suggest
      // primitive type names — same as the DDL TYPE_POSITION set, minus
      // composite types (CAST AS ROW/ARRAY etc. is not in our whitelist).
      return CheckCandidates.of(PRIMITIVES, false, false);
    }

    // -------------------------------------------------------------------
    // TRIM ([BOTH|...]? <expr> [FROM <expr>]?)
    // -------------------------------------------------------------------
    if ("(".equals(prevText) && previousPrev != null
        && previousPrev.getType() == TRIM) {
      return CheckCandidates.of(CHECK_AFTER_TRIM_PAREN, true, true);
    }

    // -------------------------------------------------------------------
    // POSITION (<needle> IN <haystack>)
    //   After <needle> the only legal continuation is IN.
    // -------------------------------------------------------------------
    if (isInsidePositionAwaitingIn(tokens, prefixStart)) {
      return CheckCandidates.of(new int[] {IN}, false, false);
    }

    // -------------------------------------------------------------------
    // SUBSTRING (<s> FROM <i> [FOR <n>])
    //   After SUBSTRING(s FROM i, only FOR is legal next.
    // -------------------------------------------------------------------
    if (isInsideSubstringAwaitingFor(tokens, prefixStart)) {
      return CheckCandidates.of(new int[] {FOR}, false, false);
    }

    // -------------------------------------------------------------------
    // BETWEEN — SYMMETRIC + start of lower-bound expression
    // -------------------------------------------------------------------
    if (prevType == BETWEEN) {
      return CheckCandidates.of(CHECK_AFTER_BETWEEN, true, true);
    }

    // -------------------------------------------------------------------
    // NOT after a value: postfix predicates (IN/BETWEEN/LIKE)
    // NOT at expression-start: logical NOT — fall through to expression.
    // We can't always tell which, so when the token before NOT is a
    // value-style token, suggest the postfix predicates; otherwise
    // expression-start.
    // -------------------------------------------------------------------
    if (prevType == NOT) {
      if (previousPrev != null && isValueLikeToken(previousPrev)) {
        return CheckCandidates.of(CHECK_AFTER_NOT_POSTFIX, false, false);
      }
      return expressionStartCandidates();
    }

    // -------------------------------------------------------------------
    // CASE control flow: WHEN <expr> THEN, THEN <expr> WHEN/ELSE/END,
    // ELSE <expr> END. Detect by walking back to the most recent
    // unmatched WHEN/THEN/ELSE inside an open CASE.
    // -------------------------------------------------------------------
    int caseFollow = caseFollowKeyword(tokens, prefixStart);
    if (caseFollow == 1) {
      return CheckCandidates.of(CHECK_AFTER_WHEN_VALUE, false, false);
    }
    if (caseFollow == 2) {
      return CheckCandidates.of(CHECK_AFTER_THEN_VALUE, false, false);
    }
    if (caseFollow == 3) {
      return CheckCandidates.of(CHECK_AFTER_ELSE_VALUE, false, false);
    }

    // -------------------------------------------------------------------
    // LIKE 'pat' — after the pattern string, ESCAPE or AND/OR/etc.
    // -------------------------------------------------------------------
    if (previousPrev != null && previousPrev.getType() == LIKE) {
      return CheckCandidates.of(CHECK_AFTER_LIKE_PATTERN, false, false);
    }

    // -------------------------------------------------------------------
    // After a column ref or value-shaped token, expect an operator
    // -------------------------------------------------------------------
    if (isValueLikeToken(previous)) {
      // Inside a macro body the iter-var should appear in the column list
      // for the next sub-expression, but at this position (after a value)
      // we want operators, not columns — so don't add them here.
      return CheckCandidates.of(CHECK_AFTER_VALUE, false, false);
    }

    // -------------------------------------------------------------------
    // After `(` or `,` or operator tokens, expect a fresh expression.
    // Inside macro bodies, add the iter-var to the column list.
    // -------------------------------------------------------------------
    if (isExpressionStartToken(previous)) {
      CheckCandidates c = expressionStartCandidates();
      String iterVar = enclosingMacroIterVar(tokens, prefixStart);
      if (iterVar != null) {
        c.extraColumns = java.util.Collections.singletonList(iterVar);
      }
      return c;
    }

    // -------------------------------------------------------------------
    // Fallback: full Tier 1 union.
    // -------------------------------------------------------------------
    return CheckCandidates.of(CHECK_KEYWORDS, true, true);
  }

  /** Default expression-start: keywords + functions + columns. */
  private static CheckCandidates expressionStartCandidates() {
    return CheckCandidates.of(CHECK_EXPRESSION_START_KEYWORDS, true, true);
  }

  /**
   * Set of EXTRACT field names recognized when emitted as identifiers.
   * Used to detect "we just typed an EXTRACT field" so we can suggest FROM.
   */
  private static final Set<String> EXTRACT_FIELD_NAMES = Set.of(
      "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "EPOCH");

  private static final Set<Integer> VALUE_LIKE_KEYWORD_TYPES = Set.of(
      END, TRUE, FALSE, NULL, CURRENT_TIMESTAMP);

  private static final Set<Integer> VALUE_LIKE_LITERAL_TYPES = Set.of(
      LogicalTypesLexer.ID, LogicalTypesLexer.QUOTED_ID,
      LogicalTypesLexer.INT_LITERAL, LogicalTypesLexer.FLOAT_LITERAL,
      LogicalTypesLexer.STRING_LITERAL, LogicalTypesLexer.BYTES_LITERAL);

  private static final Set<Integer> EXPRESSION_START_KEYWORD_TYPES = Set.of(
      AND, OR, THEN, ELSE, WHEN, CASE);

  private static final Set<String> EXPRESSION_START_OPERATOR_TEXTS = Set.of(
      "=", "<>", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "||");

  private static boolean isExtractField(Token t) {
    if (t == null) {
      return false;
    }
    int type = t.getType();
    if (type != LogicalTypesLexer.ID && type != LogicalTypesLexer.QUOTED_ID) {
      return false;
    }
    return EXTRACT_FIELD_NAMES.contains(t.getText().toUpperCase());
  }

  /**
   * True when the token represents a "value-shaped" thing — a column ref
   * (identifier or non-reserved keyword), a literal, a closing paren, or
   * a CASE-END. Used to decide whether the next position expects an
   * operator vs a fresh expression.
   */
  private static boolean isValueLikeToken(Token t) {
    if (t == null) {
      return false;
    }
    if (")".equals(t.getText())) {
      return true;
    }
    int type = t.getType();
    return VALUE_LIKE_KEYWORD_TYPES.contains(type)
        || VALUE_LIKE_LITERAL_TYPES.contains(type);
  }

  /**
   * True when the token starts a fresh sub-expression position: paren-open,
   * comma, AND/OR/NOT/comparison/arithmetic/concat operator, THEN/ELSE.
   */
  private static boolean isExpressionStartToken(Token t) {
    String text = t.getText();
    if ("(".equals(text) || ",".equals(text)) {
      return true;
    }
    if (EXPRESSION_START_OPERATOR_TEXTS.contains(text)) {
      return true;
    }
    return EXPRESSION_START_KEYWORD_TYPES.contains(t.getType());
  }

  /**
   * Walk back from {@code prefixStart} to the innermost open `(` in the
   * CHECK expression. If that paren was opened by POSITION and the caret
   * sits right after the needle expression (no `IN` consumed yet), return
   * true. Used to suggest only `IN` at that position.
   */
  private static boolean isInsidePositionAwaitingIn(
      List<? extends Token> tokens, int prefixStart) {
    return innerCallExpects(tokens, prefixStart, POSITION, IN);
  }

  /** Same shape: SUBSTRING(s FROM i | — expect FOR. */
  private static boolean isInsideSubstringAwaitingFor(
      List<? extends Token> tokens, int prefixStart) {
    return innerCallExpects(tokens, prefixStart, SUBSTRING, FOR)
        && hasKeywordSinceOpenParen(tokens, prefixStart, FROM);
  }

  /**
   * Walk tokens left of {@code prefixStart} maintaining a paren stack;
   * return true when the innermost open `(` is preceded by {@code openerType}
   * AND the keyword {@code expectedKeyword} has NOT yet appeared at depth 0
   * inside that paren.
   */
  private static boolean innerCallExpects(
      List<? extends Token> tokens, int prefixStart,
      int openerType, int expectedKeyword) {
    java.util.Deque<Integer> openStack = new java.util.ArrayDeque<>();
    int innerOpenerIdx = -1;
    for (int i = 0; i < tokens.size(); i++) {
      Token t = tokens.get(i);
      if (t.getStartIndex() >= prefixStart) {
        break;
      }
      String text = t.getText();
      if ("(".equals(text)) {
        openStack.push(i);
      } else if (")".equals(text)) {
        if (!openStack.isEmpty()) {
          openStack.pop();
        }
      }
    }
    if (openStack.isEmpty()) {
      return false;
    }
    innerOpenerIdx = openStack.peek();
    int prevIdx = innerOpenerIdx - 1;
    while (prevIdx >= 0) {
      Token p = tokens.get(prevIdx);
      int pt = p.getType();
      if (pt == LogicalTypesLexer.WS
          || pt == LogicalTypesLexer.LINE_COMMENT
          || pt == LogicalTypesLexer.BLOCK_COMMENT) {
        prevIdx--;
        continue;
      }
      break;
    }
    if (prevIdx < 0 || tokens.get(prevIdx).getType() != openerType) {
      return false;
    }
    // Confirm `expectedKeyword` hasn't appeared yet at depth 0 inside.
    int depth = 0;
    for (int i = innerOpenerIdx + 1; i < tokens.size(); i++) {
      Token t = tokens.get(i);
      if (t.getStartIndex() >= prefixStart) {
        break;
      }
      String text = t.getText();
      if ("(".equals(text)) {
        depth++;
      } else if (")".equals(text)) {
        depth--;
      } else if (depth == 0 && t.getType() == expectedKeyword) {
        return false;
      }
    }
    return true;
  }

  /**
   * True when {@code keyword} has already been seen at depth 0 inside the
   * innermost open paren, before the caret. Used by SUBSTRING handling to
   * confirm we're past the FROM (so FOR is the next legal continuation).
   */
  private static boolean hasKeywordSinceOpenParen(
      List<? extends Token> tokens, int prefixStart, int keyword) {
    java.util.Deque<Integer> openStack = new java.util.ArrayDeque<>();
    for (int i = 0; i < tokens.size(); i++) {
      Token t = tokens.get(i);
      if (t.getStartIndex() >= prefixStart) {
        break;
      }
      String text = t.getText();
      if ("(".equals(text)) {
        openStack.push(i);
      } else if (")".equals(text)) {
        if (!openStack.isEmpty()) {
          openStack.pop();
        }
      }
    }
    if (openStack.isEmpty()) {
      return false;
    }
    int innerOpenerIdx = openStack.peek();
    int depth = 0;
    for (int i = innerOpenerIdx + 1; i < tokens.size(); i++) {
      Token t = tokens.get(i);
      if (t.getStartIndex() >= prefixStart) {
        return false;
      }
      String text = t.getText();
      if ("(".equals(text)) {
        depth++;
      } else if (")".equals(text)) {
        depth--;
      } else if (depth == 0 && t.getType() == keyword) {
        return true;
      }
    }
    return false;
  }

  /**
   * Detect CASE follow position by scanning back from the caret:
   * <ul>
   *   <li>1 — caret is right after a {@code WHEN <expr>}; suggest THEN.</li>
   *   <li>2 — caret is right after a {@code THEN <expr>}; suggest WHEN/ELSE/END.</li>
   *   <li>3 — caret is right after an {@code ELSE <expr>}; suggest END.</li>
   *   <li>0 — not in a CASE follow position.</li>
   * </ul>
   *
   * <p>Heuristic: walk back through value-then-keyword sequences. If the
   * immediately-prior keyword is WHEN and the value is non-empty → THEN
   * is expected. Etc. Doesn't handle deeply nested CASEs perfectly but
   * covers the common cases.
   */
  private static int caseFollowKeyword(
      List<? extends Token> tokens, int prefixStart) {
    Token prevValue = null;
    Token prevKeyword = null;
    for (int i = 0; i < tokens.size(); i++) {
      Token t = tokens.get(i);
      if (t.getStartIndex() >= prefixStart) {
        break;
      }
      int type = t.getType();
      if (type == LogicalTypesLexer.WS
          || type == LogicalTypesLexer.LINE_COMMENT
          || type == LogicalTypesLexer.BLOCK_COMMENT) {
        continue;
      }
      if (type == WHEN || type == THEN || type == ELSE) {
        prevKeyword = t;
        prevValue = null;
      } else {
        prevValue = t;
      }
    }
    if (prevKeyword == null || prevValue == null) {
      return 0;
    }
    int kt = prevKeyword.getType();
    if (kt == WHEN) {
      return 1;
    }
    if (kt == THEN) {
      return 2;
    }
    if (kt == ELSE) {
      return 3;
    }
    return 0;
  }

  /**
   * If the caret is inside the predicate body of an {@code EVERY/ANY/ONE(arr,
   * <iter>, |)} call, return the iter-var name. Otherwise null.
   *
   * <p>Scan: find the innermost open `(`; check if its preceding identifier
   * is a macro name (EVERY/ANY/ONE); if so, find the second top-level
   * comma-separated arg, which is the iter-var identifier.
   */
  private static String enclosingMacroIterVar(
      List<? extends Token> tokens, int prefixStart) {
    java.util.Deque<Integer> openStack = new java.util.ArrayDeque<>();
    for (int i = 0; i < tokens.size(); i++) {
      Token t = tokens.get(i);
      if (t.getStartIndex() >= prefixStart) {
        break;
      }
      String text = t.getText();
      if ("(".equals(text)) {
        openStack.push(i);
      } else if (")".equals(text)) {
        if (!openStack.isEmpty()) {
          openStack.pop();
        }
      }
    }
    if (openStack.isEmpty()) {
      return null;
    }
    int openIdx = openStack.peek();
    int prevIdx = openIdx - 1;
    while (prevIdx >= 0) {
      Token p = tokens.get(prevIdx);
      int pt = p.getType();
      if (pt == LogicalTypesLexer.WS
          || pt == LogicalTypesLexer.LINE_COMMENT
          || pt == LogicalTypesLexer.BLOCK_COMMENT) {
        prevIdx--;
        continue;
      }
      break;
    }
    if (prevIdx < 0) {
      return null;
    }
    Token caller = tokens.get(prevIdx);
    if (caller.getType() != LogicalTypesLexer.ID
        && caller.getType() != LogicalTypesLexer.QUOTED_ID) {
      return null;
    }
    if (!CHECK_MACRO_NAMES.contains(caller.getText().toUpperCase())) {
      return null;
    }
    // Walk top-level commas inside the macro paren. Iter-var is the second
    // argument; we want it as a column when the caret is inside the third
    // argument.
    int depth = 0;
    int commaCount = 0;
    Token iterVar = null;
    boolean atArgStart = true;
    for (int i = openIdx + 1; i < tokens.size(); i++) {
      Token t = tokens.get(i);
      if (t.getStartIndex() >= prefixStart) {
        break;
      }
      int type = t.getType();
      if (type == LogicalTypesLexer.WS
          || type == LogicalTypesLexer.LINE_COMMENT
          || type == LogicalTypesLexer.BLOCK_COMMENT) {
        continue;
      }
      String text = t.getText();
      if ("(".equals(text)) {
        depth++;
        atArgStart = false;
        continue;
      }
      if (")".equals(text)) {
        depth--;
        atArgStart = false;
        continue;
      }
      if (depth == 0 && ",".equals(text)) {
        commaCount++;
        atArgStart = true;
        continue;
      }
      if (depth == 0 && atArgStart && commaCount == 1) {
        // First token of the second argument — the iter-var name.
        if (type == LogicalTypesLexer.ID || type == LogicalTypesLexer.QUOTED_ID) {
          iterVar = t;
        }
        atArgStart = false;
      } else if (depth == 0) {
        atArgStart = false;
      }
    }
    // Only expose the iter-var when caret is inside the THIRD arg (commaCount == 2).
    if (commaCount >= 2 && iterVar != null) {
      return iterVar.getText();
    }
    return null;
  }

  private List<String> enclosingCheckScope(
      List<? extends Token> tokens, int caretOffset) {
    // Find the index of the most recent CHECK whose `(` is unmatched at
    // caret position. Walk all tokens before the caret, maintaining a
    // simple stack of paren contexts.
    java.util.Deque<ParenCtx> stack = new java.util.ArrayDeque<>();
    int lastCheckBeforeCaret = -1;
    boolean nextOpenIsCheck = false;
    for (int i = 0; i < tokens.size(); i++) {
      Token t = tokens.get(i);
      if (t.getStartIndex() >= caretOffset) {
        break;
      }
      int type = t.getType();
      String text = t.getText();
      if (type == CHECK) {
        nextOpenIsCheck = true;
        continue;
      }
      if ("(".equals(text)) {
        if (nextOpenIsCheck) {
          stack.push(new ParenCtx(true, i));
          lastCheckBeforeCaret = i;
        } else {
          stack.push(new ParenCtx(false, i));
        }
        nextOpenIsCheck = false;
      } else if (")".equals(text)) {
        if (!stack.isEmpty()) {
          stack.pop();
        }
        nextOpenIsCheck = false;
      } else if (type != LogicalTypesLexer.WS
          && type != LogicalTypesLexer.LINE_COMMENT
          && type != LogicalTypesLexer.BLOCK_COMMENT) {
        // Any other token after CHECK without a `(` cancels the pending
        // open-is-check expectation (handles malformed input where the
        // user typed CHECK x without parens).
        nextOpenIsCheck = false;
      }
    }
    // Find the deepest CHECK frame in the stack — `peek` is the immediate
    // enclosing paren which may be a sub-expression inside the CHECK
    // (e.g., `CHECK ((a + |))` has stack [check, other] at the caret).
    // We treat the caret as "inside CHECK" as long as any frame is a CHECK.
    int checkBodyOpenIdx = -1;
    for (ParenCtx ctx : stack) {
      if (ctx.isCheck) {
        checkBodyOpenIdx = ctx.openIdx;
        break;  // innermost CHECK
      }
    }
    if (checkBodyOpenIdx < 0) {
      return null;
    }
    return resolveCheckScope(tokens, checkBodyOpenIdx);
  }

  /** Tracks whether a `(` was opened by a CHECK clause. */
  private static final class ParenCtx {
    final boolean isCheck;
    final int openIdx;

    ParenCtx(boolean isCheck, int openIdx) {
      this.isCheck = isCheck;
      this.openIdx = openIdx;
    }
  }

  /**
   * Given the token index of the CHECK clause's opening `(`, resolve the
   * column scope: column-level → just the attached field; table-level →
   * the surrounding struct's fields. Returns an empty list if the
   * enclosing named-type body can't be located.
   */
  private List<String> resolveCheckScope(
      List<? extends Token> tokens, int checkOpenIdx) {
    // Find the CHECK token (immediately before the `(`).
    int checkIdx = -1;
    for (int j = checkOpenIdx - 1; j >= 0; j--) {
      Token t = tokens.get(j);
      int type = t.getType();
      if (type == LogicalTypesLexer.WS
          || type == LogicalTypesLexer.LINE_COMMENT
          || type == LogicalTypesLexer.BLOCK_COMMENT) {
        continue;
      }
      if (type == CHECK) {
        checkIdx = j;
      }
      break;
    }
    if (checkIdx < 0) {
      return java.util.Collections.emptyList();
    }

    // Walk backward from the CHECK looking for the enclosing struct body.
    // We care about the most-recent unmatched `(` to the left — that is
    // the struct body's opening paren. Track depth.
    int depth = 0;
    int structOpenIdx = -1;
    Integer constraintIdx = null;
    for (int j = checkIdx - 1; j >= 0; j--) {
      Token t = tokens.get(j);
      String text = t.getText();
      int type = t.getType();
      if (")".equals(text)) {
        depth++;
      } else if ("(".equals(text)) {
        if (depth == 0) {
          structOpenIdx = j;
          break;
        }
        depth--;
      } else if (depth == 0 && type == LogicalTypesLexer.CONSTRAINT) {
        constraintIdx = j;
      }
    }
    if (structOpenIdx < 0) {
      return java.util.Collections.emptyList();
    }

    // Decide column-level vs table-level. If the most recent non-WS token
    // before the CHECK (skipping a CONSTRAINT identifier? clause) is `,`
    // or `(`, it's table-level (a constraint sibling of fields). Otherwise
    // it's column-level (the CHECK is attached to a field declaration —
    // walk back to find the field name).
    int sentinelIdx = constraintIdx != null ? constraintIdx : checkIdx;
    int prevIdx = previousNonTrivialIdx(tokens, sentinelIdx);
    if (prevIdx >= 0) {
      String prevText = tokens.get(prevIdx).getText();
      if (",".equals(prevText) || "(".equals(prevText)) {
        // Table-level: every field of the surrounding struct is in scope.
        return extractFieldNames(tokens, structOpenIdx);
      }
    }
    // Column-level: walk back from the CHECK skipping the type and any
    // intervening tokens to find the field-name identifier. The simplest
    // heuristic: the field name is the first identifier whose immediately-
    // prior non-WS token is `(` or `,` (i.e., it opens a fieldDef).
    String fieldName = findEnclosingFieldName(tokens, structOpenIdx, checkIdx);
    if (fieldName == null) {
      return java.util.Collections.emptyList();
    }
    return java.util.Collections.singletonList(fieldName);
  }

  /**
   * Given the opening `(` of a struct body, extract the field names — the
   * identifier that opens each comma-separated entry at depth 1. Skips
   * entries that begin with CHECK or CONSTRAINT (those are table-level
   * constraints, not field declarations).
   */
  private List<String> extractFieldNames(
      List<? extends Token> tokens, int structOpenIdx) {
    List<String> out = new ArrayList<>();
    int depth = 1;
    boolean atEntryStart = true;
    for (int j = structOpenIdx + 1; j < tokens.size(); j++) {
      Token t = tokens.get(j);
      int type = t.getType();
      if (type == LogicalTypesLexer.WS
          || type == LogicalTypesLexer.LINE_COMMENT
          || type == LogicalTypesLexer.BLOCK_COMMENT) {
        continue;
      }
      String text = t.getText();
      if ("(".equals(text)) {
        depth++;
        atEntryStart = false;
        continue;
      }
      if (")".equals(text)) {
        depth--;
        if (depth == 0) {
          break;
        }
        atEntryStart = false;
        continue;
      }
      if (depth == 1 && ",".equals(text)) {
        atEntryStart = true;
        continue;
      }
      if (depth == 1 && atEntryStart) {
        atEntryStart = false;
        if (type == CHECK || type == LogicalTypesLexer.CONSTRAINT) {
          continue;
        }
        if (isIdentifierToken(type)) {
          out.add(t.getText());
        }
      }
    }
    return out;
  }

  /**
   * Best-effort: find the field name that owns a column-level CHECK at
   * {@code checkIdx}. The grammar allows {@code CHECK} after the type,
   * default, and any number of prior CHECKs, so we walk backward at depth 0
   * within the struct body and return the first identifier whose immediate
   * predecessor is `(` or `,` — that's the {@code fieldName}.
   */
  private String findEnclosingFieldName(
      List<? extends Token> tokens, int structOpenIdx, int checkIdx) {
    int depth = 0;
    for (int j = checkIdx - 1; j > structOpenIdx; j--) {
      Token t = tokens.get(j);
      int type = t.getType();
      if (type == LogicalTypesLexer.WS
          || type == LogicalTypesLexer.LINE_COMMENT
          || type == LogicalTypesLexer.BLOCK_COMMENT) {
        continue;
      }
      String text = t.getText();
      if (")".equals(text)) {
        depth++;
        continue;
      }
      if ("(".equals(text)) {
        if (depth > 0) {
          depth--;
          continue;
        }
        // Hit the struct's own open paren — no fieldName found.
        return null;
      }
      if (depth == 0 && isIdentifierToken(type)) {
        int pj = previousNonTrivialIdx(tokens, j);
        if (pj > structOpenIdx) {
          String pt = tokens.get(pj).getText();
          if ("(".equals(pt) || ",".equals(pt)) {
            return t.getText();
          }
        } else if (pj == structOpenIdx) {
          return t.getText();  // first field in the body
        }
      }
    }
    return null;
  }

  /**
   * Index of the most recent non-trivial (non-WS/non-comment) token
   * strictly before {@code idx}, or -1 if none.
   */
  private int previousNonTrivialIdx(List<? extends Token> tokens, int idx) {
    for (int j = idx - 1; j >= 0; j--) {
      int type = tokens.get(j).getType();
      if (type == LogicalTypesLexer.WS
          || type == LogicalTypesLexer.LINE_COMMENT
          || type == LogicalTypesLexer.BLOCK_COMMENT) {
        continue;
      }
      return j;
    }
    return -1;
  }

  /**
   * Collects names that can be referenced as types in this doc:
   * <ul>
   *   <li>{@code ROW <qualifiedName> (...)}</li>
   *   <li>{@code ENUM <qualifiedName> (...)}</li>
   *   <li>{@code REFERENCE TYPE <qualifiedName>}</li>
   * </ul>
   * For each, the full dotted qualifiedName is captured. If a
   * {@code NAMESPACE} is present and the name is unqualified, the
   * namespace-qualified variant is included alongside the bare name (both are
   * valid as references, depending on the use-site's resolution).
   */
  private List<String> declaredTypeNames(List<? extends Token> tokens) {
    String namespace = findNamespace(tokens);
    List<String> out = new ArrayList<>();
    int n = tokens.size();
    int i = 0;
    while (i < n) {
      Token a = tokens.get(i);
      int kind = a.getType();
      // Statement-leading ROW or ENUM at statement-start position opens a
      // named-type declaration. Statement-start = either the first token,
      // or a token immediately after a `;`.
      if ((kind == ROW || kind == ENUM) && isStatementStart(tokens, i)) {
        int[] consumed = new int[1];
        String name = readQualifiedName(tokens, i + 1, consumed);
        if (name != null && i + 1 + consumed[0] < n
            && "(".equals(tokens.get(i + 1 + consumed[0]).getText())) {
          addNameAndQualified(out, name, namespace);
          i += 1 + consumed[0];
          continue;
        }
      } else if (kind == REFERENCE && i + 1 < n
          && tokens.get(i + 1).getType() == TYPE) {
        int[] consumed = new int[1];
        String name = readQualifiedName(tokens, i + 2, consumed);
        if (name != null) {
          addNameAndQualified(out, name, namespace);
          i += 2 + consumed[0];
          continue;
        }
      }
      i++;
    }
    return out;
  }

  /**
   * True when the token at index {@code i} sits at the start of a statement
   * — either the first non-trivia token in the doc, or directly after a `;`.
   */
  private static boolean isStatementStart(List<? extends Token> tokens, int i) {
    for (int j = i - 1; j >= 0; j--) {
      int t = tokens.get(j).getType();
      if (t == LogicalTypesLexer.WS
          || t == LogicalTypesLexer.LINE_COMMENT
          || t == LogicalTypesLexer.BLOCK_COMMENT) {
        continue;
      }
      return ";".equals(tokens.get(j).getText());
    }
    return true;
  }

  /** First NAMESPACE qualifiedName in the token list, or null. */
  private static String findNamespace(List<? extends Token> tokens) {
    int n = tokens.size();
    for (int i = 0; i + 1 < n; i++) {
      if (tokens.get(i).getType() == NAMESPACE && isStatementStart(tokens, i)) {
        int[] consumed = new int[1];
        return readQualifiedName(tokens, i + 1, consumed);
      }
    }
    return null;
  }

  /**
   * Read {@code identifier ('.' identifier)*} starting at {@code start}. The
   * grammar allows {@link #isIdentifierToken non-reserved keywords} as
   * identifiers, so {@code io.type.Foo} tokenizes as ID, '.', TYPE, '.', ID
   * — all three pieces are part of the qualified name. Returns null if the
   * start token isn't an identifier. Sets {@code consumed[0]} to the number of
   * tokens consumed.
   */
  private static String readQualifiedName(
      List<? extends Token> tokens, int start, int[] consumed) {
    consumed[0] = 0;
    int n = tokens.size();
    if (start >= n || !isIdentifierToken(tokens.get(start).getType())) {
      return null;
    }
    StringBuilder sb = new StringBuilder(tokens.get(start).getText());
    int i = start + 1;
    consumed[0] = 1;
    while (i + 1 < n
        && ".".equals(tokens.get(i).getText())
        && isIdentifierToken(tokens.get(i + 1).getType())) {
      sb.append('.').append(tokens.get(i + 1).getText());
      i += 2;
      consumed[0] += 2;
    }
    return sb.toString();
  }

  private static final Set<Integer> IDENTIFIER_TOKEN_TYPES = Set.of(
      LogicalTypesLexer.ID, LogicalTypesLexer.QUOTED_ID,
      ENUM, MAP, NAMESPACE, REFERENCE,
      LogicalTypesLexer.TAGS, TYPE, VARIANT, ZONE);

  /**
   * Token types the grammar's {@code identifier} rule accepts: ID, QUOTED_ID,
   * and any non-reserved keyword.
   */
  private static boolean isIdentifierToken(int tokenType) {
    return IDENTIFIER_TOKEN_TYPES.contains(tokenType);
  }

  private static void addNameAndQualified(
      List<String> out, String name, String namespace) {
    out.add(name);
    if (namespace != null && !name.contains(".")) {
      out.add(namespace + "." + name);
    }
  }

  private static boolean isIdChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  private static int[] concat(int[] a, int[] b) {
    int[] out = new int[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
