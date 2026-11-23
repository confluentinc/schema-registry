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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypesParserFactory;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintToCelTranslator;
import io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintValidationContext;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * CHECK constraints of a struct type: compiling their SQL to CEL, the columns they use, renaming
 * those, and the names given to constraints written without one, as PostgreSQL names them.
 */
public final class Constraints {

  // The macros whose second argument names an iteration variable bound in the third.
  private static final Set<String> MACROS = Set.of("EVERY", "ANY", "ONE");

  private Constraints() {
  }

  /** A constraint compiled against the columns in scope. */
  public static Rule compile(String name, String message, String sql,
      ConstraintValidationContext scope) {
    try {
      return new Rule(name, message, ConstraintToCelTranslator.translate(parse(sql), scope),
          sql);
    } catch (RuntimeException e) {
      throw new TypeRefusedException("Invalid CHECK (" + sql + "): " + e.getMessage(), e);
    }
  }

  public static Rule compileTable(Rule rule, List<Field> fields, Map<String, Schema> namedTypes) {
    return compile(rule.getName(), rule.getDoc(), rule.getSql(),
        ConstraintValidationContext.tableLevel(fields).withNamedTypes(namedTypes));
  }

  public static Rule compileColumn(Rule rule, Field field, Map<String, Schema> namedTypes) {
    return compile(rule.getName(), rule.getDoc(), rule.getSql(),
        ConstraintValidationContext.columnLevel(field.getName(), field.getSchema())
          .withNamedTypes(namedTypes));
  }

  /** The columns a constraint uses, by their first name in each column reference. */
  public static Set<String> columns(String sql) {
    Set<String> columns = new LinkedHashSet<>();
    collect(parse(sql), Set.of(), columns, null);
    return columns;
  }

  /** The iteration variables a constraint's macros bind. */
  public static Set<String> variables(String sql) {
    Set<String> variables = new LinkedHashSet<>();
    collectVariables(parse(sql), variables);
    return variables;
  }

  private static void collectVariables(ParseTree tree, Set<String> variables) {
    List<LogicalTypesParser.Check_exprContext> args = macroArgs(tree);
    if (args != null) {
      variables.add(args.get(1).getText());
    }
    for (int i = 0; i < tree.getChildCount(); i++) {
      collectVariables(tree.getChild(i), variables);
    }
  }

  /** A constraint's SQL with a column renamed. */
  public static String rename(String sql, String from, String to) {
    List<Token> refs = new ArrayList<>();
    collect(parse(sql), Set.of(), null, refs);
    String name = bare(to) ? to : TypeScripts.quote(to);
    StringBuilder sb = new StringBuilder(sql);
    // From the end, so that earlier offsets still hold; token indexes count code points.
    for (int i = refs.size() - 1; i >= 0; i--) {
      Token ref = refs.get(i);
      if (unquote(ref.getText()).equals(from)) {
        sb.replace(sql.offsetByCodePoints(0, ref.getStartIndex()),
            sql.offsetByCodePoints(0, ref.getStopIndex() + 1), name);
      }
    }
    return sb.toString();
  }

  // Whether a name reads back as itself unquoted, which a reserved word such as dec does not.
  private static boolean bare(String name) {
    if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      return false;
    }
    try {
      return columns(name).equals(Set.of(name));
    } catch (TypeRefusedException e) {
      return false;
    }
  }

  /**
   * A struct type with a name for each constraint of its own or of an attribute written
   * without.
   */
  public static LogicalType named(LogicalType lt, String subject) {
    Set<String> used = new HashSet<>();
    addGiven(lt, used);
    return named(lt, subject, used);
  }

  /**
   * As {@link #named(LogicalType, String)}, making no name in used, those given here and in
   * another type, or made for it, and adding those it makes.
   */
  public static LogicalType named(LogicalType lt, String subject, Set<String> used) {
    Schema root = lt.getRootSchema();
    boolean ref = root.getType() == Schema.Type.NAMED_TYPE_REF;
    Schema struct = ref ? lt.getNamedTypes().get(root.getQualifiedName()) : root;
    if (struct == null || struct.getType() != Schema.Type.STRUCT) {
      return lt;
    }
    String base = base(subject);
    List<Field> fields = new ArrayList<>();
    for (Field f : struct.getFields()) {
      fields.add(withRules(f, namedRules(f.getRules(), base + "_" + f.getName(), used)));
    }
    Schema copy = Schema.createStruct(fields).setNullable(struct.isNullable())
        .setDoc(struct.getDoc()).setTags(struct.getTags()).setParams(struct.getParams())
        .setRules(namedRules(struct.getRules(), base, used));
    if (!ref) {
      return TypeScripts.withRoot(lt, copy, lt.getNamedTypes());
    }
    Map<String, Schema> namedTypes = new LinkedHashMap<>(lt.getNamedTypes());
    namedTypes.put(root.getQualifiedName(), copy);
    return TypeScripts.withRoot(lt, root, namedTypes);
  }

  /** The names of a struct's constraints and its attributes', in lower case. */
  public static Set<String> names(Schema struct) {
    Set<String> names = new LinkedHashSet<>();
    for (Rule r : struct.getRules()) {
      add(names, r);
    }
    for (Field f : struct.getFields()) {
      for (Rule r : f.getRules()) {
        add(names, r);
      }
    }
    return names;
  }

  // Adds the names given to a struct type's constraints and its attributes' to used, in lower
  // case; refused if one is given twice, here or among those added before, as PostgreSQL does.
  public static void addGiven(LogicalType lt, Set<String> used) {
    Schema root = lt.getRootSchema();
    Schema struct = root.getType() == Schema.Type.NAMED_TYPE_REF
        ? lt.getNamedTypes().get(root.getQualifiedName()) : root;
    if (struct == null || struct.getType() != Schema.Type.STRUCT) {
      return;
    }
    List<Rule> rules = new ArrayList<>(struct.getRules());
    struct.getFields().forEach(f -> rules.addAll(f.getRules()));
    for (Rule r : rules) {
      if (r.getName() != null && !used.add(r.getName().toLowerCase(Locale.ROOT))) {
        throw new TypeRefusedException("Constraint " + r.getName() + " is named more than once");
      }
    }
  }

  /** A name not yet used, prefix_check, then prefix_check1, ..., recorded as used. */
  public static String newName(String prefix, Set<String> used) {
    String name = prefix + "_check";
    for (int i = 1; used.contains(name.toLowerCase(Locale.ROOT)); i++) {
      name = prefix + "_check" + i;
    }
    used.add(name.toLowerCase(Locale.ROOT));
    return name;
  }

  /** The prefix of generated names: the subject, without a context, as an identifier. */
  public static String base(String subject) {
    String unqualified = subject.startsWith(":.")
        ? subject.substring(subject.indexOf(':', 2) + 1) : subject;
    return LogicalTypeConversion.rowName(unqualified);
  }

  public static Field withRules(Field f, List<Rule> rules) {
    return new Field(f.getName(), f.getSchema(), f.getPosition(), f.getDefaultValue(),
        f.hasDefaultValue(), f.hasDerivedDefault(), f.getDoc(), f.getTags(), f.getParams(),
        rules);
  }

  private static List<Rule> namedRules(List<Rule> rules, String prefix, Set<String> used) {
    List<Rule> named = new ArrayList<>();
    for (Rule r : rules) {
      named.add(r.getName() != null ? r
          : new Rule(newName(prefix, used), r.getDoc(), r.getExpr(), r.getSql()));
    }
    return named;
  }

  private static void add(Set<String> names, Rule r) {
    if (r.getName() != null) {
      names.add(r.getName().toLowerCase(Locale.ROOT));
    }
  }

  private static LogicalTypesParser.Check_exprContext parse(String sql) {
    LogicalTypesParser parser = LogicalTypesParserFactory.newParser(sql);
    try {
      LogicalTypesParser.Check_exprContext tree = parser.check_expr();
      if (parser.getCurrentToken().getType() != Token.EOF) {
        throw new TypeRefusedException("Invalid CHECK (" + sql + "): unexpected '"
            + parser.getCurrentToken().getText() + "'");
      }
      return tree;
    } catch (TypeRefusedException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new TypeRefusedException("Invalid CHECK (" + sql + "): " + e.getMessage(), e);
    }
  }

  // Each column reference's first name: the column; any further names are fields within it. A
  // name that an enclosing macro binds is its iteration variable, not a column.
  private static void collect(ParseTree tree, Set<String> bound, Set<String> names,
                  List<Token> tokens) {
    List<LogicalTypesParser.Check_exprContext> args = macroArgs(tree);
    if (args != null) {
      collect(args.get(0), bound, names, tokens);
      Set<String> inner = new HashSet<>(bound);
      inner.add(args.get(1).getText());
      collect(args.get(2), inner, names, tokens);
      return;
    }
    if (tree instanceof LogicalTypesParser.ColumnrefContext) {
      LogicalTypesParser.ColidContext colid = ((LogicalTypesParser.ColumnrefContext) tree).colid();
      if (!bound.contains(unquote(colid.getText()))) {
        if (names != null) {
          names.add(unquote(colid.getText()));
        }
        if (tokens != null) {
          tokens.add(colid.getStart());
        }
      }
    }
    for (int i = 0; i < tree.getChildCount(); i++) {
      collect(tree.getChild(i), bound, names, tokens);
    }
  }

  // The arguments of an EVERY, ANY or ONE macro: a list, an iteration variable and a predicate;
  // null for anything else.
  private static List<LogicalTypesParser.Check_exprContext> macroArgs(ParseTree tree) {
    if (!(tree instanceof LogicalTypesParser.Func_applicationContext)) {
      return null;
    }
    LogicalTypesParser.Func_applicationContext f =
        (LogicalTypesParser.Func_applicationContext) tree;
    if (f.check_expr_list() == null
        || !MACROS.contains(f.identifier().getText().toUpperCase(Locale.ROOT))) {
      return null;
    }
    List<LogicalTypesParser.Check_exprContext> args = f.check_expr_list().check_expr();
    return args.size() == 3 ? args : null;
  }

  private static String unquote(String id) {
    return id.length() >= 2 && id.startsWith("`") && id.endsWith("`")
        ? id.substring(1, id.length() - 1).replace("``", "`") : id;
  }
}
