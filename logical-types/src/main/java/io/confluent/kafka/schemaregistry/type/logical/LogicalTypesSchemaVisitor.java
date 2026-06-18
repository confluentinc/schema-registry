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

import io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintValidationContext;
import io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintToCelTranslator;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesBaseVisitor;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ANTLR visitor that converts a parsed DDL script into a {@link LogicalType}.
 *
 * <p>Visitor outputs (consumed by {@link #toLogicalType}):
 * <ul>
 *   <li>{@code namespace} — from any {@code NAMESPACE} statement.</li>
 *   <li>{@code namedTypes} — from each {@code STRUCT <name> (...)} or
 *       {@code ENUM <name> (...)} statement (locals).</li>
 *   <li>{@code externalImports} — from each
 *       {@code DECLARE x FOR 'uri'} statement; maps the typeName to the
 *       wire-format URI.</li>
 *   <li>{@code rootSchema} — from the trailing {@code TYPE <typeExpr>}
 *       statement (or sugar-inferred when absent).</li>
 * </ul>
 *
 * <p>{@code externalTypes} is <b>inferred</b> by {@link #toLogicalType}:
 * any {@code NAMED_TYPE_REF} FQN reachable from the root or from a local body
 * that isn't declared locally is treated as external. There is no syntactic
 * marker; bare names always inherit the active {@code NAMESPACE}, matching
 * Avro's rule.
 *
 * <p><b>What the visitor does NOT produce: the {@code references} list.</b>
 * The DDL grammar has no syntax for SR-side coordinates (subject + version)
 * because those are deployment metadata that vary across environments and
 * shouldn't be embedded in schema source. So the produced LT has an empty
 * {@code references} list. The caller is responsible for attaching
 * {@code references} (and the corresponding {@code resolvedReferences}
 * content) before SR registration. {@code DECLARE} populates schema-text shape
 * (which {@code $ref}/{@code import} string to emit); it does not bridge to SR
 * resolution.
 */
public class LogicalTypesSchemaVisitor extends LogicalTypesBaseVisitor<Object> {

  private String namespace;
  private final Map<String, String> externalImports = new LinkedHashMap<>();
  private final Map<String, Schema> namedTypes = new LinkedHashMap<>();
  private Schema rootSchema;

  public String getNamespace() {
    return namespace;
  }

  /**
   * URI bindings for {@code DECLARE x FOR '<uri>';} declarations. Becomes
   * {@link LogicalType#getExternalImports()} on {@link #toLogicalType}.
   */
  public Map<String, String> getExternalImports() {
    return externalImports;
  }

  public Map<String, Schema> getNamedTypes() {
    return namedTypes;
  }

  public Schema getRootSchema() {
    return rootSchema;
  }

  /**
   * Convenience: bundles all visitor outputs into a {@link LogicalType},
   * including the namespace from any {@code NAMESPACE} statement.
   *
   * <p>{@link LogicalType#getExternalTypes()} is inferred: every
   * {@code NAMED_TYPE_REF} FQN reachable from the root or any local body that
   * isn't itself a local declaration is treated as external. Downstream
   * enrichment is responsible for SR-fetching and promoting bodies into
   * {@code namedTypes}.
   *
   * <p>DECLARE validation (no shadowing, no dangling entries) runs during
   * parsing inside {@link #visitScript}, so a {@code LogicalType} produced
   * here is guaranteed to satisfy both invariants.
   */
  public LogicalType toLogicalType() {
    return new LogicalType(namespace, rootSchema, namedTypes,
        inferExternalTypes(),
        externalImports,
        java.util.Collections.emptyList(), java.util.Collections.emptyMap(),
        java.util.Collections.emptyMap());
  }

  /**
   * Walk the root + every local body, collect every {@code NAMED_TYPE_REF}
   * FQN, subtract the locally-declared names. What remains is the inferred
   * set of external types.
   */
  private Set<String> inferExternalTypes() {
    Set<String> refs = new LinkedHashSet<>();
    if (rootSchema != null) {
      LogicalType.collectNamedRefs(rootSchema, refs);
    }
    for (Schema body : namedTypes.values()) {
      LogicalType.collectNamedRefs(body, refs);
    }
    refs.removeAll(namedTypes.keySet());
    return refs;
  }

  // =========================================================================
  // Statement-level visitors
  // =========================================================================

  @Override
  public Object visitScript(LogicalTypesParser.ScriptContext ctx) {
    if (ctx.declareNamespaceStmt() != null) {
      visit(ctx.declareNamespaceStmt());
    }
    for (LogicalTypesParser.AliasStmtContext stmt : ctx.aliasStmt()) {
      visit(stmt);
    }
    // Pre-pass: register every named-type declaration's qualified name (with
    // an empty placeholder) before building any body. This lets a parent's
    // body reference its own nested children by name (e.g., `STRUCT Outer
    // (i Outer.Inner); STRUCT Outer.Inner (...)`). Qualification still happens
    // incrementally — each declaration's qualification consults the names
    // registered by previously-visited declarations in this same pre-pass.
    for (LogicalTypesParser.CreateTypeStmtContext stmt : ctx.createTypeStmt()) {
      preRegisterCreateTypeName(stmt);
    }
    for (LogicalTypesParser.CreateTypeStmtContext stmt : ctx.createTypeStmt()) {
      visit(stmt);
    }
    if (ctx.registerTypeStmt() != null) {
      visit(ctx.registerTypeStmt());
    } else {
      inferAndRegisterRoot(ctx);
    }
    validateAliases();
    return null;
  }

  /**
   * After parsing completes, reject DECLARE declarations that either shadow a
   * local declaration or aren't referenced by any local body / the root.
   * Externals are inferred from usage, so an unused DECLARE has no effect and
   * an DECLARE-on-local-name is contradictory — both are surfaced as errors at
   * parse time rather than allowed to silently no-op.
   */
  private void validateAliases() {
    if (externalImports.isEmpty()) {
      return;
    }
    Set<String> shadowed = new LinkedHashSet<>(externalImports.keySet());
    shadowed.retainAll(namedTypes.keySet());
    if (!shadowed.isEmpty()) {
      throw new ValidationException(
          "DECLARE declared for name(s) that are also locally declared as "
              + "STRUCT/ENUM — an DECLARE attaches a URI to an external reference, "
              + "so it must not collide with a local declaration: " + shadowed);
    }
    Set<String> externals = inferExternalTypes();
    Set<String> dangling = new LinkedHashSet<>(externalImports.keySet());
    dangling.removeAll(externals);
    if (!dangling.isEmpty()) {
      throw new ValidationException(
          "DECLARE declared for name(s) that aren't referenced by any local "
              + "type or by the root — externals are inferred from usage, so "
              + "an unused DECLARE has no effect: " + dangling);
    }
  }

  /**
   * Pre-pass for {@link #visitScript}: extract each named-type declaration's
   * qualified name and put a placeholder STRUCT/ENUM in {@link #namedTypes}.
   * The body is filled in by {@link #visitCreateTypeStmt} during the second
   * pass.
   */
  private void preRegisterCreateTypeName(LogicalTypesParser.CreateTypeStmtContext ctx) {
    String name = qualifyWithNamespace(buildQualifiedName(ctx.qualifiedName()));
    if (namedTypes.containsKey(name)) {
      throw error(ctx.qualifiedName(), "Duplicate named type: " + name);
    }
    // Gap-rejection: parent of a nested name must already exist in this
    // pre-pass, which means the user must declare parents before children.
    String parent = LogicalType.parentOf(name, namedTypes.keySet());
    if (parent != null) {
      int lastDot = name.lastIndexOf('.');
      String expectedParent = name.substring(0, lastDot);
      if (!parent.equals(expectedParent)) {
        throw error(ctx.qualifiedName(),
            "Nested type '" + name + "' has no immediate parent. "
                + "Found ancestor '" + parent + "' but missing '"
                + expectedParent + "'. Declare '" + expectedParent + "' first.");
      }
    }
    Schema placeholder = (ctx.structBody() != null)
        ? Schema.createStruct(new ArrayList<>())
        : Schema.createEnum(new ArrayList<>());
    namedTypes.put(name, placeholder);
  }

  /**
   * Sugar: when the script has no explicit trailing {@code TYPE <typeExpr>}
   * statement, infer the unique root from the dependency graph of named-type
   * declarations and register it as if the user had written
   * {@code TYPE <root> NOT NULL} at the end.
   *
   * <p>The unique root is the single defined type that is not referenced by
   * any other defined type. Externals (inferred from any FQN used in a type
   * position that isn't declared locally) don't influence the in-script
   * dependency graph.
   *
   * <p><b>Note on nullability:</b> the sugar bakes in {@code NOT NULL} because
   * a registered root is almost always non-null at the wire level (Avro would
   * otherwise emit {@code [null, T]}, Protobuf disallows null roots, etc.).
   * The explicit form ({@code TYPE Foo} with no marker) intentionally defers
   * to the grammar's general typeExpr default, which is <em>nullable</em> —
   * same as a field type in any other position. So {@code STRUCT Foo (...)}
   * (sugar) and {@code STRUCT Foo (...); TYPE Foo} (explicit) produce different
   * root nullability by design. Callers wanting a nullable root must write
   * the trailing {@code TYPE Foo} (or {@code ... NULL}) explicitly.
   *
   * <p>Multi-root: when more than one defined type is unreferenced (e.g.,
   * a script with several peer messages and no explicit trailing
   * registration), the inferred root is a {@code UNION} of
   * {@code NAMED_TYPE_REF}s to all of them. Avro/JSON writers emit this as a
   * tagged union / {@code oneOf}; the proto writer detects the shape and
   * treats it as a multi-message file (first member becomes the primary root,
   * rest emit as peer messages).
   *
   * <p>Errors:
   * <ul>
   *   <li>no named-type declarations → "no type to register"
   *   <li>every defined type is referenced by another (cycle) → cycle error
   * </ul>
   */
  private void inferAndRegisterRoot(LogicalTypesParser.ScriptContext ctx) {
    if (namedTypes.isEmpty()) {
      throw error(ctx,
          "No type to register. Add a 'STRUCT <name> (...)' or 'ENUM <name> (...)' "
              + "declaration (which the script will auto-register) or a trailing "
              + "'TYPE <typeExpr>' statement.");
    }

    // For each defined type, the set of in-script defined types it references.
    Map<String, Set<String>> uses = new LinkedHashMap<>();
    Set<String> defined = namedTypes.keySet();
    for (Map.Entry<String, Schema> e : namedTypes.entrySet()) {
      Set<String> refs = new LinkedHashSet<>();
      LogicalType.collectNamedRefs(e.getValue(), refs);
      refs.retainAll(defined);
      // A self-recursive named type (e.g. STRUCT Node (next Node)) is
      // still a valid root — it just refers to itself. Strip self-references
      // so the type isn't mistakenly flagged as "referenced by another".
      refs.remove(e.getKey());
      uses.put(e.getKey(), refs);
    }

    Set<String> referenced = new LinkedHashSet<>();
    for (Set<String> r : uses.values()) {
      referenced.addAll(r);
    }
    Set<String> roots = new LinkedHashSet<>(defined);
    roots.removeAll(referenced);
    // Nested types can never be the root — even if no sibling references them
    // (which would make them orphans, not roots). Filter them out so they don't
    // pollute the "no unique root" / "multiple roots" diagnostics.
    roots.removeIf(name -> LogicalType.parentOf(name, defined) != null);

    if (roots.size() == 1) {
      String name = roots.iterator().next();
      Schema ref = Schema.createNamedTypeRef(name);
      ref.setNullable(false);
      rootSchema = ref;
      return;
    }

    if (roots.isEmpty()) {
      List<String> cycle = findCycle(uses);
      throw error(ctx,
          "Cannot infer the type to register: dependency cycle among defined types: "
              + String.join(" -> ", cycle)
              + ". Add an explicit trailing 'TYPE <name>' statement.");
    }

    // Multiple unreferenced top-level types → wrap them in a UNION at the root.
    // Avro/JSON emit this as a tagged union / oneOf, which is the natural
    // multi-message representation. The proto writer detects this shape (UNION
    // whose members are all NAMED_TYPE_REFs to local types at the root) and
    // treats it as a multi-message file: the first member becomes the primary
    // root, the rest emit as peer messages via the existing namedTypes path.
    List<Schema.UnionBranch> branches = new ArrayList<>(roots.size());
    for (String name : roots) {
      branches.add(new Schema.UnionBranch(
          simpleNameOf(name),
          Schema.createNamedTypeRef(name).setNullable(false),
          null, null));
    }
    rootSchema = Schema.createUnion(branches).setNullable(false);
  }

  /**
   * Simple-name slice of a qualified name: everything after the last dot, or
   * the full name if it's unqualified. Used as the union branch name in
   * multi-root sugar.
   */
  private static String simpleNameOf(String qualifiedName) {
    int dot = qualifiedName.lastIndexOf('.');
    return dot < 0 ? qualifiedName : qualifiedName.substring(dot + 1);
  }

  /**
   * Find a sample cycle in the dependency graph. Returns the sequence of node
   * names along the cycle (with the start node repeated at the end), suitable
   * for a "A -> B -> C -> A" error message.
   */
  private static List<String> findCycle(Map<String, Set<String>> uses) {
    Set<String> visited = new HashSet<>();
    for (String start : uses.keySet()) {
      if (visited.contains(start)) {
        continue;
      }
      List<String> path = new ArrayList<>();
      Set<String> onPath = new LinkedHashSet<>();
      List<String> cycle = dfsFindCycle(start, uses, visited, onPath, path);
      if (cycle != null) {
        return cycle;
      }
    }
    return List.of("?"); // unreachable when called with a known cycle
  }

  private static List<String> dfsFindCycle(
      String node, Map<String, Set<String>> uses,
      Set<String> visited, Set<String> onPath, List<String> path) {
    visited.add(node);
    onPath.add(node);
    path.add(node);
    for (String next : uses.getOrDefault(node, Set.of())) {
      if (onPath.contains(next)) {
        // Found cycle. Trim path to start at `next`.
        List<String> cycle = new ArrayList<>();
        boolean started = false;
        for (String p : path) {
          if (p.equals(next)) {
            started = true;
          }
          if (started) {
            cycle.add(p);
          }
        }
        cycle.add(next);
        return cycle;
      }
      if (!visited.contains(next)) {
        List<String> result = dfsFindCycle(next, uses, visited, onPath, path);
        if (result != null) {
          return result;
        }
      }
    }
    onPath.remove(node);
    path.remove(path.size() - 1);
    return null;
  }

  @Override
  public Object visitDeclareNamespaceStmt(LogicalTypesParser.DeclareNamespaceStmtContext ctx) {
    namespace = buildQualifiedName(ctx.qualifiedName());
    return null;
  }

  @Override
  public Object visitAliasStmt(LogicalTypesParser.AliasStmtContext ctx) {
    String name = buildQualifiedName(ctx.qualifiedName());
    if (externalImports.containsKey(name)) {
      throw error(ctx.qualifiedName(),
          "Duplicate DECLARE: " + name
              + " (each alias FQN may be declared at most once)");
    }
    String uri = stripStringLiteral(ctx.stringLiteral().getText());
    if (uri.trim().isEmpty()) {
      throw error(ctx.stringLiteral(),
          "DECLARE " + name + " FOR clause must be a non-empty URI");
    }
    externalImports.put(name, uri);
    return null;
  }

  @Override
  public Object visitCreateTypeStmt(LogicalTypesParser.CreateTypeStmtContext ctx) {
    final String name = qualifyWithNamespace(buildQualifiedName(ctx.qualifiedName()));
    Schema schema = (ctx.structBody() != null)
        ? buildStruct(ctx.structBody())
        : buildEnum(ctx.enumBody());
    attachMetadata(schema, ctx.commentClause(), ctx.tagsClause(), ctx.withClause());
    putNamedType(name, schema, ctx.qualifiedName());
    return null;
  }

  @Override
  public Object visitRegisterTypeStmt(LogicalTypesParser.RegisterTypeStmtContext ctx) {
    rootSchema = (Schema) visit(ctx.typeExpr());
    rejectNestedRoot(rootSchema, ctx);
    return null;
  }

  /**
   * A nested type (one with a parent in {@link #namedTypes}) may not be the
   * registered root — only top-level types can be roots. The user should
   * register the top-level ancestor instead.
   */
  private void rejectNestedRoot(Schema root, ParserRuleContext ctx) {
    if (root == null || root.getType() != Schema.Type.NAMED_TYPE_REF) {
      return;
    }
    String name = root.getQualifiedName();
    String parent = LogicalType.parentOf(name, namedTypes.keySet());
    if (parent == null) {
      return;
    }
    String topLevel = name;
    String p;
    while ((p = LogicalType.parentOf(topLevel, namedTypes.keySet())) != null) {
      topLevel = p;
    }
    throw error(ctx,
        "Cannot register nested type '" + name
            + "' as the root; register '" + topLevel + "' instead.");
  }

  /**
   * Replace a placeholder (set by {@link #preRegisterCreateTypeName}) with the
   * fully-built body. Duplicate detection and gap-rejection happened in the
   * pre-pass, so this is just a write here.
   */
  private void putNamedType(
      String name, Schema schema, LogicalTypesParser.QualifiedNameContext nameCtx) {
    namedTypes.put(name, schema);
  }

  private void attachMetadata(
      Schema schema,
      LogicalTypesParser.CommentClauseContext comment,
      LogicalTypesParser.TagsClauseContext tags,
      LogicalTypesParser.WithClauseContext withC) {
    if (comment != null) {
      schema.setDoc(stripStringLiteral(comment.stringLiteral().getText()));
    }
    if (tags != null) {
      schema.setTags(tags.stringLiteral().stream()
          .map(s -> stripStringLiteral(s.getText()))
          .collect(Collectors.toList()));
    }
    if (withC != null) {
      schema.setParams(buildParams(withC));
    }
  }

  // =========================================================================
  // Type expression visitors — each returns a Schema
  // =========================================================================

  @Override
  public Schema visitPrimitiveTypeExpr(LogicalTypesParser.PrimitiveTypeExprContext ctx) {
    Schema schema = (Schema) visit(ctx.primitiveType());
    return applyNullability(schema, ctx.nullability());
  }

  @Override
  public Schema visitVariantTypeExpr(LogicalTypesParser.VariantTypeExprContext ctx) {
    Schema schema = Schema.create(Schema.Type.VARIANT);
    return applyNullability(schema, ctx.nullability());
  }

  @Override
  public Schema visitRowTypeExpr(LogicalTypesParser.RowTypeExprContext ctx) {
    Schema schema = (Schema) visit(ctx.rowType());
    return applyNullability(schema, ctx.nullability());
  }

  @Override
  public Schema visitUnionTypeExpr(LogicalTypesParser.UnionTypeExprContext ctx) {
    Schema schema = (Schema) visit(ctx.unionType());
    return applyNullability(schema, ctx.nullability());
  }

  @Override
  public Schema visitMapTypeExpr(LogicalTypesParser.MapTypeExprContext ctx) {
    Schema schema = (Schema) visit(ctx.mapType());
    return applyNullability(schema, ctx.nullability());
  }

  @Override
  public Schema visitQualifiedNameExpr(LogicalTypesParser.QualifiedNameExprContext ctx) {
    Schema schema = Schema.createNamedTypeRef(
        qualifyTypeRef(buildQualifiedName(ctx.qualifiedName())));
    return applyNullability(schema, ctx.nullability());
  }

  @Override
  public Schema visitPrefixArray(LogicalTypesParser.PrefixArrayContext ctx) {
    Schema elementType = (Schema) visit(ctx.typeExpr());
    Schema schema = Schema.createArray(elementType);
    return applyNullability(schema, ctx.nullability());
  }

  @Override
  public Schema visitPostfixArray(LogicalTypesParser.PostfixArrayContext ctx) {
    Schema elementType = (Schema) visit(ctx.typeExpr());
    Schema schema = Schema.createArray(elementType);
    return applyNullability(schema, ctx.nullability());
  }

  @Override
  public Schema visitPrefixMultiset(LogicalTypesParser.PrefixMultisetContext ctx) {
    Schema elementType = (Schema) visit(ctx.typeExpr());
    Schema schema = Schema.createMultiset(elementType);
    return applyNullability(schema, ctx.nullability());
  }

  @Override
  public Schema visitPostfixMultiset(LogicalTypesParser.PostfixMultisetContext ctx) {
    Schema elementType = (Schema) visit(ctx.typeExpr());
    Schema schema = Schema.createMultiset(elementType);
    return applyNullability(schema, ctx.nullability());
  }

  private Schema applyNullability(Schema schema,
                                  LogicalTypesParser.NullabilityContext ctx) {
    if (ctx != null && ctx.NOT() != null) {
      schema.setNullable(false);
    }
    return schema;
  }

  // =========================================================================
  // Primitive type visitor
  // =========================================================================

  @Override
  public Schema visitPrimitiveType(LogicalTypesParser.PrimitiveTypeContext ctx) {
    if (ctx.BOOLEAN() != null) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    if (ctx.TINYINT() != null) {
      return Schema.create(Schema.Type.TINYINT);
    }
    if (ctx.SMALLINT() != null) {
      return Schema.create(Schema.Type.SMALLINT);
    }
    if (ctx.INTEGER() != null) {
      return Schema.create(Schema.Type.INT);
    }
    if (ctx.INT() != null) {
      return Schema.create(Schema.Type.INT);
    }
    if (ctx.BIGINT() != null) {
      return Schema.create(Schema.Type.BIGINT);
    }
    if (ctx.FLOAT() != null) {
      return Schema.create(Schema.Type.FLOAT);
    }
    if (ctx.REAL() != null) {
      return Schema.create(Schema.Type.FLOAT);
    }
    if (ctx.DOUBLE() != null) {
      return Schema.create(Schema.Type.DOUBLE);
    }
    if (ctx.DECIMAL() != null || ctx.DEC() != null || ctx.NUMERIC() != null) {
      return buildDecimal(ctx);
    }
    if (ctx.CHARACTER() != null) {
      if (ctx.VARYING() != null) {
        return buildLengthType(ctx, Schema.Type.VARCHAR);
      }
      return buildLengthType(ctx, Schema.Type.CHAR);
    }
    if (ctx.VARCHAR() != null) {
      return buildLengthType(ctx, Schema.Type.VARCHAR);
    }
    if (ctx.STRING() != null) {
      return Schema.createString();
    }
    if (ctx.CHAR() != null) {
      return buildLengthType(ctx, Schema.Type.CHAR);
    }
    if (ctx.BINARY() != null) {
      if (ctx.VARYING() != null) {
        return buildLengthType(ctx, Schema.Type.VARBINARY);
      }
      return buildLengthType(ctx, Schema.Type.BINARY);
    }
    if (ctx.VARBINARY() != null) {
      return buildLengthType(ctx, Schema.Type.VARBINARY);
    }
    if (ctx.BYTES() != null) {
      return Schema.createBytes();
    }
    if (ctx.DATE() != null) {
      return Schema.create(Schema.Type.DATE);
    }
    if (ctx.TIMESTAMP_LTZ() != null) {
      int precision = ctx.intLiteral().isEmpty()
          ? Schema.NO_PARAM
          : parseIntLiteral(ctx.intLiteral(0));
      return Schema.createTimestampLtz(precision);
    }
    if (ctx.TIMESTAMP() != null) {
      int precision = ctx.intLiteral().isEmpty()
          ? Schema.NO_PARAM
          : parseIntLiteral(ctx.intLiteral(0));
      if (ctx.LOCAL() != null) {
        return Schema.createTimestampLtz(precision);
      }
      return Schema.createTimestamp(precision);
    }
    if (ctx.TIME() != null) {
      int precision = ctx.intLiteral().isEmpty()
          ? Schema.NO_PARAM
          : parseIntLiteral(ctx.intLiteral(0));
      return Schema.createTime(precision);
    }
    throw error(ctx, "Unknown primitive type: " + ctx.getText());
  }

  private Schema buildDecimal(LogicalTypesParser.PrimitiveTypeContext ctx) {
    List<LogicalTypesParser.IntLiteralContext> params = ctx.intLiteral();
    int precision = params.size() > 0 ? parseIntLiteral(params.get(0)) : Schema.NO_PARAM;
    int scale = params.size() > 1 ? parseIntLiteral(params.get(1)) : Schema.NO_PARAM;
    return Schema.createDecimal(precision, scale);
  }

  private Schema buildLengthType(LogicalTypesParser.PrimitiveTypeContext ctx,
                                 Schema.Type type) {
    List<LogicalTypesParser.IntLiteralContext> params = ctx.intLiteral();
    int length = params.isEmpty() ? Schema.NO_PARAM : parseIntLiteral(params.get(0));
    switch (type) {
      case VARCHAR:
        return Schema.createVarchar(length);
      case CHAR:
        return Schema.createChar(length);
      case BINARY:
        return Schema.createBinary(length);
      case VARBINARY:
        return Schema.createVarbinary(length);
      default:
        throw error(ctx, "Not a length type: " + type);
    }
  }

  // =========================================================================
  // Complex type builders
  // =========================================================================

  @Override
  public Schema visitRowType(LogicalTypesParser.RowTypeContext ctx) {
    return buildFieldList(ctx.fieldDef());
  }

  private Schema buildStruct(LogicalTypesParser.StructBodyContext ctx) {
    // Two-pass: build all fields first (with column-level CHECK validation),
    // then build table-level CHECKs against the full field set.
    List<Schema.Field> fields = new ArrayList<>();
    int pos = 0;
    for (LogicalTypesParser.StructBodyItemContext item : ctx.structBodyItem()) {
      if (item.fieldDef() != null) {
        fields.add(buildField(item.fieldDef(), pos++));
      }
    }
    List<Rule> tableRules = new ArrayList<>();
    ConstraintValidationContext tableCtx = ConstraintValidationContext.tableLevel(fields)
        .withNamedTypes(namedTypes);
    for (LogicalTypesParser.StructBodyItemContext item : ctx.structBodyItem()) {
      if (item.tableConstraint() != null) {
        tableRules.add(buildRule(item.tableConstraint().checkClause(), tableCtx));
      }
    }
    Schema struct = Schema.createStruct(fields);
    if (!tableRules.isEmpty()) {
      struct.setRules(tableRules);
    }
    return struct;
  }

  private Schema buildFieldList(List<LogicalTypesParser.FieldDefContext> fieldDefs) {
    List<Schema.Field> fields = new ArrayList<>();
    int pos = 0;
    for (LogicalTypesParser.FieldDefContext fctx : fieldDefs) {
      fields.add(buildField(fctx, pos++));
    }
    return Schema.createStruct(fields);
  }

  private Schema.Field buildField(LogicalTypesParser.FieldDefContext ctx, int position) {
    final String name = buildIdentifier(ctx.fieldName().identifier());
    validateUserName(name, ctx.fieldName(), "Field");
    final Schema schema = (Schema) visit(ctx.typeExpr());

    Object defaultValue = null;
    boolean hasDefault = false;
    if (ctx.defaultClause() != null) {
      defaultValue = buildLiteralForType(ctx.defaultClause().literal(), schema);
      hasDefault = true;
    }

    String doc = null;
    if (ctx.commentClause() != null) {
      doc = stripStringLiteral(ctx.commentClause().stringLiteral().getText());
    }

    List<String> tags = null;
    if (ctx.tagsClause() != null) {
      tags = ctx.tagsClause().stringLiteral().stream()
          .map(s -> stripStringLiteral(s.getText()))
          .collect(Collectors.toList());
    }

    Map<String, Object> params = null;
    if (ctx.withClause() != null) {
      params = buildParams(ctx.withClause());
    }

    List<Rule> rules = null;
    if (ctx.columnConstraint() != null && !ctx.columnConstraint().isEmpty()) {
      // Column-level CHECK: only the attached field is in scope.
      ConstraintValidationContext columnCtx = ConstraintValidationContext
          .columnLevel(name, schema).withNamedTypes(namedTypes);
      final List<Rule> rulesList = new ArrayList<>();
      for (LogicalTypesParser.ColumnConstraintContext cc : ctx.columnConstraint()) {
        rulesList.add(buildRule(cc.checkClause(), columnCtx));
      }
      rules = rulesList;
    }

    return new Schema.Field(name, schema, position,
        defaultValue, hasDefault, doc, tags, params, rules);
  }

  /**
   * Build a {@link Rule} from a parsed CHECK clause. Used for both column-level
   * CHECKs (unwrapped from {@code columnConstraint}) and table-level CHECKs
   * (unwrapped from {@code tableConstraint}) — the wrappers exist purely to
   * document placement and both delegate to this single body.
   */
  private Rule buildRule(
      LogicalTypesParser.CheckClauseContext ctx, ConstraintValidationContext vctx) {
    String name = ctx.identifier() != null
        ? buildIdentifier(ctx.identifier()) : null;
    String message = ctx.messageClause() != null
        ? stripStringLiteral(ctx.messageClause().stringLiteral().getText())
        : null;
    String sql = originalSourceText(ctx.check_expr());
    String expr = ConstraintToCelTranslator.translate(ctx.check_expr(), vctx);
    return new Rule(name, message, expr, sql);
  }

  /**
   * Return the original source-text slice for a parser rule context, including
   * the whitespace and casing the user wrote. {@link ParserRuleContext#getText}
   * concatenates only the surviving terminal tokens, and our grammar's
   * {@code WS} rule uses {@code -> skip}, so {@code getText()} would collapse
   * {@code x > 0 AND y < 10} to {@code x>0ANDy<10}. Pulling the slice from
   * the underlying input stream preserves the source verbatim — what
   * {@link Rule#getSql()} promises (lossless round-trip to readable DDL).
   */
  private static String originalSourceText(ParserRuleContext ctx) {
    Token start = ctx.getStart();
    Token stop = ctx.getStop();
    if (start == null || stop == null) {
      return ctx.getText();
    }
    return start.getInputStream().getText(
        org.antlr.v4.runtime.misc.Interval.of(
            start.getStartIndex(), stop.getStopIndex()));
  }

  private Schema buildEnum(LogicalTypesParser.EnumBodyContext ctx) {
    List<Schema.EnumValue> values = new ArrayList<>();
    for (LogicalTypesParser.EnumValueContext vctx : ctx.enumValue()) {
      values.add(buildEnumValue(vctx));
    }
    return Schema.createEnum(values);
  }

  private Schema.EnumValue buildEnumValue(LogicalTypesParser.EnumValueContext ctx) {
    String symbol = stripStringLiteral(ctx.stringLiteral().getText());
    validateUserName(symbol, ctx, "Enum symbol");

    String doc = null;
    if (ctx.commentClause() != null) {
      doc = stripStringLiteral(ctx.commentClause().stringLiteral().getText());
    }

    Map<String, Object> params = null;
    if (ctx.withClause() != null) {
      params = buildParams(ctx.withClause());
    }

    return new Schema.EnumValue(symbol, doc, params);
  }

  @Override
  public Schema visitUnionType(LogicalTypesParser.UnionTypeContext ctx) {
    List<Schema.UnionBranch> branches = new ArrayList<>();
    for (LogicalTypesParser.UnionBranchContext bctx : ctx.unionBranch()) {
      branches.add(buildUnionBranch(bctx));
    }
    return Schema.createUnion(branches);
  }

  private Schema.UnionBranch buildUnionBranch(LogicalTypesParser.UnionBranchContext ctx) {
    String name = buildIdentifier(ctx.identifier());
    validateUserName(name, ctx, "Union branch");
    Schema schema = (Schema) visit(ctx.typeExpr());

    String doc = null;
    if (ctx.commentClause() != null) {
      doc = stripStringLiteral(ctx.commentClause().stringLiteral().getText());
    }

    Map<String, Object> params = null;
    if (ctx.withClause() != null) {
      params = buildParams(ctx.withClause());
    }

    return new Schema.UnionBranch(name, schema, doc, params);
  }

  @Override
  public Schema visitMapType(LogicalTypesParser.MapTypeContext ctx) {
    Schema keyType = (Schema) visit(ctx.typeExpr(0));
    Schema valueType = (Schema) visit(ctx.typeExpr(1));
    return Schema.createMap(keyType, valueType);
  }

  // =========================================================================
  // Utility methods
  // =========================================================================

  private String buildQualifiedName(LogicalTypesParser.QualifiedNameContext ctx) {
    return ctx.identifier().stream()
        .map(this::buildIdentifier)
        .collect(Collectors.joining("."));
  }

  /**
   * Applies the active namespace to a name written at a definition site
   * (a {@code STRUCT} or {@code ENUM} declaration).
   *
   * <p>Rule:
   * <ul>
   *   <li>No dots in {@code name} → unqualified, always prepend the namespace.
   *   <li>Has dots, and a dotted prefix of {@code name} matches a previously-
   *       declared local type (either as-written or with the namespace
   *       prepended) → the dots denote nesting under that local type, so
   *       prepend the namespace.
   *   <li>Has dots and no matching local-type prefix → the dots denote a
   *       different namespace; leave the name as-written.
   * </ul>
   *
   * <p>Because lookups consult {@link #namedTypes}, declarations must be in
   * order: a parent type must be declared before any nested child.
   */
  private String qualifyWithNamespace(String name) {
    if (namespace == null || namespace.isEmpty()) {
      return name;
    }
    if (!name.contains(".") || hasLocalParentPrefix(name)) {
      // Don't double-prepend if the user wrote the fully-qualified path.
      if (name.startsWith(namespace + ".")) {
        return name;
      }
      return namespace + "." + name;
    }
    return name;
  }

  /**
   * Applies the active namespace to a name used as a type reference (field
   * type, root schema reference). Bare names are always namespace-prefixed
   * (matching Avro's rule); externals must be written fully qualified.
   *
   * <p>Dotted names pass through unless a dotted prefix matches a locally
   * declared parent type — in which case the name is treated as a nested-type
   * reference and inherits the active namespace.
   */
  private String qualifyTypeRef(String name) {
    if (namespace == null || namespace.isEmpty()) {
      return name;
    }
    if (!name.contains(".")) {
      return namespace + "." + name;
    }
    if (hasLocalParentPrefix(name)) {
      if (name.startsWith(namespace + ".")) {
        return name;
      }
      return namespace + "." + name;
    }
    return name;
  }

  /**
   * Does a dotted prefix of {@code name} match a locally-declared type — either
   * stored verbatim, or under the active namespace? Longest matching prefix
   * decides; we only need to know whether <em>any</em> prefix matches.
   */
  private boolean hasLocalParentPrefix(String name) {
    int dot = name.lastIndexOf('.');
    while (dot > 0) {
      String prefix = name.substring(0, dot);
      if (namedTypes.containsKey(prefix)) {
        return true;
      }
      if (namespace != null && !namespace.isEmpty()
          && namedTypes.containsKey(namespace + "." + prefix)) {
        return true;
      }
      dot = name.lastIndexOf('.', dot - 1);
    }
    return false;
  }

  private String buildIdentifier(LogicalTypesParser.IdentifierContext ctx) {
    if (ctx.QUOTED_ID() != null) {
      String text = ctx.QUOTED_ID().getText();
      // strip backticks and unescape doubled backticks
      return text.substring(1, text.length() - 1).replace("``", "`");
    }
    if (ctx.ID() != null) {
      return ctx.ID().getText();
    }
    // nonReservedKeyword
    return ctx.nonReservedKeyword().getText();
  }

  /**
   * Reject user-supplied identifiers that would produce unparseable wire-form
   * or DDL output. Specifically: empty (already caught elsewhere defensively)
   * and whitespace-or-control-char-bearing (QUOTED_ID lexer accepts these
   * but the result is unusable downstream).
   *
   * <p>CEL reserved words (true/false/null/in/as) are intentionally allowed
   * here so DDL ingestion remains symmetric with the programmatic API
   * (round 11 relaxed {@link Schema#validateUserName}). The CHECK translator
   * escapes such names via index syntax ({@code this["null"]}), and the DDL
   * emitter quotes them with backticks.
   *
   * @param name the unquoted identifier text
   * @param ctx parser context for source-position attribution
   * @param role short label for the error message (e.g. "Field", "Enum symbol")
   */
  private void validateUserName(String name,
                                org.antlr.v4.runtime.ParserRuleContext ctx,
                                String role) {
    if (name == null || name.isEmpty()) {
      throw error(ctx, role + " name must not be empty");
    }
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isWhitespace(c) || Character.isISOControl(c)) {
        throw error(ctx, role + " name must not contain whitespace or "
            + "control characters; got: '" + name + "'");
      }
    }
  }

  private Map<String, Object> buildParams(LogicalTypesParser.WithClauseContext ctx) {
    Map<String, Object> props = new LinkedHashMap<>();
    // Grammar: withProperty = stringLiteral '=' stringLiteral
    // Both key and value are quoted string literals; strip the quotes.
    for (LogicalTypesParser.WithPropertyContext pctx : ctx.withProperty()) {
      String key = stripStringLiteral(pctx.stringLiteral(0).getText());
      String value = stripStringLiteral(pctx.stringLiteral(1).getText());
      props.put(key, value);
    }
    return props;
  }

  private Object buildLiteral(LogicalTypesParser.LiteralContext ctx) {
    if (ctx.NULL() != null) {
      return null;
    }
    if (ctx.intLiteral() != null) {
      return Integer.parseInt(ctx.intLiteral().getText());
    }
    if (ctx.decimalLiteral() != null) {
      return Double.parseDouble(ctx.decimalLiteral().getText());
    }
    if (ctx.doubleLiteral() != null) {
      return Double.parseDouble(ctx.doubleLiteral().getText());
    }
    if (ctx.stringLiteral() != null) {
      return stripStringLiteral(ctx.stringLiteral().getText());
    }
    if (ctx.bytesLiteral() != null) {
      return parseBytesLiteral(ctx.bytesLiteral());
    }
    if (ctx.boolLiteral() != null) {
      return ctx.boolLiteral().TRUE() != null;
    }
    throw error(ctx, "Unknown literal: " + ctx.getText());
  }

  /**
   * Coerces a parsed literal to the Java type expected by the field's
   * LogicalType. Throws ValidationException if the literal kind is incompatible
   * with the target type or if a numeric value is out of range.
   */
  private Object buildLiteralForType(
      LogicalTypesParser.LiteralContext ctx, Schema targetType) {
    if (ctx.NULL() != null) {
      return null;
    }
    final Schema.Type type = targetType.getType();
    switch (type) {
      case BOOLEAN:
        if (ctx.boolLiteral() == null) {
          throw typeMismatch(type, ctx);
        }
        return ctx.boolLiteral().TRUE() != null;
      case TINYINT: {
        long n = requireInt(ctx, type);
        if (n < Byte.MIN_VALUE || n > Byte.MAX_VALUE) {
          throw error(ctx, "TINYINT default value out of range: " + n);
        }
        return (byte) n;
      }
      case SMALLINT: {
        long n = requireInt(ctx, type);
        if (n < Short.MIN_VALUE || n > Short.MAX_VALUE) {
          throw error(ctx, "SMALLINT default value out of range: " + n);
        }
        return (short) n;
      }
      case INT: {
        long n = requireInt(ctx, type);
        if (n < Integer.MIN_VALUE || n > Integer.MAX_VALUE) {
          throw error(ctx, "INT default value out of range: " + n);
        }
        return (int) n;
      }
      case BIGINT:
        return requireInt(ctx, type);
      case FLOAT:
        return (float) requireNumeric(ctx, type);
      case DOUBLE:
        return requireNumeric(ctx, type);
      case CHAR:
        return charDefault(ctx, targetType);
      case VARCHAR:
        return varcharDefault(ctx, targetType);
      case BINARY:
        return binaryDefault(ctx, targetType);
      case VARBINARY:
        return varbinaryDefault(ctx, targetType);
      case DECIMAL: {
        java.math.BigDecimal value;
        if (ctx.intLiteral() != null || ctx.decimalLiteral() != null
            || ctx.doubleLiteral() != null) {
          value = new java.math.BigDecimal(ctx.getText());
        } else if (ctx.stringLiteral() != null) {
          value = new java.math.BigDecimal(
              stripStringLiteral(ctx.stringLiteral().getText()));
        } else {
          throw typeMismatch(type, ctx);
        }
        int p = targetType.getPrecision();
        int s = targetType.getScale();
        // Bare `DECIMAL` (no explicit precision/scale) gets DEFAULT_DECIMAL_PRECISION/
        // DEFAULT_DECIMAL_SCALE — treat that as "unbounded" for default-value
        // validation. Users who wrote `DECIMAL(10, 0)` explicitly get the
        // strict check; users who wrote bare `DECIMAL` get lenient behavior.
        boolean explicit = !(p == Schema.DEFAULT_DECIMAL_PRECISION
            && s == Schema.DEFAULT_DECIMAL_SCALE);
        if (explicit && p != Schema.NO_PARAM) {
          if (s != Schema.NO_PARAM && value.scale() > s) {
            throw error(ctx, "DECIMAL(" + p + ", " + s + ") default value '"
                + value.toPlainString() + "' has scale " + value.scale()
                + " (more digits after the decimal point than declared scale)");
          }
          int valuePrecision = value.precision();
          int valueScale = Math.max(value.scale(), 0);
          int intDigits = valuePrecision - valueScale;
          int allowedIntDigits =
              p - (s == Schema.NO_PARAM ? Schema.DEFAULT_DECIMAL_SCALE : s);
          if (intDigits > allowedIntDigits) {
            throw error(ctx, "DECIMAL(" + p + (s == Schema.NO_PARAM ? "" : ", " + s)
                + ") default value '" + value.toPlainString()
                + "' exceeds declared precision (integer-part digits "
                + intDigits + " > " + allowedIntDigits + ")");
          }
        }
        return value;
      }
      case DATE:
        if (ctx.stringLiteral() == null) {
          throw typeMismatch(type, ctx);
        }
        return java.time.LocalDate.parse(
            stripStringLiteral(ctx.stringLiteral().getText()));
      case TIME: {
        if (ctx.stringLiteral() == null) {
          throw typeMismatch(type, ctx);
        }
        String s = stripStringLiteral(ctx.stringLiteral().getText());
        rejectExcessFractionalDigits(ctx, s, targetType.getPrecision(), "TIME");
        return java.time.LocalTime.parse(s);
      }
      case TIMESTAMP: {
        if (ctx.stringLiteral() == null) {
          throw typeMismatch(type, ctx);
        }
        String s = stripStringLiteral(ctx.stringLiteral().getText());
        rejectExcessFractionalDigits(ctx, s, targetType.getPrecision(), "TIMESTAMP");
        return java.time.LocalDateTime.parse(s);
      }
      case TIMESTAMP_LTZ: {
        if (ctx.stringLiteral() == null) {
          throw typeMismatch(type, ctx);
        }
        String s = stripStringLiteral(ctx.stringLiteral().getText());
        rejectExcessFractionalDigits(
            ctx, s, targetType.getPrecision(), "TIMESTAMP_LTZ");
        return java.time.Instant.parse(s);
      }
      case VARIANT:
      case STRUCT:
      case ENUM:
      case UNION:
      case ARRAY:
      case MAP:
      case MULTISET:
      case NAMED_TYPE_REF:
      default:
        throw error(ctx, "Default values are not supported for type: " + type);
    }
  }

  /**
   * Reject TIME/TIMESTAMP/TIMESTAMP_LTZ default literals whose fractional-second
   * portion has more digits than the type's declared precision. Counts the
   * digits after the last {@code .}, ignoring trailing offsets like
   * {@code +00:00}/{@code Z}/{@code [Zone]}. {@code precision == NO_PARAM}
   * skips the check (caller didn't constrain).
   */
  private String charDefault(
      LogicalTypesParser.LiteralContext ctx, Schema targetType) {
    if (ctx.stringLiteral() == null) {
      throw typeMismatch(Schema.Type.CHAR, ctx);
    }
    String s = stripStringLiteral(ctx.stringLiteral().getText());
    int len = targetType.getLength();
    if (len != Schema.NO_PARAM && len != Schema.MAX_LENGTH
        && s.length() != len) {
      throw error(ctx, "CHAR(" + len + ") default value '" + s
          + "' has " + s.length() + " characters; must be exactly " + len
          + " (CHAR is fixed-length — use VARCHAR for variable length).");
    }
    return s;
  }

  private String varcharDefault(
      LogicalTypesParser.LiteralContext ctx, Schema targetType) {
    if (ctx.stringLiteral() == null) {
      throw typeMismatch(Schema.Type.VARCHAR, ctx);
    }
    String s = stripStringLiteral(ctx.stringLiteral().getText());
    int max = targetType.getLength();
    if (max != Schema.NO_PARAM && max != Schema.MAX_LENGTH
        && s.length() > max) {
      throw error(ctx, "VARCHAR(" + max + ") default value '" + s
          + "' exceeds the declared length (" + s.length() + " > " + max + ")");
    }
    return s;
  }

  private byte[] binaryDefault(
      LogicalTypesParser.LiteralContext ctx, Schema targetType) {
    if (ctx.bytesLiteral() == null) {
      throw typeMismatch(Schema.Type.BINARY, ctx);
    }
    byte[] bytes = parseBytesLiteral(ctx.bytesLiteral());
    int len = targetType.getLength();
    if (len != Schema.NO_PARAM && len != Schema.MAX_LENGTH
        && bytes.length != len) {
      throw error(ctx, "BINARY(" + len + ") default value has "
          + bytes.length + " bytes; must be exactly " + len
          + " (BINARY is fixed-length — use VARBINARY for variable length).");
    }
    return bytes;
  }

  private byte[] varbinaryDefault(
      LogicalTypesParser.LiteralContext ctx, Schema targetType) {
    if (ctx.bytesLiteral() == null) {
      throw typeMismatch(Schema.Type.VARBINARY, ctx);
    }
    byte[] bytes = parseBytesLiteral(ctx.bytesLiteral());
    int max = targetType.getLength();
    if (max != Schema.NO_PARAM && max != Schema.MAX_LENGTH
        && bytes.length > max) {
      throw error(ctx, "VARBINARY(" + max + ") default value of "
          + bytes.length + " bytes exceeds the declared length (max "
          + max + ")");
    }
    return bytes;
  }

  private void rejectExcessFractionalDigits(
      LogicalTypesParser.LiteralContext ctx, String literal,
      int precision, String typeName) {
    if (precision == Schema.NO_PARAM) {
      return;
    }
    int dot = literal.lastIndexOf('.');
    if (dot < 0) {
      return;  // no fractional part
    }
    int end = literal.length();
    for (int i = dot + 1; i < literal.length(); i++) {
      char c = literal.charAt(i);
      if (c < '0' || c > '9') {
        end = i;
        break;
      }
    }
    int fracDigits = end - dot - 1;
    if (fracDigits > precision) {
      throw error(ctx, typeName + "(" + precision + ") default value '"
          + literal + "' has " + fracDigits + " fractional-second digits, "
          + "exceeds the declared precision (" + precision + ").");
    }
  }

  private long requireInt(LogicalTypesParser.LiteralContext ctx, Schema.Type type) {
    if (ctx.intLiteral() == null) {
      throw typeMismatch(type, ctx);
    }
    try {
      return Long.parseLong(ctx.intLiteral().getText());
    } catch (NumberFormatException e) {
      throw error(ctx,
          type + " default value out of range: " + ctx.intLiteral().getText(), e);
    }
  }

  private double requireNumeric(LogicalTypesParser.LiteralContext ctx, Schema.Type type) {
    if (ctx.intLiteral() != null) {
      return Double.parseDouble(ctx.intLiteral().getText());
    }
    if (ctx.decimalLiteral() != null) {
      return Double.parseDouble(ctx.decimalLiteral().getText());
    }
    if (ctx.doubleLiteral() != null) {
      return Double.parseDouble(ctx.doubleLiteral().getText());
    }
    throw typeMismatch(type, ctx);
  }

  private static ValidationException typeMismatch(
      Schema.Type type, LogicalTypesParser.LiteralContext ctx) {
    return new ValidationException(
        "Default literal " + ctx.getText() + " is not compatible with type " + type);
  }

  /**
   * Parses an ANSI SQL bytes literal into a byte array. The token text
   * looks like {@code X'48656C6C6F'} or {@code x'48656c6c6f'}: case-insensitive
   * X prefix, single-quoted hex digits. The body must have an even number of
   * hex chars (each pair = one byte).
   */
  private static byte[] parseBytesLiteral(LogicalTypesParser.BytesLiteralContext ctx) {
    String text = ctx.getText();
    // Strip the X' / x' prefix and the trailing '
    String body = text.substring(2, text.length() - 1);
    // SQL '' escape — rare in bytes literals but allowed by the lexer
    body = body.replace("''", "'");
    if (body.length() % 2 != 0) {
      throw error(ctx,
          "Bytes literal " + text + " has odd number of hex digits; "
              + "must be even (each pair = one byte)");
    }
    byte[] result = new byte[body.length() / 2];
    for (int i = 0; i < result.length; i++) {
      int hi = Character.digit(body.charAt(i * 2), 16);
      int lo = Character.digit(body.charAt(i * 2 + 1), 16);
      if (hi < 0 || lo < 0) {
        throw error(ctx,
            "Invalid hex character at position " + (i * 2)
                + " in bytes literal " + text);
      }
      result[i] = (byte) ((hi << 4) | lo);
    }
    return result;
  }

  private static int parseIntLiteral(LogicalTypesParser.IntLiteralContext ctx) {
    return Integer.parseInt(ctx.getText());
  }

  private static String stripStringLiteral(String text) {
    // remove surrounding single quotes and unescape doubled quotes
    return text.substring(1, text.length() - 1).replace("''", "'");
  }

  // =========================================================================
  // Error construction with source position attribution
  // =========================================================================

  /**
   * Build a {@link LocatedValidationException} that points at the full span of
   * {@code ctx} (start of the first token, end of the last token + token length).
   */
  private static ValidationException error(ParserRuleContext ctx, String message) {
    return error(ctx, message, null);
  }

  private static ValidationException error(
      ParserRuleContext ctx, String message, Throwable cause) {
    Token start = ctx.getStart();
    Token stop = ctx.getStop() != null ? ctx.getStop() : start;
    int endCol = stop.getCharPositionInLine()
        + (stop.getText() != null ? stop.getText().length() : 1);
    return cause == null
        ? new LocatedValidationException(
            message,
            start.getLine(), start.getCharPositionInLine(),
            stop.getLine(), endCol)
        : new LocatedValidationException(
            message,
            start.getLine(), start.getCharPositionInLine(),
            stop.getLine(), endCol, cause);
  }

}
