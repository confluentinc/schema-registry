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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Emits a {@link LogicalType} as DDL text — the inverse of
 * {@link LogicalTypesSchemaVisitor}. Output is canonical (one statement per
 * line; struct bodies multi-line when more than two fields). Round-trips
 * through the visitor produce an equivalent {@code LogicalType}.
 *
 * <p><b>Lossy points</b> (intentional, matching visitor capability):
 * <ul>
 *   <li>External references have no syntactic marker — they're inferred on
 *       read-back from usage. {@code DECLARE} statements survive (carrying the
 *       wire-format URI binding) but bare externals (no URI) are implicit,
 *       identified by appearing in a type position without a matching local
 *       {@code STRUCT}/{@code ENUM}.</li>
 *   <li>Path-keyed {@code defaultValues} aren't re-emitted; field-level
 *       defaults survive via {@link Schema.Field#getDefaultValue()}.</li>
 *   <li>The single-root sugar is detected and the trailing root-registration
 *       {@code TYPE} statement is elided when it would be redundant. The
 *       multi-root UNION sugar is detected similarly.</li>
 * </ul>
 */
public final class LogicalTypeToDdlConverter {

  // Identifier-quoting rules (reserved keyword set, lexer rules) live in
  // Schema.quoteIdentifierIfNeeded so this emitter and Schema.toDdl share
  // a single source of truth. Removed the local RESERVED_KEYWORDS list.

  private LogicalTypeToDdlConverter() {
  }

  public static String toDdl(LogicalType logicalType) {
    return new Printer(logicalType).script();
  }

  private static final class Printer {
    private final LogicalType lt;
    private final Set<String> localNames;
    // Not final: rowExpr/unionExpr swap in a fresh local builder so they
    // can reuse appendField/appendBranch (which write to `sb`) without
    // their output landing in the outer accumulator. Restored in a
    // try/finally.
    private StringBuilder sb = new StringBuilder();

    Printer(LogicalType lt) {
      this.lt = lt;
      this.localNames = lt.getNamedTypes().keySet();
    }

    String script() {
      if (lt.getNamespace() != null) {
        sb.append("NAMESPACE ")
            .append(qualifiedName(lt.getNamespace()))
            .append(";\n");
      }

      // DECLARE declarations: external-ness is inferred on read-back from
      // usage, so bare externals don't need a syntactic marker. Only the
      // URI bindings (synthetic-wrapper $ref / import targets) need to be
      // emitted so they survive DDL → LT → DDL round-trip. Restrict to FQNs
      // that local code actually references (intersect with externalRefs)
      // to drop any stale entries.
      Set<String> externalRefs = new LinkedHashSet<>(collectExternalRefs());
      for (Map.Entry<String, String> e : lt.getExternalImports().entrySet()) {
        if (!externalRefs.contains(e.getKey())) {
          continue;
        }
        sb.append("DECLARE ").append(qualifiedName(e.getKey()))
            .append(" FOR ").append(stringLiteral(e.getValue()))
            .append(";\n");
      }

      // Named-type declarations (`STRUCT <name> (...)` / `ENUM <name> (...)`) in
      // declaration order, LOCAL types only. Externals' bodies are also in
      // namedTypes (lazy-promoted by the reader from resolvedReferences),
      // but they're external by inference on read-back — must NOT be
      // re-emitted here.
      for (String name : orderedLocalNames()) {
        if (lt.getExternalTypes().contains(name)) {
          continue;
        }
        printCreateType(name, lt.getNamedTypes().get(name));
      }

      // Trailing root-registration `TYPE <typeExpr>`: omit when the visitor's
      // auto-detect would produce the same root from the local types alone.
      if (!sugarWouldInferRoot()) {
        sb.append("TYPE ").append(typeExpr(lt.getRootSchema())).append(";\n");
      }

      return sb.toString();
    }

    // ---------------------------------------------------------------------
    // Named-type declarations
    // ---------------------------------------------------------------------

    private void printCreateType(String name, Schema body) {
      switch (body.getType()) {
        case STRUCT:
          sb.append("STRUCT ").append(qualifiedName(name)).append(" ");
          appendStructBody(body, /*indent=*/"");
          break;
        case ENUM:
          sb.append("ENUM ").append(qualifiedName(name)).append(" ");
          appendEnumBody(body);
          break;
        default:
          throw new ValidationException(
              "Named-type body must be STRUCT or ENUM, got " + body.getType()
                  + " for '" + name + "'");
      }
      appendCommentTagsParams(body);
      sb.append(";\n");
    }

    private void appendStructBody(Schema struct, String indent) {
      List<Schema.Field> fields = struct.getFields();
      List<Rule> tableRules = struct.getRules();
      // Use the multi-line shape if there are any table-level CHECKs, regardless
      // of field count — table CHECKs read clearly on their own line.
      boolean multiline = fields.size() > 2 || !tableRules.isEmpty();
      int totalItems = fields.size() + tableRules.size();
      if (!multiline) {
        sb.append("(");
        for (int i = 0; i < fields.size(); i++) {
          if (i > 0) {
            sb.append(", ");
          }
          appendField(fields.get(i));
        }
        sb.append(")");
      } else {
        sb.append("(\n");
        String childIndent = indent + "  ";
        int written = 0;
        for (Schema.Field field : fields) {
          sb.append(childIndent);
          appendField(field);
          written++;
          if (written < totalItems) {
            sb.append(",");
          }
          sb.append("\n");
        }
        for (Rule rule : tableRules) {
          sb.append(childIndent);
          appendCheckRule(rule);
          written++;
          if (written < totalItems) {
            sb.append(",");
          }
          sb.append("\n");
        }
        sb.append(indent).append(")");
      }
    }

    private void appendEnumBody(Schema enumSchema) {
      List<Schema.EnumValue> values = enumSchema.getEnumValues();
      sb.append("(");
      for (int i = 0; i < values.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(stringLiteral(values.get(i).getSymbol()));
        if (values.get(i).getDoc() != null) {
          sb.append(" COMMENT ").append(stringLiteral(values.get(i).getDoc()));
        }
        appendWithParams(values.get(i).getParams());
      }
      sb.append(")");
    }

    // ---------------------------------------------------------------------
    // Field / branch
    // ---------------------------------------------------------------------

    private void appendField(Schema.Field field) {
      sb.append(identifier(field.getName())).append(" ")
          .append(typeExpr(field.getSchema()));
      if (field.hasDefaultValue()) {
        sb.append(" DEFAULT ").append(literal(field.getDefaultValue(), field.getSchema()));
      }
      // Per grammar order: defaultClause -> checkClause* -> commentClause
      // -> tagsClause -> withClause.
      for (Rule rule : field.getRules()) {
        sb.append(' ');
        appendCheckRule(rule);
      }
      if (field.getDoc() != null) {
        sb.append(" COMMENT ").append(stringLiteral(field.getDoc()));
      }
      if (!field.getTags().isEmpty()) {
        sb.append(" TAGS(");
        for (int i = 0; i < field.getTags().size(); i++) {
          if (i > 0) {
            sb.append(", ");
          }
          sb.append(stringLiteral(field.getTags().get(i)));
        }
        sb.append(")");
      }
      appendWithParams(field.getParams());
    }

    /**
     * Emit one CHECK clause: {@code [CONSTRAINT name] CHECK (sql) [MESSAGE 'doc']}.
     * Uses {@link Rule#getSql()} verbatim for lossless DDL round-trip.
     */
    private void appendCheckRule(Rule rule) {
      if (rule.getName() != null) {
        sb.append("CONSTRAINT ").append(identifier(rule.getName())).append(' ');
      }
      sb.append("CHECK (").append(rule.getSql()).append(')');
      if (rule.getDoc() != null) {
        sb.append(" MESSAGE ").append(stringLiteral(rule.getDoc()));
      }
    }

    private void appendBranch(Schema.UnionBranch branch) {
      sb.append(identifier(branch.getName())).append(" ")
          .append(typeExpr(branch.getSchema()));
      if (branch.getDoc() != null) {
        sb.append(" COMMENT ").append(stringLiteral(branch.getDoc()));
      }
      appendWithParams(branch.getParams());
    }

    // ---------------------------------------------------------------------
    // Schema-level metadata (doc, tags, params on a named-type body)
    // ---------------------------------------------------------------------

    private void appendCommentTagsParams(Schema schema) {
      if (schema.getDoc() != null) {
        sb.append(" COMMENT ").append(stringLiteral(schema.getDoc()));
      }
      if (!schema.getTags().isEmpty()) {
        sb.append(" TAGS(");
        for (int i = 0; i < schema.getTags().size(); i++) {
          if (i > 0) {
            sb.append(", ");
          }
          sb.append(stringLiteral(schema.getTags().get(i)));
        }
        sb.append(")");
      }
      appendWithParams(schema.getParams());
    }

    private void appendWithParams(Map<String, Object> params) {
      if (params == null || params.isEmpty()) {
        return;
      }
      sb.append(" WITH(");
      boolean first = true;
      for (Map.Entry<String, Object> entry : params.entrySet()) {
        if (!first) {
          sb.append(", ");
        }
        first = false;
        sb.append(stringLiteral(entry.getKey())).append(" = ")
            .append(stringLiteral(String.valueOf(entry.getValue())));
      }
      sb.append(")");
    }

    // ---------------------------------------------------------------------
    // Type expressions
    // ---------------------------------------------------------------------

    /**
     * DDL form of {@code schema} as a type expression suitable for use in a
     * field, branch, or trailing root-registration position. STRUCTs render
     * as {@code STRUCT(...)}; everything else delegates to {@link Schema#toDdl}
     * and adds richer metadata-aware nesting only where needed.
     */
    private String typeExpr(Schema schema) {
      switch (schema.getType()) {
        case STRUCT:
          return rowExpr(schema);
        case UNION:
          return unionExpr(schema);
        case ARRAY:
          return wrapNullable("ARRAY<" + typeExpr(schema.getElementType()) + ">", schema);
        case MULTISET:
          return wrapNullable("MULTISET<" + typeExpr(schema.getElementType()) + ">", schema);
        case MAP:
          return wrapNullable(
              "MAP<" + typeExpr(schema.getKeyType()) + ", "
                  + typeExpr(schema.getValueType()) + ">", schema);
        case NAMED_TYPE_REF:
          // Schema.toDdl returns the bare qualified name without identifier
          // quoting — collides with reserved words (e.g., a type literally
          // named "Row" would lex as the STRUCT keyword). Quote per-segment.
          return wrapNullable(qualifiedName(schema.getQualifiedName()), schema);
        default:
          // Primitive / decimal / parametric / ENUM — Schema.toDdl already
          // emits the right syntax including the " NOT NULL" suffix when
          // appropriate.
          return schema.toDdl();
      }
    }

    private String rowExpr(Schema struct) {
      // Reuse appendField so inline STRUCT(...) emission preserves all
      // per-field metadata (defaults, CHECK rules, doc, tags, params) —
      // matching what the grammar's `rowType` accepts via `fieldDef`.
      // Without this, e.g. `addr STRUCT(zip INT CHECK (zip > 0))` would
      // round-trip to `addr STRUCT(zip INT)`, silently dropping the rule.
      StringBuilder local = new StringBuilder("STRUCT(");
      StringBuilder outer = sb;
      sb = local;
      try {
        List<Schema.Field> fields = struct.getFields();
        for (int i = 0; i < fields.size(); i++) {
          if (i > 0) {
            local.append(", ");
          }
          appendField(fields.get(i));
        }
      } finally {
        sb = outer;
      }
      local.append(")");
      return wrapNullable(local.toString(), struct);
    }

    private String unionExpr(Schema union) {
      // Same swap-and-restore pattern as rowExpr so appendBranch can
      // emit branch-level COMMENT/WITH metadata into the inline UNION(...)
      // form.
      StringBuilder local = new StringBuilder("UNION(");
      StringBuilder outer = sb;
      sb = local;
      try {
        List<Schema.UnionBranch> branches = union.getBranches();
        for (int i = 0; i < branches.size(); i++) {
          if (i > 0) {
            local.append(", ");
          }
          appendBranch(branches.get(i));
        }
      } finally {
        sb = outer;
      }
      local.append(")");
      return wrapNullable(local.toString(), union);
    }

    private String wrapNullable(String base, Schema schema) {
      return schema.isNullable() ? base : base + " NOT NULL";
    }

    // ---------------------------------------------------------------------
    // Reference / sugar detection
    // ---------------------------------------------------------------------

    /**
     * Walk the root schema and every LOCAL named-type body, collecting every
     * {@link Schema.Type#NAMED_TYPE_REF} FQN. Intersect with
     * {@link LogicalType#getExternalTypes} to find externals that local code
     * actually references. Returns the FQNs eligible to receive a
     * {@code DECLARE} statement (when a URI binding exists).
     *
     * <p>Walking only LOCAL bodies (not external-promoted ones) avoids
     * surfacing transitive externals in the emitted DDL — only the externals
     * the user's local code directly references.
     */
    private List<String> collectExternalRefs() {
      Set<String> all = new LinkedHashSet<>();
      LogicalType.collectNamedRefs(lt.getRootSchema(), all);
      for (Map.Entry<String, Schema> e : lt.getNamedTypes().entrySet()) {
        if (lt.getExternalTypes().contains(e.getKey())) {
          continue;
        }
        LogicalType.collectNamedRefs(e.getValue(), all);
      }
      all.retainAll(lt.getExternalTypes());
      return new ArrayList<>(all);
    }

    /**
     * True iff {@link LogicalTypesSchemaVisitor}'s "infer root from named-type
     * declarations" sugar would, given just the local named types, produce
     * the same {@code rootSchema} we have. When true, the trailing
     * root-registration {@code TYPE} statement is elided.
     */
    private boolean sugarWouldInferRoot() {
      if (lt.getNamedTypes().isEmpty()) {
        return false;
      }
      // The visitor's sugar reasons over LOCAL types only — externals are
      // inferred from usage and don't participate in root inference.
      Set<String> defined = new LinkedHashSet<>(lt.getNamedTypes().keySet());
      defined.removeAll(lt.getExternalTypes());
      if (defined.isEmpty()) {
        return false;
      }
      Map<String, Set<String>> uses = new LinkedHashMap<>();
      for (Map.Entry<String, Schema> e : lt.getNamedTypes().entrySet()) {
        if (lt.getExternalTypes().contains(e.getKey())) {
          continue;
        }
        Set<String> refs = new LinkedHashSet<>();
        LogicalType.collectNamedRefs(e.getValue(), refs);
        refs.retainAll(defined);
        refs.remove(e.getKey()); // self-recursive types still count as roots
        uses.put(e.getKey(), refs);
      }
      Set<String> referenced = new LinkedHashSet<>();
      for (Set<String> r : uses.values()) {
        referenced.addAll(r);
      }
      Set<String> roots = new LinkedHashSet<>(defined);
      roots.removeAll(referenced);
      // Nested types are never roots.
      roots.removeIf(name -> LogicalType.parentOf(name, defined) != null);

      Schema root = lt.getRootSchema();
      if (roots.size() == 1) {
        if (root.getType() != Schema.Type.NAMED_TYPE_REF || root.isNullable()) {
          return false;
        }
        return roots.iterator().next().equals(root.getQualifiedName());
      }
      if (roots.isEmpty()) {
        // Cycle in local types — visitor can't infer; explicit trailing TYPE required.
        return false;
      }
      // Multi-root sugar: UNION of NAMED_TYPE_REFs (NOT NULL) to all roots,
      // each branch named with the simple name.
      if (root.getType() != Schema.Type.UNION || root.isNullable()) {
        return false;
      }
      List<Schema.UnionBranch> branches = root.getBranches();
      if (branches.size() != roots.size()) {
        return false;
      }
      int i = 0;
      for (String fqn : roots) {
        Schema.UnionBranch b = branches.get(i++);
        if (!b.getName().equals(simpleNameOf(fqn))) {
          return false;
        }
        Schema bs = b.getSchema();
        if (bs.getType() != Schema.Type.NAMED_TYPE_REF) {
          return false;
        }
        if (bs.isNullable() || !fqn.equals(bs.getQualifiedName())) {
          return false;
        }
      }
      return true;
    }

    private static String simpleNameOf(String qualifiedName) {
      int dot = qualifiedName.lastIndexOf('.');
      return dot < 0 ? qualifiedName : qualifiedName.substring(dot + 1);
    }

    // ---------------------------------------------------------------------
    // Ordering local named types (parents first, then children)
    // ---------------------------------------------------------------------

    /**
     * Local named types in an order that respects the dotted-nesting
     * convention: a parent (e.g., {@code Outer}) precedes any of its
     * children ({@code Outer.Inner}). Within the same nesting level,
     * insertion order from {@link LogicalType#getNamedTypes()} is preserved.
     */
    private List<String> orderedLocalNames() {
      // Stable sort by depth (number of nesting levels in the local-name
      // tree) so parents come first. Depth is computed via parentOf chain
      // length; a name with no local parent has depth 0.
      List<String> names = new ArrayList<>(lt.getNamedTypes().keySet());
      Map<String, Integer> depth = new LinkedHashMap<>();
      for (String name : names) {
        int d = 0;
        String cur = name;
        String p;
        while ((p = LogicalType.parentOf(cur, lt.getNamedTypes().keySet())) != null) {
          d++;
          cur = p;
        }
        depth.put(name, d);
      }
      names.sort((a, b) -> Integer.compare(depth.get(a), depth.get(b)));
      return names;
    }

    // ---------------------------------------------------------------------
    // Names and literals
    // ---------------------------------------------------------------------

    private static String qualifiedName(String dotted) {
      String[] parts = dotted.split("\\.");
      StringBuilder b = new StringBuilder();
      for (int i = 0; i < parts.length; i++) {
        if (i > 0) {
          b.append(".");
        }
        b.append(identifier(parts[i]));
      }
      return b.toString();
    }

    private static String identifier(String name) {
      // Delegates to Schema.quoteIdentifierIfNeeded so this emitter and
      // Schema.toDdl() share a single source of truth for the lexer keyword
      // list and quoting rules.
      return Schema.quoteIdentifierIfNeeded(name);
    }

    private static String stringLiteral(String s) {
      return "'" + s.replace("'", "''") + "'";
    }

    private static String literal(Object value, Schema schema) {
      if (value == null) {
        return "NULL";
      }
      switch (schema.getType()) {
        case BOOLEAN:
          return Boolean.TRUE.equals(value) ? "TRUE" : "FALSE";
        case TINYINT:
        case SMALLINT:
        case INT:
        case BIGINT:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case TIMESTAMP_LTZ:
          return value.toString();
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
          return value.toString();
        case VARCHAR:
        case CHAR:
          return stringLiteral(value.toString());
        case BINARY:
        case VARBINARY:
          return bytesLiteral(value);
        default:
          // Defaults on composite types or NAMED_TYPE_REF aren't supported by
          // the grammar. Fall back to a quoted string so something is emitted;
          // this won't round-trip cleanly but is better than a silent drop.
          return stringLiteral(String.valueOf(value));
      }
    }

    private static String bytesLiteral(Object value) {
      byte[] bytes;
      if (value instanceof byte[]) {
        bytes = (byte[]) value;
      } else if (value instanceof String) {
        bytes = ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
      } else {
        bytes = value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      }
      StringBuilder hex = new StringBuilder("x'");
      for (byte b : bytes) {
        hex.append(String.format("%02X", b));
      }
      hex.append("'");
      return hex.toString();
    }
  }
}
