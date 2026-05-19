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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class Schema {

  public static final int NO_PARAM = -1;

  public static final int MAX_LENGTH = Integer.MAX_VALUE;
  public static final int DEFAULT_LENGTH = 1;
  public static final int DEFAULT_DECIMAL_PRECISION = 10;
  public static final int DEFAULT_DECIMAL_SCALE = 0;
  public static final int DEFAULT_TIME_PRECISION = 0;
  public static final int DEFAULT_TIMESTAMP_PRECISION = 6;

  public enum Type {
    STRUCT, ENUM, UNION, MAP, ARRAY, MULTISET,
    BOOLEAN,
    TINYINT, SMALLINT, INT, BIGINT,
    FLOAT, DOUBLE,
    DECIMAL,
    VARCHAR, CHAR,
    BINARY, VARBINARY,
    DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ,
    VARIANT,
    NAMED_TYPE_REF
  }

  private final Type type;
  private boolean nullable = true;
  private String doc;
  private List<String> tags = Collections.emptyList();
  private Map<String, Object> params = Collections.emptyMap();
  private List<Rule> rules = Collections.emptyList();

  Schema(Type type) {
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public boolean isNullable() {
    return nullable;
  }

  public Schema setNullable(boolean nullable) {
    this.nullable = nullable;
    return this;
  }

  public String getDoc() {
    return doc;
  }

  public Schema setDoc(String doc) {
    this.doc = doc;
    return this;
  }

  public List<String> getTags() {
    return tags;
  }

  public Schema setTags(List<String> tags) {
    this.tags = tags != null
        ? Collections.unmodifiableList(new ArrayList<>(tags))
        : Collections.emptyList();
    return this;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public Schema setParams(Map<String, Object> params) {
    this.params = params != null
        ? Collections.unmodifiableMap(new LinkedHashMap<>(params))
        : Collections.emptyMap();
    return this;
  }

  /**
   * Validation rules attached to this schema. For struct types this carries
   * table-level CHECK constraints (constraints that may reference any field).
   * Empty for non-struct schemas in the typical case.
   */
  public List<Rule> getRules() {
    return rules;
  }

  public Schema setRules(List<Rule> rules) {
    if (rules != null && !rules.isEmpty() && type != Type.STRUCT) {
      throw new ValidationException(
          "Rules are only supported on STRUCT (table-level CHECK); "
              + "for column-level CHECK attach the rule to Schema.Field. "
              + "Got non-empty rules on " + type);
    }
    this.rules = rules != null
        ? Collections.unmodifiableList(new ArrayList<>(rules))
        : Collections.emptyList();
    return this;
  }

  // --- complex type accessors ---

  public List<Field> getFields() {
    throw new UnsupportedOperationException("Not a struct: " + this);
  }

  public Field getField(String name) {
    throw new UnsupportedOperationException("Not a struct: " + this);
  }

  public List<EnumValue> getEnumValues() {
    throw new UnsupportedOperationException("Not an enum: " + this);
  }

  public List<UnionBranch> getBranches() {
    throw new UnsupportedOperationException("Not a union: " + this);
  }

  public Schema getElementType() {
    throw new UnsupportedOperationException("Not an array or multiset: " + this);
  }

  public Schema getKeyType() {
    throw new UnsupportedOperationException("Not a map: " + this);
  }

  public Schema getValueType() {
    throw new UnsupportedOperationException("Not a map: " + this);
  }

  // --- parameterized type accessors ---

  public int getPrecision() {
    throw new UnsupportedOperationException("Not a parameterized type: " + this);
  }

  public int getScale() {
    throw new UnsupportedOperationException("Not a decimal type: " + this);
  }

  public int getLength() {
    throw new UnsupportedOperationException("Not a length-parameterized type: " + this);
  }

  // --- named type ref ---

  public String getQualifiedName() {
    throw new UnsupportedOperationException("Not a named type reference: " + this);
  }

  // --- toDdl ---

  public abstract String toDdl();

  protected String withNullableSuffix(String ddl) {
    return nullable ? ddl : ddl + " NOT NULL";
  }

  // --- static factory methods ---

  public static Schema create(Type type) {
    switch (type) {
      case BOOLEAN:
        return new BooleanSchema();
      case TINYINT:
        return new TinyintSchema();
      case SMALLINT:
        return new SmallintSchema();
      case INT:
        return new IntSchema();
      case BIGINT:
        return new BigintSchema();
      case FLOAT:
        return new FloatSchema();
      case DOUBLE:
        return new DoubleSchema();
      case DATE:
        return new DateSchema();
      case VARIANT:
        return new VariantSchema();
      default:
        throw new ValidationException(
            "Cannot create type without parameters: " + type);
    }
  }

  public static Schema createDecimal(int precision, int scale) {
    // NO_PARAM precision and scale flow through to DecimalSchema and are
    // emitted by toDdl as `DECIMAL` / `DECIMAL(p)` (omitting the absent
    // parameters). Coercing scale to DEFAULT_DECIMAL_SCALE here would lose
    // the user's "scale omitted" intent and break round-tripping the
    // SQL form `DECIMAL(p)` through schema → toDdl.
    return new DecimalSchema(
        precision == NO_PARAM ? DEFAULT_DECIMAL_PRECISION : precision,
        scale);
  }

  /** Creates an unbounded string type (alias for VARCHAR(MAX_LENGTH)). */
  public static Schema createString() {
    return new VarcharSchema(MAX_LENGTH);
  }

  public static Schema createVarchar(int length) {
    return new VarcharSchema(length == NO_PARAM ? DEFAULT_LENGTH : length);
  }

  public static Schema createChar(int length) {
    return new CharSchema(length == NO_PARAM ? DEFAULT_LENGTH : length);
  }

  /** Creates an unbounded bytes type (alias for VARBINARY(MAX_LENGTH)). */
  public static Schema createBytes() {
    return new VarbinarySchema(MAX_LENGTH);
  }

  public static Schema createBinary(int length) {
    return new BinarySchema(length == NO_PARAM ? DEFAULT_LENGTH : length);
  }

  public static Schema createVarbinary(int length) {
    return new VarbinarySchema(length == NO_PARAM ? DEFAULT_LENGTH : length);
  }

  public static Schema createTime(int precision) {
    int p = precision == NO_PARAM ? DEFAULT_TIME_PRECISION : precision;
    validatePrecision("TIME", p);
    return new TimeSchema(p);
  }

  public static Schema createTimestamp(int precision) {
    int p = precision == NO_PARAM ? DEFAULT_TIMESTAMP_PRECISION : precision;
    validatePrecision("TIMESTAMP", p);
    return new TimestampSchema(p);
  }

  public static Schema createTimestampLtz(int precision) {
    int p = precision == NO_PARAM ? DEFAULT_TIMESTAMP_PRECISION : precision;
    validatePrecision("TIMESTAMP_LTZ", p);
    return new TimestampLtzSchema(p);
  }

  private static void validatePrecision(String typeName, int precision) {
    if (precision < 0 || precision > 9) {
      throw new ValidationException(typeName
          + " precision must be in [0, 9], got " + precision);
    }
  }

  public static Schema createStruct(List<Field> fields) {
    return new StructSchema(new ArrayList<>(fields));
  }

  public static Schema createEnum(List<EnumValue> values) {
    return new EnumSchema(new ArrayList<>(values));
  }

  public static Schema createUnion(List<UnionBranch> branches) {
    return new UnionSchema(new ArrayList<>(branches));
  }

  public static Schema createArray(Schema elementType) {
    return new ArraySchema(elementType);
  }

  public static Schema createMultiset(Schema elementType) {
    return new MultisetSchema(elementType);
  }

  public static Schema createMap(Schema keyType, Schema valueType) {
    return new MapSchema(keyType, valueType);
  }

  public static Schema createNamedTypeRef(String qualifiedName) {
    return new NamedTypeRefSchema(qualifiedName);
  }

  // --- equals / hashCode / toString ---

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Schema)) {
      return false;
    }
    Schema that = (Schema) o;
    return type == that.type
        && nullable == that.nullable
        && Objects.equals(doc, that.doc)
        && tags.equals(that.tags)
        && params.equals(that.params)
        && rules.equals(that.rules);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, nullable, doc, tags, params, rules);
  }

  @Override
  public String toString() {
    return toDdl();
  }

  // =========================================================================
  // User-name validation
  // =========================================================================

  /**
   * CEL reserved words. A user-supplied name (field, union branch, enum
   * symbol) that matches one would emit unparseable CEL when referenced
   * via dot syntax (e.g. {@code this.null}). The DDL-path visitor rejects
   * these at parse time; wire-format readers tolerate them so existing
   * Avro/JSON/Proto schemas remain readable. The CHECK translator queries
   * {@link #isCelReservedName(String)} and emits index syntax
   * ({@code this["null"]}) instead of dot syntax for such fields.
   */
  private static final java.util.Set<String> CEL_RESERVED = Collections
      .unmodifiableSet(new java.util.HashSet<>(java.util.Arrays.asList(
          "true", "false", "null", "in", "as")));

  /**
   * True if {@code name} matches a CEL reserved word (case-insensitive).
   * The translator uses this to switch between dot-syntax and index-syntax
   * column-ref emit so reserved-name fields still produce valid CEL.
   */
  public static boolean isCelReservedName(String name) {
    return name != null
        && CEL_RESERVED.contains(name.toLowerCase(java.util.Locale.ROOT));
  }

  /**
   * Lexer-level keywords from {@code LogicalTypes.g4} that must be backtick-
   * quoted when used as user-supplied identifiers in DDL. Excludes the
   * {@code nonReservedKeyword} set ({@code ENUM, MAP, NAMESPACE, REFERENCE,
   * REGISTER, TAGS, TYPE, VARIANT, ZONE}) which the grammar accepts as
   * identifiers without quoting.
   */
  private static final java.util.Set<String> SQL_KEYWORDS_NEEDING_QUOTING =
      Collections.unmodifiableSet(new java.util.HashSet<>(java.util.Arrays.asList(
          "AND", "ARRAY", "AS", "BETWEEN", "BIGINT", "BINARY", "BOOLEAN", "BOTH",
          "BYTES", "CASE", "CAST", "CHARACTER", "CHAR", "CHECK", "COMMENT",
          "CONSTRAINT", "CREATE", "CURRENT_TIMESTAMP", "DATE", "DEC", "DECIMAL",
          "DECLARE", "DEFAULT", "DOUBLE", "ELSE", "END", "ESCAPE", "EXTRACT",
          "FALSE", "FLOAT", "FOR", "FROM", "IN", "INT", "INTEGER", "IS",
          "LEADING", "LIKE", "LOCAL", "MESSAGE", "MULTISET", "NOT", "NULL",
          "NUMERIC", "OR", "POSITION", "PRECISION", "REAL", "ROW", "SMALLINT",
          "STRING", "SUBSTRING", "SYMMETRIC", "SYNONYM", "THEN", "TIME",
          "TIMESTAMP", "TIMESTAMP_LTZ", "TINYINT", "TRAILING", "TRIM", "TRUE",
          "UNION", "VARBINARY", "VARCHAR", "VARYING", "WHEN", "WITH", "WITHOUT")));

  /**
   * Quote {@code name} as a DDL identifier if it would otherwise fail to
   * tokenize as one — i.e. it's empty, contains non-identifier characters,
   * matches a SQL keyword that the grammar doesn't accept as an identifier,
   * or matches a CEL reserved word (so CHECK references emit cleanly).
   * Quoted form: backtick-wrap and double internal backticks.
   *
   * <p>Used by both {@link Schema#toDdl()} and the LT-to-DDL converter so
   * identifier emission is consistent across paths.
   */
  public static String quoteIdentifierIfNeeded(String name) {
    if (name == null) {
      throw new IllegalArgumentException("identifier must not be null");
    }
    if (name.isEmpty()) {
      throw new IllegalArgumentException("identifier must not be empty");
    }
    if (needsQuoting(name)) {
      return "`" + name.replace("`", "``") + "`";
    }
    return name;
  }

  private static boolean needsQuoting(String name) {
    if (name.isEmpty()) {
      return true;
    }
    String upper = name.toUpperCase(java.util.Locale.ROOT);
    if (SQL_KEYWORDS_NEEDING_QUOTING.contains(upper)) {
      return true;
    }
    if (CEL_RESERVED.contains(name.toLowerCase(java.util.Locale.ROOT))) {
      return true;
    }
    char first = name.charAt(0);
    if (!isIdStart(first)) {
      return true;
    }
    for (int i = 1; i < name.length(); i++) {
      if (!isIdPart(name.charAt(i))) {
        return true;
      }
    }
    return false;
  }

  private static boolean isIdStart(char c) {
    boolean isAscii = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    return isAscii || c == '_';
  }

  private static boolean isIdPart(char c) {
    return isIdStart(c) || (c >= '0' && c <= '9');
  }

  /**
   * Validate a user-supplied identifier (field name, union branch name,
   * enum symbol). Reject empty and whitespace/control chars — both would
   * make any wire-format or DDL emit unparseable. CEL-reserved-word
   * rejection is deferred to the DDL visitor (and the translator escapes
   * such names via index-syntax) so wire-format readers can still ingest
   * existing schemas with names like {@code in} or {@code null}.
   *
   * @param name the candidate name
   * @param role short label for the error message (e.g. "Field", "Union branch")
   * @throws ValidationException if the name is invalid
   */
  static void validateUserName(String name, String role) {
    Objects.requireNonNull(name, "name");
    if (name.isEmpty()) {
      throw new ValidationException(role + " name must not be empty");
    }
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isWhitespace(c) || Character.isISOControl(c)) {
        throw new ValidationException(role + " name must not contain "
            + "whitespace or control characters; got: '" + name + "'");
      }
    }
  }

  // =========================================================================
  // Field
  // =========================================================================

  public static class Field {

    private final String name;
    private final Schema schema;
    private final int position;
    private final Object defaultValue;
    private final boolean hasDefault;
    private final String doc;
    private final List<String> tags;
    private final Map<String, Object> params;
    private final List<Rule> rules;

    public Field(String name, Schema schema, int position,
                 Object defaultValue, boolean hasDefault, String doc,
                 List<String> tags, Map<String, Object> params,
                 List<Rule> rules) {
      validateUserName(name, "Field");
      this.name = name;
      this.schema = Objects.requireNonNull(schema, "schema");
      this.position = position;
      this.defaultValue = defaultValue;
      this.hasDefault = hasDefault;
      this.doc = doc;
      this.tags = tags != null
          ? Collections.unmodifiableList(new ArrayList<>(tags))
          : Collections.emptyList();
      this.params = params != null
          ? Collections.unmodifiableMap(new LinkedHashMap<>(params))
          : Collections.emptyMap();
      this.rules = rules != null
          ? Collections.unmodifiableList(new ArrayList<>(rules))
          : Collections.emptyList();
    }

    public Field(String name, Schema schema, int position,
                 Object defaultValue, boolean hasDefault, String doc,
                 List<String> tags, Map<String, Object> params) {
      this(name, schema, position, defaultValue, hasDefault, doc, tags, params, null);
    }

    public Field(String name, Schema schema, int position) {
      this(name, schema, position, null, false, null, null, null, null);
    }

    public String getName() {
      return name;
    }

    public Schema getSchema() {
      return schema;
    }

    public int getPosition() {
      return position;
    }

    public Object getDefaultValue() {
      return defaultValue;
    }

    public boolean hasDefaultValue() {
      return hasDefault;
    }

    public String getDoc() {
      return doc;
    }

    public List<String> getTags() {
      return tags;
    }

    public Map<String, Object> getParams() {
      return params;
    }

    /**
     * Validation rules attached to this field — CHECK constraints written at
     * the column level. Each rule's {@code expr} (CEL) must evaluate to true
     * for the field's value to be considered valid.
     */
    public List<Rule> getRules() {
      return rules;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Field)) {
        return false;
      }
      Field that = (Field) o;
      return position == that.position
          && hasDefault == that.hasDefault
          && Objects.equals(name, that.name)
          && Objects.equals(schema, that.schema)
          && Objects.equals(defaultValue, that.defaultValue)
          && Objects.equals(doc, that.doc)
          && tags.equals(that.tags)
          && params.equals(that.params)
          && rules.equals(that.rules);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, schema, position, defaultValue, hasDefault,
          doc, tags, params, rules);
    }

    @Override
    public String toString() {
      return quoteIdentifierIfNeeded(name) + " " + schema.toDdl();
    }
  }

  // =========================================================================
  // EnumValue
  // =========================================================================

  public static class EnumValue {

    private final String symbol;
    private final String doc;
    private final Map<String, Object> params;

    public EnumValue(String symbol, String doc, Map<String, Object> params) {
      validateUserName(symbol, "Enum symbol");
      this.symbol = symbol;
      this.doc = doc;
      this.params = params != null
          ? Collections.unmodifiableMap(new LinkedHashMap<>(params))
          : Collections.emptyMap();
    }

    public EnumValue(String symbol) {
      this(symbol, null, null);
    }

    public String getSymbol() {
      return symbol;
    }

    public String getDoc() {
      return doc;
    }

    public Map<String, Object> getParams() {
      return params;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof EnumValue)) {
        return false;
      }
      EnumValue that = (EnumValue) o;
      return Objects.equals(symbol, that.symbol);
    }

    @Override
    public int hashCode() {
      return symbol.hashCode();
    }

    @Override
    public String toString() {
      return "'" + symbol + "'";
    }
  }

  // =========================================================================
  // UnionBranch
  // =========================================================================

  public static class UnionBranch {

    private final String name;
    private final Schema schema;
    private final String doc;
    private final Map<String, Object> params;

    public UnionBranch(String name, Schema schema, String doc,
                       Map<String, Object> params) {
      validateUserName(name, "Union branch");
      this.name = name;
      this.schema = Objects.requireNonNull(schema, "schema");
      this.doc = doc;
      this.params = params != null
          ? Collections.unmodifiableMap(new LinkedHashMap<>(params))
          : Collections.emptyMap();
    }

    public UnionBranch(String name, Schema schema) {
      this(name, schema, null, null);
    }

    public String getName() {
      return name;
    }

    public Schema getSchema() {
      return schema;
    }

    public String getDoc() {
      return doc;
    }

    public Map<String, Object> getParams() {
      return params;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof UnionBranch)) {
        return false;
      }
      UnionBranch that = (UnionBranch) o;
      return Objects.equals(name, that.name)
          && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, schema);
    }

    @Override
    public String toString() {
      return quoteIdentifierIfNeeded(name) + " " + schema.toDdl();
    }
  }

  // =========================================================================
  // Complex type schemas
  // =========================================================================

  private static class StructSchema extends Schema {

    private final List<Field> fields;
    private final Map<String, Field> fieldMap;

    StructSchema(List<Field> fields) {
      super(Type.STRUCT);
      // Empty STRUCT is intentionally allowed: real-world proto schemas use
      // `message Empty {}` and the converter must round-trip those. Note
      // that `Schema.toDdl()` would emit `ROW()` which the visitor rejects;
      // callers programmatically working with empty structs shouldn't
      // expect a DDL round-trip.
      this.fields = Collections.unmodifiableList(fields);
      this.fieldMap = new LinkedHashMap<>(fields.size() * 2);
      for (Field f : fields) {
        if (fieldMap.put(f.getName(), f) != null) {
          throw new ValidationException("Duplicate field: " + f.getName());
        }
      }
    }

    @Override
    public List<Field> getFields() {
      return fields;
    }

    @Override
    public Field getField(String name) {
      return fieldMap.get(name);
    }

    @Override
    public String toDdl() {
      StringBuilder sb = new StringBuilder("ROW(");
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(fields.get(i));
      }
      sb.append(")");
      return withNullableSuffix(sb.toString());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StructSchema)) {
        return false;
      }
      return super.equals(o) && fields.equals(((StructSchema) o).fields);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + fields.hashCode();
    }
  }

  private static class EnumSchema extends Schema {

    private final List<EnumValue> values;

    EnumSchema(List<EnumValue> values) {
      super(Type.ENUM);
      // Empty ENUM is allowed for symmetry with empty STRUCT (some wire
      // formats may surface degenerate enums); `Schema.toDdl()` would
      // emit `ENUM()` which the visitor rejects, so callers working with
      // empty enums shouldn't expect a DDL round-trip.
      this.values = Collections.unmodifiableList(values);
      java.util.Set<String> seen = new java.util.HashSet<>(values.size() * 2);
      for (EnumValue v : values) {
        if (!seen.add(v.getSymbol())) {
          throw new ValidationException(
              "Duplicate enum symbol: " + v.getSymbol());
        }
      }
    }

    @Override
    public List<EnumValue> getEnumValues() {
      return values;
    }

    @Override
    public String toDdl() {
      StringBuilder sb = new StringBuilder("ENUM(");
      for (int i = 0; i < values.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(values.get(i));
      }
      sb.append(")");
      return withNullableSuffix(sb.toString());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof EnumSchema)) {
        return false;
      }
      return super.equals(o) && values.equals(((EnumSchema) o).values);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + values.hashCode();
    }
  }

  private static class UnionSchema extends Schema {

    private final List<UnionBranch> branches;

    UnionSchema(List<UnionBranch> branches) {
      super(Type.UNION);
      this.branches = Collections.unmodifiableList(branches);
      java.util.Set<String> seen = new java.util.HashSet<>(branches.size() * 2);
      for (UnionBranch b : branches) {
        if (!seen.add(b.getName())) {
          throw new ValidationException(
              "Duplicate union branch: " + b.getName());
        }
      }
    }

    @Override
    public List<UnionBranch> getBranches() {
      return branches;
    }

    @Override
    public String toDdl() {
      StringBuilder sb = new StringBuilder("UNION(");
      for (int i = 0; i < branches.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(branches.get(i));
      }
      sb.append(")");
      return withNullableSuffix(sb.toString());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof UnionSchema)) {
        return false;
      }
      return super.equals(o) && branches.equals(((UnionSchema) o).branches);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + branches.hashCode();
    }
  }

  private static class ArraySchema extends Schema {

    private final Schema elementType;

    ArraySchema(Schema elementType) {
      super(Type.ARRAY);
      this.elementType = Objects.requireNonNull(elementType);
    }

    @Override
    public Schema getElementType() {
      return elementType;
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("ARRAY<" + elementType.toDdl() + ">");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ArraySchema)) {
        return false;
      }
      return super.equals(o) && elementType.equals(((ArraySchema) o).elementType);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + elementType.hashCode();
    }
  }

  private static class MultisetSchema extends Schema {

    private final Schema elementType;

    MultisetSchema(Schema elementType) {
      super(Type.MULTISET);
      this.elementType = Objects.requireNonNull(elementType);
    }

    @Override
    public Schema getElementType() {
      return elementType;
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("MULTISET<" + elementType.toDdl() + ">");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MultisetSchema)) {
        return false;
      }
      return super.equals(o) && elementType.equals(((MultisetSchema) o).elementType);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + elementType.hashCode();
    }
  }

  private static class MapSchema extends Schema {

    private final Schema keyType;
    private final Schema valueType;

    MapSchema(Schema keyType, Schema valueType) {
      super(Type.MAP);
      this.keyType = Objects.requireNonNull(keyType);
      this.valueType = Objects.requireNonNull(valueType);
    }

    @Override
    public Schema getKeyType() {
      return keyType;
    }

    @Override
    public Schema getValueType() {
      return valueType;
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("MAP<" + keyType.toDdl() + ", " + valueType.toDdl() + ">");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MapSchema)) {
        return false;
      }
      MapSchema that = (MapSchema) o;
      return super.equals(o)
          && keyType.equals(that.keyType) && valueType.equals(that.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), keyType, valueType);
    }
  }

  private static class NamedTypeRefSchema extends Schema {

    private final String qualifiedName;

    NamedTypeRefSchema(String qualifiedName) {
      super(Type.NAMED_TYPE_REF);
      this.qualifiedName = Objects.requireNonNull(qualifiedName);
    }

    @Override
    public String getQualifiedName() {
      return qualifiedName;
    }

    @Override
    public String toDdl() {
      return withNullableSuffix(qualifiedName);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof NamedTypeRefSchema)) {
        return false;
      }
      return super.equals(o)
          && qualifiedName.equals(((NamedTypeRefSchema) o).qualifiedName);
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + qualifiedName.hashCode();
    }
  }

  // =========================================================================
  // Primitive type schemas
  // =========================================================================

  private static class BooleanSchema extends Schema {

    BooleanSchema() {
      super(Type.BOOLEAN);
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("BOOLEAN");
    }
  }

  private static class TinyintSchema extends Schema {

    TinyintSchema() {
      super(Type.TINYINT);
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("TINYINT");
    }
  }

  private static class SmallintSchema extends Schema {

    SmallintSchema() {
      super(Type.SMALLINT);
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("SMALLINT");
    }
  }

  private static class IntSchema extends Schema {

    IntSchema() {
      super(Type.INT);
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("INT");
    }
  }

  private static class BigintSchema extends Schema {

    BigintSchema() {
      super(Type.BIGINT);
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("BIGINT");
    }
  }

  private static class FloatSchema extends Schema {

    FloatSchema() {
      super(Type.FLOAT);
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("FLOAT");
    }
  }

  private static class DoubleSchema extends Schema {

    DoubleSchema() {
      super(Type.DOUBLE);
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("DOUBLE");
    }
  }

  private static class DecimalSchema extends Schema {

    private final int precision;
    private final int scale;

    DecimalSchema(int precision, int scale) {
      super(Type.DECIMAL);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public int getPrecision() {
      return precision;
    }

    @Override
    public int getScale() {
      return scale;
    }

    @Override
    public String toDdl() {
      String base;
      if (precision == NO_PARAM) {
        base = "DECIMAL";
      } else if (scale == NO_PARAM) {
        base = "DECIMAL(" + precision + ")";
      } else {
        base = "DECIMAL(" + precision + ", " + scale + ")";
      }
      return withNullableSuffix(base);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DecimalSchema)) {
        return false;
      }
      DecimalSchema that = (DecimalSchema) o;
      return super.equals(o)
          && precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + precision * 31 + scale;
    }
  }

  private static class VarcharSchema extends Schema {

    private final int length;

    VarcharSchema(int length) {
      super(Type.VARCHAR);
      this.length = length;
    }

    @Override
    public int getLength() {
      return length;
    }

    @Override
    public String toDdl() {
      String base;
      if (length == MAX_LENGTH) {
        base = "STRING";
      } else if (length == NO_PARAM) {
        base = "VARCHAR";
      } else {
        base = "VARCHAR(" + length + ")";
      }
      return withNullableSuffix(base);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof VarcharSchema)) {
        return false;
      }
      return super.equals(o) && length == ((VarcharSchema) o).length;
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + length;
    }
  }

  private static class CharSchema extends Schema {

    private final int length;

    CharSchema(int length) {
      super(Type.CHAR);
      this.length = length;
    }

    @Override
    public int getLength() {
      return length;
    }

    @Override
    public String toDdl() {
      String base = length == NO_PARAM ? "CHAR" : "CHAR(" + length + ")";
      return withNullableSuffix(base);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CharSchema)) {
        return false;
      }
      return super.equals(o) && length == ((CharSchema) o).length;
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + length;
    }
  }

  private static class BinarySchema extends Schema {

    private final int length;

    BinarySchema(int length) {
      super(Type.BINARY);
      this.length = length;
    }

    @Override
    public int getLength() {
      return length;
    }

    @Override
    public String toDdl() {
      String base = length == NO_PARAM ? "BINARY" : "BINARY(" + length + ")";
      return withNullableSuffix(base);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BinarySchema)) {
        return false;
      }
      return super.equals(o) && length == ((BinarySchema) o).length;
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + length;
    }
  }

  private static class VarbinarySchema extends Schema {

    private final int length;

    VarbinarySchema(int length) {
      super(Type.VARBINARY);
      this.length = length;
    }

    @Override
    public int getLength() {
      return length;
    }

    @Override
    public String toDdl() {
      String base;
      if (length == MAX_LENGTH) {
        base = "BYTES";
      } else if (length == NO_PARAM) {
        base = "VARBINARY";
      } else {
        base = "VARBINARY(" + length + ")";
      }
      return withNullableSuffix(base);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof VarbinarySchema)) {
        return false;
      }
      return super.equals(o) && length == ((VarbinarySchema) o).length;
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + length;
    }
  }

  private static class DateSchema extends Schema {

    DateSchema() {
      super(Type.DATE);
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("DATE");
    }
  }

  private static class TimeSchema extends Schema {

    private final int precision;

    TimeSchema(int precision) {
      super(Type.TIME);
      this.precision = precision;
    }

    @Override
    public int getPrecision() {
      return precision;
    }

    @Override
    public String toDdl() {
      String base = precision == NO_PARAM ? "TIME" : "TIME(" + precision + ")";
      return withNullableSuffix(base);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimeSchema)) {
        return false;
      }
      return super.equals(o) && precision == ((TimeSchema) o).precision;
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + precision;
    }
  }

  private static class TimestampSchema extends Schema {

    private final int precision;

    TimestampSchema(int precision) {
      super(Type.TIMESTAMP);
      this.precision = precision;
    }

    @Override
    public int getPrecision() {
      return precision;
    }

    @Override
    public String toDdl() {
      String base = precision == NO_PARAM ? "TIMESTAMP" : "TIMESTAMP(" + precision + ")";
      return withNullableSuffix(base);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimestampSchema)) {
        return false;
      }
      return super.equals(o) && precision == ((TimestampSchema) o).precision;
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + precision;
    }
  }

  private static class TimestampLtzSchema extends Schema {

    private final int precision;

    TimestampLtzSchema(int precision) {
      super(Type.TIMESTAMP_LTZ);
      this.precision = precision;
    }

    @Override
    public int getPrecision() {
      return precision;
    }

    @Override
    public String toDdl() {
      String base = precision == NO_PARAM ? "TIMESTAMP_LTZ" : "TIMESTAMP_LTZ(" + precision + ")";
      return withNullableSuffix(base);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimestampLtzSchema)) {
        return false;
      }
      return super.equals(o) && precision == ((TimestampLtzSchema) o).precision;
    }

    @Override
    public int hashCode() {
      return super.hashCode() * 31 + precision;
    }
  }

  private static class VariantSchema extends Schema {

    VariantSchema() {
      super(Type.VARIANT);
    }

    @Override
    public String toDdl() {
      return withNullableSuffix("VARIANT");
    }
  }
}
