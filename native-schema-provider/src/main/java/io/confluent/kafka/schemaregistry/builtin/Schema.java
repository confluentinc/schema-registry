/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.builtin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.confluent.kafka.schemaregistry.builtin.Schema.ArraySchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.BooleanSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.ByteSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.BytesSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.DoubleSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.EnumSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.FixedBinarySchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.FixedCharSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.FloatSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.IntSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.LongSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.MapSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.NullSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.ShortSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.StringSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.StructSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema.UnionSchema;
import io.confluent.kafka.schemaregistry.builtin.util.internal.Accessor;
import io.confluent.kafka.schemaregistry.builtin.util.internal.Accessor.FieldAccessor;
import io.confluent.kafka.schemaregistry.builtin.util.internal.JacksonUtils;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.util.internal.ThreadLocalWithInitial;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract data type.
 *
 * <p/>
 * A schema may be one of:
 * <ul>
 * <li>A <i>struct</i>, mapping field names to field value data;
 * <li>An <i>enum</i>, containing one of a small set of symbols;
 * <li>An <i>array</i> of values, all of the same schema;
 * <li>A <i>map</i>, containing string/value pairs, of a declared schema;
 * <li>A <i>union</i> of other schemas;
 * <li>A fixed sized <i>binary</i> object;
 * <li>A fixed sized <i>char</i> object;
 * <li>A unicode <i>string</i>;
 * <li>A sequence of <i>bytes</i>;
 * <li>A 32-bit signed <i>int</i>;
 * <li>A 64-bit signed <i>long</i>;
 * <li>A 32-bit IEEE single-<i>float</i>; or
 * <li>A 64-bit IEEE <i>double</i>-float; or
 * <li>A <i>boolean</i>; or
 * <li><i>null</i>.
 * </ul>
 *
 * <p/>
 * Construct a schema using one of its static <tt>createXXX</tt> methods, or
 * more conveniently using {@link SchemaBuilder}. The schema objects are
 * <i>logically</i> immutable. There are only two mutating methods -
 * {@link #setFields(List)} and {@link #addProp(String, String)}. The following
 * restrictions apply on these two methods.
 * <ul>
 * <li>{@link #setFields(List)}, can be called at most once. This method exists
 * in order to enable clients to build recursive schemas.
 * <li>{@link #addProp(String, String)} can be called with property names that
 * are not present already. It is not possible to change or delete an existing
 * property.
 * </ul>
 */
@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.EXISTING_PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = StructSchema.class, name = "struct"),
    @JsonSubTypes.Type(value = EnumSchema.class, name = "enum"),
    @JsonSubTypes.Type(value = ArraySchema.class, name = "array"),
    @JsonSubTypes.Type(value = MapSchema.class, name = "map"),
    @JsonSubTypes.Type(value = UnionSchema.class, name = "union"),
    @JsonSubTypes.Type(value = FixedCharSchema.class, name = "char"),
    @JsonSubTypes.Type(value = StringSchema.class, name = "string"),
    @JsonSubTypes.Type(value = FixedBinarySchema.class, name = "binary"),
    @JsonSubTypes.Type(value = BytesSchema.class, name = "bytes"),
    @JsonSubTypes.Type(value = ByteSchema.class, name = "int8"),
    @JsonSubTypes.Type(value = ShortSchema.class, name = "int16"),
    @JsonSubTypes.Type(value = IntSchema.class, name = "int32"),
    @JsonSubTypes.Type(value = LongSchema.class, name = "int64"),
    @JsonSubTypes.Type(value = FloatSchema.class, name = "float32"),
    @JsonSubTypes.Type(value = DoubleSchema.class, name = "float64"),
    @JsonSubTypes.Type(value = BooleanSchema.class, name = "boolean"),
    @JsonSubTypes.Type(value = NullSchema.class, name = "null"),
    // TODO decimal, etc.
})
@SuppressWarnings("unused")
public abstract class Schema extends JsonProperties {

  static final JsonFactory FACTORY = new JsonFactory();
  static final Logger LOG = LoggerFactory.getLogger(Schema.class);
  static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  private static final int NO_HASHCODE = Integer.MIN_VALUE;

  static {
    FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
    FACTORY.setCodec(MAPPER);
  }

  /**
   * The type of schema.
   */
  public enum Type {
    STRUCT, ENUM, ARRAY, MAP, UNION, CHAR, STRING, BINARY, BYTES,
    INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, NULL,
    DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ, INTERVAL_DAY_TIME, INTERVAL_YEAR_MONTH,;

    private final String name;

    Type() {
      switch (this) {
        case INT8:
          this.name = "byte";
          break;
        case INT16:
          this.name = "short";
          break;
        case INT32:
          this.name = "int";
          break;
        case INT64:
          this.name = "long";
          break;
        case FLOAT32:
          this.name = "float";
          break;
        case FLOAT64:
          this.name = "double";
          break;
        default:
          this.name = this.name().toLowerCase(Locale.ENGLISH);
          break;
      }
    }

    public String getName() {
      return name;
    }
  }

  private final Type type;

  Schema(Type type) {
    super(type == Type.ENUM ? ENUM_RESERVED : SCHEMA_RESERVED);
    this.type = type;
  }

  /**
   * Create a schema for a primitive type.
   */
  public static Schema create(Type type) {
    switch (type) {
      case STRING:
        return new StringSchema();
      case BYTES:
        return new BytesSchema();
      case INT8:
        return new ByteSchema();
      case INT16:
        return new ShortSchema();
      case INT32:
        return new IntSchema();
      case INT64:
        return new LongSchema();
      case FLOAT32:
        return new FloatSchema();
      case FLOAT64:
        return new DoubleSchema();
      case BOOLEAN:
        return new BooleanSchema();
      case NULL:
        return new NullSchema();
      default:
        throw new SchemaRuntimeException("Can't create a: " + type);
    }
  }

  private static final Set<String> SCHEMA_RESERVED = new HashSet<>(
      Arrays.asList("doc", "fields", "items", "name", "namespace", "size", "symbols", "values",
          "type", "aliases", "tags"));

  private static final Set<String> ENUM_RESERVED = new HashSet<>(SCHEMA_RESERVED);

  static {
    ENUM_RESERVED.add("default");
  }

  int hashCode = NO_HASHCODE;

  @Override
  public void addProp(String name, String value) {
    super.addProp(name, value);
    hashCode = NO_HASHCODE;
  }

  @Override
  public void addProp(String name, Object value) {
    super.addProp(name, value);
    hashCode = NO_HASHCODE;
  }

  /**
   * Create a named struct schema.
   */
  public static Schema createStruct(String name, String doc, String namespace, boolean isError) {
    return new StructSchema(new Name(name, namespace), doc, isError);
  }

  /**
   * Create a named struct schema with fields already set.
   */
  public static Schema createStruct(String name, String doc, String namespace, boolean isError,
      List<Field> fields) {
    return new StructSchema(new Name(name, namespace), doc, isError, fields);
  }

  /**
   * Create an enum schema.
   */
  public static Schema createEnum(String name, String doc, String namespace, List<String> values) {
    return new EnumSchema(new Name(name, namespace), doc, new LockableArrayList<>(values), null);
  }

  /**
   * Create an enum schema.
   */
  public static Schema createEnum(String name, String doc, String namespace, List<String> values,
      String enumDefault) {
    return new EnumSchema(new Name(name, namespace), doc, new LockableArrayList<>(values),
        enumDefault);
  }

  /**
   * Create an array schema.
   */
  public static Schema createArray(Schema elementType) {
    return new ArraySchema(elementType);
  }

  /**
   * Create a map schema.
   */
  public static Schema createMap(Schema keyType, Schema valueType) {
    return new MapSchema(keyType, valueType);
  }

  /**
   * Create a union schema.
   */
  public static Schema createUnion(List<Schema> types) {
    return new UnionSchema(new LockableArrayList<>(types));
  }

  /**
   * Create a union schema.
   */
  public static Schema createUnion(Schema... types) {
    return createUnion(new LockableArrayList<>(types));
  }

  /**
   * Create a binary schema.
   */
  public static Schema createFixedBinary(int size) {
    return new FixedBinarySchema(size);
  }

  /**
   * Create a char schema.
   */
  public static Schema createFixedChar(int size) {
    return new FixedCharSchema(size);
  }

  /**
   * Return the type of this schema.
   */
  @JsonIgnore
  public Type getType() {
    return type;
  }

  /**
   * Return the type name of this schema.
   */
  @JsonProperty("type")
  public String getTypeName() {
    return type.getName();
  }

  /**
   * If this is a struct, returns the Field with the given name
   * <tt>fieldName</tt>. If there is no field by that name, a <tt>null</tt> is
   * returned.
   */
  public Field getField(String fieldName) {
    throw new SchemaRuntimeException("Not a struct: " + this);
  }

  /**
   * If this is a struct, returns the fields in it. The returned list is in the order of their
   * positions.
   */
  @JsonIgnore
  public List<Field> getFields() {
    throw new SchemaRuntimeException("Not a struct: " + this);
  }

  /**
   * If this is a struct, returns whether the fields have been set.
   */
  @JsonIgnore
  public boolean hasFields() {
    throw new SchemaRuntimeException("Not a struct: " + this);
  }

  /**
   * If this is a struct, set its fields. The fields can be set only once in a schema.
   */
  @JsonIgnore
  public void setFields(List<Field> fields) {
    throw new SchemaRuntimeException("Not a struct: " + this);
  }

  /**
   * If this is an enum, return its symbols.
   */
  @JsonIgnore
  public List<String> getEnumSymbols() {
    throw new SchemaRuntimeException("Not an enum: " + this);
  }

  /**
   * If this is an enum, return its default value.
   */
  @JsonIgnore
  public String getEnumDefault() {
    throw new SchemaRuntimeException("Not an enum: " + this);
  }

  /**
   * If this is an enum, return a symbol's ordinal value.
   */
  public int getEnumOrdinal(String symbol) {
    throw new SchemaRuntimeException("Not an enum: " + this);
  }

  /**
   * If this is an enum, returns true if it contains given symbol.
   */
  public boolean hasEnumSymbol(String symbol) {
    throw new SchemaRuntimeException("Not an enum: " + this);
  }

  /**
   * If this is a struct or enum, returns its name, otherwise the name of the primitive type.
   */
  @JsonIgnore
  public String getName() {
    return type.name;
  }

  /**
   * If this is a struct or enum, returns its docstring, if available. Otherwise, returns null.
   */
  @JsonIgnore
  public String getDoc() {
    return null;
  }

  /**
   * If this is a struct or enum, returns its namespace, if any.
   */
  @JsonIgnore
  public String getNamespace() {
    throw new SchemaRuntimeException("Not a named type: " + this);
  }

  /**
   * If this is a struct or enum, returns its namespace-qualified name, otherwise returns the name
   * of the primitive type.
   */
  @JsonIgnore
  public String getFullName() {
    return getName();
  }

  /**
   * If this is a struct or enum, add an alias.
   */
  public void addAlias(String alias) {
    throw new SchemaRuntimeException("Not a named type: " + this);
  }

  /**
   * If this is a struct or enum, add an alias.
   */
  public void addAlias(String alias, String space) {
    throw new SchemaRuntimeException("Not a named type: " + this);
  }

  /**
   * If this is a struct or enum, return its aliases, if any.
   */
  @JsonIgnore
  public Set<String> getAliases() {
    throw new SchemaRuntimeException("Not a named type: " + this);
  }

  /**
   * If this is a struct or enum, add a tag.
   */
  public void addTag(String tag) {
    throw new SchemaRuntimeException("Not a named type: " + this);
  }

  /**
   * If this is a struct or enum, return its tags, if any.
   */
  @JsonIgnore
  public Set<String> getTags() {
    throw new SchemaRuntimeException("Not a named type: " + this);
  }

  /**
   * Returns true if this struct is an error type.
   */
  @JsonIgnore
  public boolean isError() {
    throw new SchemaRuntimeException("Not a struct: " + this);
  }

  /**
   * If this is an array, returns its element type.
   */
  @JsonIgnore
  public Schema getElementType() {
    throw new SchemaRuntimeException("Not an array: " + this);
  }

  /**
   * If this is a map, returns its key type.
   */
  @JsonIgnore
  public Schema getKeyType() {
    throw new SchemaRuntimeException("Not a map: " + this);
  }

  /**
   * If this is a map, returns its value type.
   */
  @JsonIgnore
  public Schema getValueType() {
    throw new SchemaRuntimeException("Not a map: " + this);
  }

  /**
   * If this is a union, returns its types.
   */
  @JsonIgnore
  public List<Schema> getTypes() {
    throw new SchemaRuntimeException("Not a union: " + this);
  }

  /**
   * If this is a union, return the branch with the provided full name.
   */
  public Integer getIndexNamed(String name) {
    throw new SchemaRuntimeException("Not a union: " + this);
  }

  /**
   * If this is fixed, returns its size.
   */
  @JsonIgnore
  public int getFixedSize() {
    throw new SchemaRuntimeException("Not fixed: " + this);
  }

  /**
   * <p>
   * Render this as <a href="https://json.org/">JSON</a>.
   * </p>
   *
   * <p>
   * This method is equivalent to: {@code SchemaFormatter.getInstance("json").format(this)}
   * </p>
   */
  @Override
  public String toString() {
    return toString(new HashSet<String>(), false);
  }

  String toString(Collection<Schema> referencedSchemas, boolean pretty) {
    Set<String> knownNames = new HashSet<>();
    if (referencedSchemas != null) {
      for (Schema s : referencedSchemas) {
        knownNames.add(s.getFullName());
      }
    }
    return toString(knownNames, pretty);
  }

  String toString(Set<String> knownNames, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      try (JsonGenerator gen = FACTORY.createGenerator(writer)) {
        if (pretty) {
          gen.useDefaultPrettyPrinter();
        }
        toJson(knownNames, null, gen);
        gen.flush();
        return writer.toString();
      }
    } catch (IOException e) {
      throw new SchemaRuntimeException(e);
    }
  }

  void toJson(Set<String> knownNames, String namespace, JsonGenerator gen) throws IOException {
    if (!hasProps()) { // no props defined
      gen.writeString(getName()); // just write name
    } else {
      gen.writeStartObject();
      gen.writeStringField("type", getName());
      writeProps(gen);
      gen.writeEndObject();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Schema)) {
      return false;
    }
    Schema that = (Schema) o;
    if (!(this.type == that.type)) {
      return false;
    }
    return equalCachedHash(that) && propsEqual(that);
  }

  @Override
  public final int hashCode() {
    if (hashCode == NO_HASHCODE) {
      hashCode = computeHash();
    }
    return hashCode;
  }

  int computeHash() {
    return getType().hashCode() + propsHashCode();
  }

  final boolean equalCachedHash(Schema other) {
    return (hashCode == other.hashCode) || (hashCode == NO_HASHCODE) || (other.hashCode
        == NO_HASHCODE);
  }

  private static final Set<String> FIELD_RESERVED = Collections
      .unmodifiableSet(
          new HashSet<>(Arrays.asList(
              "default", "doc", "name", "order", "type", "aliases", "tags")));

  /**
   * Returns true if this struct is a union type.
   */
  @JsonIgnore
  public boolean isUnion() {
    return this instanceof UnionSchema;
  }

  /**
   * Returns true if this struct is a union type containing null.
   */
  @JsonIgnore
  public boolean isNullable() {
    if (!isUnion()) {
      return getType().equals(Type.NULL);
    }

    for (Schema schema : getTypes()) {
      if (schema.isNullable()) {
        return true;
      }
    }

    return false;
  }

  /**
   * A field within a struct.
   */
  @JsonInclude(Include.NON_EMPTY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Field extends JsonProperties {

    static {
      Accessor.setAccessor(new FieldAccessor() {
        @Override
        protected JsonNode defaultValue(Field field) {
          return field.defaultValue();
        }

        @Override
        protected Field createField(String name, Schema schema, String doc, JsonNode defaultValue) {
          return new Field(name, schema, doc, defaultValue, true, Order.ASCENDING);
        }

        @Override
        protected Field createField(String name, Schema schema, String doc, JsonNode defaultValue,
            boolean validate,
            Order order) {
          return new Field(name, schema, doc, defaultValue, validate, order);
        }
      });
    }

    /**
     * How values of this field should be ordered when sorting structs.
     */
    public enum Order {
      ASCENDING, DESCENDING, IGNORE;

      private final String name;

      Order() {
        this.name = this.name().toLowerCase(Locale.ENGLISH);
      }
    }

    /**
     * For Schema unions with a "null" type as the first entry, this can be used to specify that the
     * default for the union is null.
     */
    public static final Object NULL_DEFAULT_VALUE = new Object();

    private final String name; // name of the field.
    private int position = -1;
    private final Schema schema;
    private final String doc;
    private final JsonNode defaultValue;
    private final Order order;
    private Set<String> aliases;
    private Set<String> tags;

    Field(String name, Schema schema, String doc, JsonNode defaultValue, boolean validateDefault,
        Order order) {
      super(FIELD_RESERVED);
      this.name = validateName(name);
      this.schema = Objects.requireNonNull(schema, "schema is required and cannot be null");
      this.doc = doc;
      this.defaultValue =
          validateDefault ? validateDefault(name, schema, defaultValue) : defaultValue;
      this.order = Objects.requireNonNull(order, "Order cannot be null");
    }

    /**
     * Constructs a new Field instance with the same {@code name}, {@code doc},
     * {@code defaultValue}, and {@code order} as {@code field} has with changing the schema to the
     * specified one. It also copies all the {@code props}, {@code aliases} and {@code tags}.
     */
    public Field(Field field, Schema schema) {
      this(field.name, schema, field.doc, field.defaultValue, true, field.order);
      putAll(field);
      if (field.aliases != null) {
        aliases = new LinkedHashSet<>(field.aliases);
      }
      if (field.tags != null) {
        tags = new LinkedHashSet<>(field.tags);
      }
    }

    /**
     *
     */
    public Field(String name, Schema schema) {
      this(name, schema, null, null, true, Order.ASCENDING);
    }

    /**
     *
     */
    public Field(String name, Schema schema, String doc) {
      this(name, schema, doc, null, true, Order.ASCENDING);
    }

    /**
     * @param defaultValue the default value for this field specified using the mapping in
     *                     {@link JsonProperties}
     */
    public Field(String name, Schema schema, String doc, Object defaultValue) {
      this(name, schema, doc,
          defaultValue == NULL_DEFAULT_VALUE ? NullNode.getInstance()
              : JacksonUtils.toJsonNode(defaultValue), true,
          Order.ASCENDING);
    }

    /**
     * @param defaultValue the default value for this field specified using the mapping in
     *                     {@link JsonProperties}
     */
    public Field(String name, Schema schema, String doc, Object defaultValue, Order order) {
      this(name, schema, doc,
          defaultValue == NULL_DEFAULT_VALUE ? NullNode.getInstance()
              : JacksonUtils.toJsonNode(defaultValue), true,
          Objects.requireNonNull(order));
    }

    @JsonProperty("name")
    public String name() {
      return name;
    }

    /**
     * The position of this field within the struct.
     */
    @JsonIgnore
    public int pos() {
      return position;
    }

    /**
     * This field's {@link Schema}.
     */
    @JsonProperty("type")
    public Schema schema() {
      return schema;
    }

    /**
     * Field's documentation within the struct, if set. May return null.
     */
    @JsonProperty("doc")
    public String doc() {
      return doc;
    }

    /**
     * @return true if this Field has a default value set. Can be used to determine if a "null"
     *         return from defaultVal() is due to that being the default value or just not set.
     */
    @JsonIgnore
    public boolean hasDefaultValue() {
      return defaultValue != null;
    }

    @JsonIgnore
    JsonNode defaultValue() {
      return defaultValue;
    }

    /**
     * @return the default value for this field specified using the mapping in
     *         {@link JsonProperties}
     */
    @JsonProperty("default")
    public Object defaultVal() {
      return JacksonUtils.toObject(defaultValue, schema);
    }

    @JsonIgnore
    public Order order() {
      return order;
    }

    public void addAlias(String alias) {
      if (aliases == null) {
        this.aliases = new LinkedHashSet<>();
      }
      aliases.add(alias);
    }

    /**
     * Return the defined aliases as an unmodifiable Set.
     */
    @JsonProperty("aliases")
    public Set<String> aliases() {
      if (aliases == null) {
        return Collections.emptySet();
      }
      return Collections.unmodifiableSet(aliases);
    }

    public void addTag(String tag) {
      if (tags == null) {
        this.tags = new LinkedHashSet<>();
      }
      tags.add(tag);
    }

    /**
     * Return the defined tags as an unmodifiable Set.
     */
    @JsonProperty("tags")
    public Set<String> tags() {
      if (tags == null) {
        return Collections.emptySet();
      }
      return Collections.unmodifiableSet(tags);
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (!(other instanceof Field)) {
        return false;
      }
      Field that = (Field) other;
      return (name.equals(that.name)) && (schema.equals(that.schema)) && defaultValueEquals(
          that.defaultValue)
          && (order == that.order) && propsEqual(that);
    }

    @Override
    public int hashCode() {
      return name.hashCode() + schema.computeHash();
    }

    private boolean defaultValueEquals(JsonNode thatDefaultValue) {
      if (defaultValue == null) {
        return thatDefaultValue == null;
      }
      if (thatDefaultValue == null) {
        return false;
      }
      if (Double.isNaN(defaultValue.doubleValue())) {
        return Double.isNaN(thatDefaultValue.doubleValue());
      }
      return defaultValue.equals(thatDefaultValue);
    }

    @Override
    public String toString() {
      return name + " type:" + schema.type + " pos:" + position;
    }
  }

  static class Name {

    private final String name;
    private final String space;
    private final String full;

    public Name(String name, String space) {
      if (name == null) { // anonymous
        this.name = this.space = this.full = null;
        return;
      }
      int lastDot = name.lastIndexOf('.');
      if (lastDot < 0) { // unqualified name
        this.name = validateName(name);
      } else { // qualified name
        space = name.substring(0, lastDot); // get space from name
        this.name = validateName(name.substring(lastDot + 1));
      }
      if ("".equals(space)) {
        space = null;
      }
      this.space = space;
      this.full = (this.space == null) ? this.name : this.space + "." + this.name;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Name)) {
        return false;
      }
      Name that = (Name) o;
      return Objects.equals(full, that.full);
    }

    @Override
    public int hashCode() {
      return full == null ? 0 : full.hashCode();
    }

    @Override
    public String toString() {
      return full;
    }

    public void writeName(String currentNamespace, JsonGenerator gen) throws IOException {
      if (name != null) {
        gen.writeStringField("name", name);
      }
      if (space != null) {
        if (!space.equals(currentNamespace)) {
          gen.writeStringField("namespace", space);
        }
      } else if (currentNamespace != null) { // null within non-null
        gen.writeStringField("namespace", "");
      }
    }

    public String getQualified(String defaultSpace) {
      return this.shouldWriteFull(defaultSpace) ? full : name;
    }

    /**
     * Determine if full name must be written. There are 2 cases for true : {@code defaultSpace} !=
     * from {@code this.space} or name is already a {@code Schema.Type} (int, array, ...)
     *
     * @param defaultSpace : default name space.
     * @return true if full name must be written.
     */
    private boolean shouldWriteFull(String defaultSpace) {
      if (space != null && space.equals(defaultSpace)) {
        for (Type schemaType : Type.values()) {
          if (schemaType.name.equals(name)) {
            // name is a 'Type', so namespace must be written
            return true;
          }
        }
        // this.space == defaultSpace
        return false;
      }
      // this.space != defaultSpace, so namespace must be written.
      return true;
    }

  }

  private abstract static class NamedSchema extends Schema {

    final Name name;
    final String doc;
    Set<Name> aliases;
    Set<String> tags;

    public NamedSchema(Type type, Name name, String doc) {
      super(type);
      this.name = name;
      this.doc = doc;
      if (PRIMITIVES.containsKey(name.full)) {
        throw new SchemaTypeException("Schemas may not be named after primitives: " + name.full);
      }
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("name")
    public String getName() {
      return name.name;
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("doc")
    public String getDoc() {
      return doc;
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("namespace")
    public String getNamespace() {
      return name.space;
    }

    @Override
    @JsonIgnore
    public String getFullName() {
      return name.full;
    }

    @Override
    public void addAlias(String alias) {
      addAlias(alias, null);
    }

    @Override
    public void addAlias(String name, String space) {
      if (aliases == null) {
        this.aliases = new LinkedHashSet<>();
      }
      if (space == null) {
        space = this.name.space;
      }
      aliases.add(new Name(name, space));
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("aliases")
    public Set<String> getAliases() {
      Set<String> result = new LinkedHashSet<>();
      if (aliases != null) {
        for (Name alias : aliases) {
          if (alias.space == null && name.space != null) {
            result.add("." + alias.name);
          } else {
            result.add(alias.full);
          }
        }
      }
      return result;
    }

    @Override
    public void addTag(String tag) {
      if (tags == null) {
        this.tags = new LinkedHashSet<>();
      }
      tags.add(tag);
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("tags")
    public Set<String> getTags() {
      Set<String> result = new LinkedHashSet<>();
      if (tags != null) {
        result.addAll(tags);
      }
      return result;
    }

    public boolean writeNameRef(Set<String> knownNames, String currentNamespace, JsonGenerator gen)
        throws IOException {
      if (name.name != null) {
        if (!knownNames.add(name.full)) {
          gen.writeString(name.getQualified(currentNamespace));
          return true;
        }
      }
      return false;
    }

    public void writeName(String currentNamespace, JsonGenerator gen) throws IOException {
      name.writeName(currentNamespace, gen);
    }

    public boolean equalNames(NamedSchema that) {
      return this.name.equals(that.name);
    }

    @Override
    int computeHash() {
      return super.computeHash() + name.hashCode();
    }
  }

  /**
   * Useful as key of {@link Map}s when traversing two schemas at the same time and need to watch
   * for recursion.
   */
  public static class SeenPair {

    private final Object s1;
    private final Object s2;

    public SeenPair(Object s1, Object s2) {
      this.s1 = s1;
      this.s2 = s2;
    }

    public boolean equals(Object o) {
      if (!(o instanceof SeenPair)) {
        return false;
      }
      return this.s1 == ((SeenPair) o).s1 && this.s2 == ((SeenPair) o).s2;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(s1) + System.identityHashCode(s2);
    }
  }

  private static final ThreadLocal<Set<SeenPair>> SEEN_EQUALS = ThreadLocalWithInitial.of(
      HashSet::new);
  private static final ThreadLocal<Map<Schema, Schema>> SEEN_HASHCODE = ThreadLocalWithInitial.of(
      IdentityHashMap::new);

  protected static class StructSchema extends NamedSchema {

    private List<Field> fields;
    private Map<String, Field> fieldMap;
    private final boolean isError;

    public StructSchema(Name name, String doc, boolean isError) {
      super(Type.STRUCT, name, doc);
      this.isError = isError;
    }

    public StructSchema(Name name, String doc, boolean isError, List<Field> fields) {
      super(Type.STRUCT, name, doc);
      this.isError = isError;
      setFields(fields);
    }

    @Override
    @JsonIgnore
    public boolean isError() {
      return isError;
    }

    @Override
    public Field getField(String fieldName) {
      if (fieldMap == null) {
        throw new SchemaRuntimeException("Schema fields not set yet");
      }
      return fieldMap.get(fieldName);
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("fields")
    public List<Field> getFields() {
      if (fields == null) {
        throw new SchemaRuntimeException("Schema fields not set yet");
      }
      return fields;
    }

    @Override
    @JsonIgnore
    public boolean hasFields() {
      return fields != null;
    }

    @Override
    @JsonProperty("fields")
    public void setFields(List<Field> fields) {
      if (this.fields != null) {
        throw new SchemaRuntimeException("Fields are already set");
      }
      int i = 0;
      fieldMap = new HashMap<>(Math.multiplyExact(2, fields.size()));
      LockableArrayList<Field> ff = new LockableArrayList<>(fields.size());
      for (Field f : fields) {
        if (f.position != -1) {
          throw new SchemaRuntimeException("Field already used: " + f);
        }
        f.position = i++;
        final Field existingField = fieldMap.put(f.name(), f);
        if (existingField != null) {
          throw new SchemaRuntimeException(
              String.format("Duplicate field %s in struct %s: %s and %s.", f.name(), name, f,
                  existingField));
        }
        ff.add(f);
      }
      this.fields = ff.lock();
      this.hashCode = NO_HASHCODE;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof StructSchema)) {
        return false;
      }
      StructSchema that = (StructSchema) o;
      if (!equalCachedHash(that)) {
        return false;
      }
      if (!equalNames(that)) {
        return false;
      }
      if (!propsEqual(that)) {
        return false;
      }
      Set<SeenPair> seen = SEEN_EQUALS.get();
      SeenPair here = new SeenPair(this, o);
      if (seen.contains(here)) {
        return true; // prevent stack overflow
      }
      boolean first = seen.isEmpty();
      try {
        seen.add(here);
        return Objects.equals(fields, that.fields);
      } finally {
        if (first) {
          seen.clear();
        }
      }
    }

    @Override
    int computeHash() {
      Map<Schema, Schema> seen = SEEN_HASHCODE.get();
      if (seen.containsKey(this)) {
        return 0; // prevent stack overflow
      }
      boolean first = seen.isEmpty();
      try {
        seen.put(this, this);
        return super.computeHash() + fields.hashCode();
      } finally {
        if (first) {
          seen.clear();
        }
      }
    }
  }

  protected static class EnumSchema extends NamedSchema {

    private final List<String> symbols;
    private final Map<String, Integer> ordinals;
    private final String enumDefault;

    public EnumSchema(Name name, String doc, LockableArrayList<String> symbols,
        String enumDefault) {
      super(Type.ENUM, name, doc);
      this.symbols = symbols.lock();
      this.ordinals = new HashMap<>(Math.multiplyExact(2, symbols.size()));
      this.enumDefault = enumDefault;
      int i = 0;
      for (String symbol : symbols) {
        if (ordinals.put(validateName(symbol), i++) != null) {
          throw new SchemaParseException("Duplicate enum symbol: " + symbol);
        }
      }
      if (enumDefault != null && !symbols.contains(enumDefault)) {
        throw new SchemaParseException(
            "The Enum Default: " + enumDefault + " is not in the enum symbol set: " + symbols);
      }
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("symbols")
    public List<String> getEnumSymbols() {
      return symbols;
    }

    @Override
    public boolean hasEnumSymbol(String symbol) {
      return ordinals.containsKey(symbol);
    }

    @Override
    public int getEnumOrdinal(String symbol) {
      Integer ordinal = ordinals.get(symbol);
      if (ordinal == null) {
        throw new SchemaTypeException(
            "enum value '" + symbol + "' is not in the enum symbol set: " + symbols);
      }
      return ordinal;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof EnumSchema)) {
        return false;
      }
      EnumSchema that = (EnumSchema) o;
      return equalCachedHash(that) && equalNames(that) && symbols.equals(that.symbols)
          && propsEqual(that);
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("default")
    public String getEnumDefault() {
      return enumDefault;
    }

    @Override
    int computeHash() {
      return super.computeHash() + symbols.hashCode();
    }
  }

  protected static class ArraySchema extends Schema {

    private final Schema elementType;

    public ArraySchema(Schema elementType) {
      super(Type.ARRAY);
      this.elementType = elementType;
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("items")
    public Schema getElementType() {
      return elementType;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof ArraySchema)) {
        return false;
      }
      ArraySchema that = (ArraySchema) o;
      return equalCachedHash(that) && elementType.equals(that.elementType) && propsEqual(that);
    }

    @Override
    int computeHash() {
      return super.computeHash() + elementType.computeHash();
    }
  }

  protected static class MapSchema extends Schema {

    private final Schema keyType;
    private final Schema valueType;

    public MapSchema(Schema keyType, Schema valueType) {
      super(Type.MAP);
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("keys")
    public Schema getKeyType() {
      return valueType;
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("values")
    public Schema getValueType() {
      return valueType;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof MapSchema)) {
        return false;
      }
      MapSchema that = (MapSchema) o;
      return equalCachedHash(that) && valueType.equals(that.valueType) && propsEqual(that);
    }

    @Override
    int computeHash() {
      return super.computeHash() + valueType.computeHash();
    }
  }

  protected static class UnionSchema extends Schema {

    private final List<Schema> types;
    private final Map<String, Integer> indexByName;

    public UnionSchema(LockableArrayList<Schema> types) {
      super(Type.UNION);
      this.indexByName = new HashMap<>(Math.multiplyExact(2, types.size()));
      this.types = types.lock();
      int index = 0;
      for (Schema type : types) {
        if (type.getType() == Type.UNION) {
          throw new SchemaRuntimeException("Nested union: " + this);
        }
        String name = type.getFullName();
        if (name == null) {
          throw new SchemaRuntimeException("Nameless in union:" + this);
        }
        if (indexByName.put(name, index++) != null) {
          throw new SchemaRuntimeException("Duplicate in union:" + name);
        }
      }
    }

    /**
     * Checks if a JSON value matches the schema.
     *
     * @param jsonValue a value to check against the schema
     * @return true if the value is valid according to this schema
     */
    public boolean isValidDefault(JsonNode jsonValue) {
      return this.types.stream().anyMatch((Schema s) -> s.isValidDefault(jsonValue));
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("types")
    public List<Schema> getTypes() {
      return types;
    }

    @Override
    public Integer getIndexNamed(String name) {
      return indexByName.get(name);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof UnionSchema)) {
        return false;
      }
      UnionSchema that = (UnionSchema) o;
      return equalCachedHash(that) && types.equals(that.types) && propsEqual(that);
    }

    @Override
    int computeHash() {
      int hash = super.computeHash();
      for (Schema type : types) {
        hash += type.computeHash();
      }
      return hash;
    }

    @Override
    public void addProp(String name, String value) {
      throw new SchemaRuntimeException("Can't set properties on a union: " + this);
    }

    @Override
    @JsonIgnore
    public String getName() {
      return super.getName()
          + this.getTypes().stream().map(Schema::getName)
          .collect(Collectors.joining(", ", "[", "]"));
    }
  }

  protected static class FixedBinarySchema extends Schema {

    private final int size;

    public FixedBinarySchema(int size) {
      super(Type.BINARY);
      SystemLimitException.checkMaxBytesLength(size);
      this.size = size;
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("size")
    public int getFixedSize() {
      return size;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof FixedBinarySchema)) {
        return false;
      }
      FixedBinarySchema that = (FixedBinarySchema) o;
      return equalCachedHash(that) && size == that.size && propsEqual(that);
    }

    @Override
    int computeHash() {
      return super.computeHash() + size;
    }
  }

  protected static class FixedCharSchema extends Schema {

    private final int size;

    public FixedCharSchema(int size) {
      super(Type.CHAR);
      SystemLimitException.checkMaxBytesLength(size);
      this.size = size;
    }

    @Override
    @JsonIgnore(false)
    @JsonProperty("size")
    public int getFixedSize() {
      return size;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof FixedCharSchema)) {
        return false;
      }
      FixedCharSchema that = (FixedCharSchema) o;
      return equalCachedHash(that) && size == that.size && propsEqual(that);
    }

    @Override
    int computeHash() {
      return super.computeHash() + size;
    }
  }

  protected static class StringSchema extends Schema {

    public StringSchema() {
      super(Type.STRING);
    }
  }

  protected static class BytesSchema extends Schema {

    public BytesSchema() {
      super(Type.BYTES);
    }
  }

  protected static class ByteSchema extends Schema {

    public ByteSchema() {
      super(Type.INT8);
    }
  }

  protected static class ShortSchema extends Schema {

    public ShortSchema() {
      super(Type.INT16);
    }
  }

  protected static class IntSchema extends Schema {

    public IntSchema() {
      super(Type.INT32);
    }
  }

  protected static class LongSchema extends Schema {

    public LongSchema() {
      super(Type.INT64);
    }
  }

  protected static class FloatSchema extends Schema {

    public FloatSchema() {
      super(Type.FLOAT32);
    }
  }

  protected static class DoubleSchema extends Schema {

    public DoubleSchema() {
      super(Type.FLOAT64);
    }
  }

  protected static class BooleanSchema extends Schema {

    public BooleanSchema() {
      super(Type.BOOLEAN);
    }
  }

  protected static class NullSchema extends Schema {

    public NullSchema() {
      super(Type.NULL);
    }
  }

  static final Map<String, Type> PRIMITIVES = new HashMap<>();

  static {
    PRIMITIVES.put("string", Type.STRING);
    PRIMITIVES.put("bytes", Type.BYTES);
    PRIMITIVES.put("int8", Type.INT8);
    PRIMITIVES.put("int16", Type.INT16);
    PRIMITIVES.put("int32", Type.INT32);
    PRIMITIVES.put("int64", Type.INT64);
    PRIMITIVES.put("float32", Type.FLOAT32);
    PRIMITIVES.put("float64", Type.FLOAT64);
    PRIMITIVES.put("boolean", Type.BOOLEAN);
    PRIMITIVES.put("null", Type.NULL);
  }

  static class Names extends LinkedHashMap<Name, Schema> {

    private static final long serialVersionUID = 1L;
    private String space; // default namespace

    public Names() {
    }

    public Names(String space) {
      this.space = space;
    }

    public String space() {
      return space;
    }

    public void space(String space) {
      this.space = space;
    }

    public Schema get(String o) {
      Type primitive = PRIMITIVES.get(o);
      if (primitive != null) {
        return Schema.create(primitive);
      }
      Name name = new Name(o, space);
      if (!containsKey(name)) {
        // if not in default try anonymous
        name = new Name(o, "");
      }
      return super.get(name);
    }

    public boolean contains(Schema schema) {
      return get(((NamedSchema) schema).name) != null;
    }

    public void add(Schema schema) {
      put(((NamedSchema) schema).name, schema);
    }

    @Override
    public Schema put(Name name, Schema schema) {
      if (containsKey(name)) {
        final Schema other = super.get(name);
        if (!Objects.equals(other, schema)) {
          throw new SchemaParseException("Can't redefine: " + name);
        } else {
          return schema;
        }
      }
      return super.put(name, schema);
    }
  }

  private static final ThreadLocal<NameValidator> VALIDATE_NAMES = ThreadLocalWithInitial
      .of(() -> NameValidator.UTF_VALIDATOR);

  private static String validateName(String name) {
    NameValidator.Result result = VALIDATE_NAMES.get().validate(name);
    if (!result.isOK()) {
      throw new SchemaParseException(result.getErrors());
    }
    return name;
  }

  private static final ThreadLocal<Boolean> VALIDATE_DEFAULTS = ThreadLocalWithInitial.of(
      () -> true);

  private static JsonNode validateDefault(String fieldName, Schema schema, JsonNode defaultValue) {
    if (VALIDATE_DEFAULTS.get() && (defaultValue != null) && !schema.isValidDefault(
        defaultValue)) { // invalid default
      String message =
          "Invalid default for field " + fieldName + ": " + defaultValue + " not a " + schema;
      throw new SchemaTypeException(message); // throw exception
    }
    return defaultValue;
  }

  /**
   * Checks if a JSON value matches the schema.
   *
   * @param jsonValue a value to check against the schema
   * @return true if the value is valid according to this schema
   */
  public boolean isValidDefault(JsonNode jsonValue) {
    return isValidDefault(this, jsonValue);
  }

  private static boolean isValidDefault(Schema schema, JsonNode defaultValue) {
    if (defaultValue == null) {
      return false;
    }
    switch (schema.getType()) {
      case BINARY:
      case BYTES:
      case CHAR:
      case STRING:
      case ENUM:
        return defaultValue.isTextual();
      case INT8:
      case INT16:
      case INT32:
        return defaultValue.isIntegralNumber() && defaultValue.canConvertToInt();
      case INT64:
        return defaultValue.isIntegralNumber() && defaultValue.canConvertToLong();
      case FLOAT32:
      case FLOAT64:
        return defaultValue.isNumber();
      case BOOLEAN:
        return defaultValue.isBoolean();
      case NULL:
        return defaultValue.isNull();
      case ARRAY:
        if (!defaultValue.isArray()) {
          return false;
        }
        for (JsonNode element : defaultValue) {
          if (!isValidDefault(schema.getElementType(), element)) {
            return false;
          }
        }
        return true;
      case MAP:
        if (!defaultValue.isObject()) {
          return false;
        }
        for (JsonNode value : defaultValue) {
          if (!isValidDefault(schema.getValueType(), value)) {
            return false;
          }
        }
        return true;
      case UNION: // union default: any branch
        return schema.getTypes().stream().anyMatch((Schema s) -> isValidValue(s, defaultValue));
      case STRUCT:
        if (!defaultValue.isObject()) {
          return false;
        }
        for (Field field : schema.getFields()) {
          if (!isValidValue(field.schema(),
              defaultValue.has(field.name()) ? defaultValue.get(field.name())
                  : field.defaultValue())) {
            return false;
          }
        }
        return true;
      default:
        return false;
    }
  }

  /**
   * Validate a value against the schema.
   *
   * @param schema : schema for value.
   * @param value  : value to validate.
   * @return true if ok.
   */
  private static boolean isValidValue(Schema schema, JsonNode value) {
    if (value == null) {
      return false;
    }
    if (schema.isUnion()) {
      // For Union, only need that one sub schema is ok.
      for (Schema sub : schema.getTypes()) {
        if (Schema.isValidDefault(sub, value)) {
          return true;
        }
      }
      return false;
    } else {
      // for other types, same as validate default.
      return Schema.isValidDefault(schema, value);
    }
  }

  /**
   * Rewrite a writer's schema using the aliases from a reader's schema. This permits reading
   * structs and enums schemas whose names have changed, and structs whose field names have changed.
   * The returned schema always contains the same data elements in the same order, but with possibly
   * different names.
   */
  public static Schema applyAliases(Schema writer, Schema reader) {
    if (writer.equals(reader)) {
      return writer; // same schema
    }

    // create indexes of names
    Map<Schema, Schema> seen = new IdentityHashMap<>(1);
    Map<Name, Name> aliases = new HashMap<>(1);
    Map<Name, Map<String, String>> fieldAliases = new HashMap<>(1);
    getAllAliases(reader, seen, aliases, fieldAliases);

    if (aliases.isEmpty() && fieldAliases.isEmpty()) {
      return writer; // no aliases
    }

    seen.clear();
    return applyAliases(writer, seen, aliases, fieldAliases);
  }

  @SuppressWarnings("DataFlowIssue")
  private static Schema applyAliases(Schema s, Map<Schema, Schema> seen, Map<Name, Name> aliases,
      Map<Name, Map<String, String>> fieldAliases) {

    Name name = s instanceof NamedSchema ? ((NamedSchema) s).name : null;
    Schema result = s;
    switch (s.getType()) {
      case STRUCT:
        if (seen.containsKey(s)) {
          return seen.get(s); // break loops
        }
        if (aliases.containsKey(name)) {
          name = aliases.get(name);
        }
        result = Schema.createStruct(name.full, s.getDoc(), null, s.isError());
        seen.put(s, result);
        List<Field> newFields = new ArrayList<>();
        for (Field f : s.getFields()) {
          Schema fieldSchema = applyAliases(f.schema, seen, aliases, fieldAliases);
          String fieldName = getFieldAlias(name, f.name, fieldAliases);
          Field newF = new Field(fieldName, fieldSchema, f.doc, f.defaultValue, true, f.order);
          newF.putAll(f); // copy props
          newFields.add(newF);
        }
        result.setFields(newFields);
        break;
      case ENUM:
        if (aliases.containsKey(name)) {
          result = Schema.createEnum(aliases.get(name).full, s.getDoc(), null, s.getEnumSymbols(),
              s.getEnumDefault());
        }
        break;
      case ARRAY:
        Schema e = applyAliases(s.getElementType(), seen, aliases, fieldAliases);
        if (!e.equals(s.getElementType())) {
          result = Schema.createArray(e);
        }
        break;
      case MAP:
        Schema k = applyAliases(s.getKeyType(), seen, aliases, fieldAliases);
        Schema v = applyAliases(s.getValueType(), seen, aliases, fieldAliases);
        if (!k.equals(s.getKeyType()) && !v.equals(s.getValueType())) {
          result = Schema.createMap(k, v);
        }
        break;
      case UNION:
        List<Schema> types = new ArrayList<>();
        for (Schema branch : s.getTypes()) {
          types.add(applyAliases(branch, seen, aliases, fieldAliases));
        }
        result = Schema.createUnion(types);
        break;
      case BINARY:
        if (aliases.containsKey(name)) {
          result = Schema.createFixedBinary(s.getFixedSize());
        }
        break;
      case CHAR:
        if (aliases.containsKey(name)) {
          result = Schema.createFixedChar(s.getFixedSize());
        }
        break;
      default:
        // NO-OP
    }
    if (!result.equals(s)) {
      result.putAll(s); // copy props
    }
    return result;
  }

  @SuppressWarnings("DataFlowIssue")
  private static void getAllAliases(Schema schema, Map<Schema, Schema> seen,
      Map<Name, Name> aliases, Map<Name, Map<String, String>> fieldAliases) {
    if (schema instanceof NamedSchema) {
      NamedSchema namedSchema = (NamedSchema) schema;
      if (namedSchema.aliases != null) {
        for (Name alias : namedSchema.aliases) {
          aliases.put(alias, namedSchema.name);
        }
      }
    }
    switch (schema.getType()) {
      case STRUCT:
        if (seen.containsKey(schema)) {
          return; // break loops
        }
        seen.put(schema, schema);
        StructSchema struct = (StructSchema) schema;
        for (Field field : schema.getFields()) {
          if (field.aliases != null) {
            for (String fieldAlias : field.aliases) {
              Map<String, String> structAliases = fieldAliases.computeIfAbsent(struct.name,
                  k -> new HashMap<>());
              structAliases.put(fieldAlias, field.name);
            }
          }
          getAllAliases(field.schema, seen, aliases, fieldAliases);
        }
        if (struct.aliases != null && fieldAliases.containsKey(struct.name)) {
          for (Name structAlias : struct.aliases) {
            fieldAliases.put(structAlias, fieldAliases.get(struct.name));
          }
        }
        break;
      case ARRAY:
        getAllAliases(schema.getElementType(), seen, aliases, fieldAliases);
        break;
      case MAP:
        getAllAliases(schema.getValueType(), seen, aliases, fieldAliases);
        break;
      case UNION:
        for (Schema s : schema.getTypes()) {
          getAllAliases(s, seen, aliases, fieldAliases);
        }
        break;
      default:
        break;
    }
  }

  private static String getFieldAlias(Name struct, String field,
      Map<Name, Map<String, String>> fieldAliases) {
    Map<String, String> structAliases = fieldAliases.get(struct);
    if (structAliases == null) {
      return field;
    }
    String alias = structAliases.get(field);
    if (alias == null) {
      return field;
    }
    return alias;
  }

  /**
   * No change is permitted on LockableArrayList once lock() has been called on it.
   *
   * <p/>
   * This class keeps a boolean variable <tt>locked</tt> which is set to
   * <tt>true</tt> in the lock() method. It's legal to call lock() any number of
   * times. Any lock() other than the first one is a no-op.
   *
   * <p/>
   * If a mutating operation is performed after being locked, it throws an
   * <tt>IllegalStateException</tt>. Since modifications through iterator also use
   * the list's mutating operations, this effectively blocks all modifications.
   */
  @SuppressWarnings("unused")
  static class LockableArrayList<E> extends ArrayList<E> {

    private static final long serialVersionUID = 1L;
    private boolean locked = false;

    public LockableArrayList() {
    }

    public LockableArrayList(int size) {
      super(size);
    }

    public LockableArrayList(List<E> types) {
      super(types);
    }

    @SafeVarargs
    public LockableArrayList(E... types) {
      super(types.length);
      Collections.addAll(this, types);
    }

    public List<E> lock() {
      locked = true;
      return this;
    }

    private void ensureUnlocked() {
      if (locked) {
        throw new IllegalStateException();
      }
    }

    @Override
    public boolean add(E e) {
      ensureUnlocked();
      return super.add(e);
    }

    @Override
    public boolean remove(Object o) {
      ensureUnlocked();
      return super.remove(o);
    }

    @Override
    public E remove(int index) {
      ensureUnlocked();
      return super.remove(index);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
      ensureUnlocked();
      return super.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
      ensureUnlocked();
      return super.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      ensureUnlocked();
      return super.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      ensureUnlocked();
      return super.retainAll(c);
    }

    @Override
    public void clear() {
      ensureUnlocked();
      super.clear();
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  public static class SchemaSerializer extends StdSerializer<Schema> {

    public SchemaSerializer() {
      this(null);
    }

    public SchemaSerializer(Class<Schema> t) {
      super(t);
    }

    @Override
    public void serialize(
        Schema value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException, JsonProcessingException {
    }
  }
}
