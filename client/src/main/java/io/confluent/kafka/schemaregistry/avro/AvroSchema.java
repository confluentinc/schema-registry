/*
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.avro;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.Incompatibility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSchema implements ParsedSchema {

  private static final Logger log = LoggerFactory.getLogger(AvroSchema.class);

  public static final String TYPE = "AVRO";

  private final Schema schemaObj;
  private String canonicalString;
  private final Integer version;
  private final List<SchemaReference> references;
  private final Map<String, String> resolvedReferences;
  private final boolean isNew;

  private transient int hashCode = NO_HASHCODE;

  private static final int NO_HASHCODE = Integer.MIN_VALUE;

  public AvroSchema(String schemaString) {
    this(schemaString, Collections.emptyList(), Collections.emptyMap(), null);
  }

  public AvroSchema(String schemaString,
                    List<SchemaReference> references,
                    Map<String, String> resolvedReferences,
                    Integer version) {
    this(schemaString, references, resolvedReferences, version, false);
  }

  public AvroSchema(String schemaString,
                    List<SchemaReference> references,
                    Map<String, String> resolvedReferences,
                    Integer version,
                    boolean isNew) {
    this.isNew = isNew;
    Schema.Parser parser = getParser();
    for (String schema : resolvedReferences.values()) {
      parser.parse(schema);
    }
    this.schemaObj = parser.parse(schemaString);
    this.references = Collections.unmodifiableList(references);
    this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
    this.version = version;
  }

  public AvroSchema(Schema schemaObj) {
    this(schemaObj, null);
  }

  public AvroSchema(Schema schemaObj, Integer version) {
    this.isNew = false;
    this.schemaObj = schemaObj;
    this.references = Collections.emptyList();
    this.resolvedReferences = Collections.emptyMap();
    this.version = version;
  }

  private AvroSchema(
      Schema schemaObj,
      String canonicalString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Integer version,
      boolean isNew
  ) {
    this.isNew = isNew;
    this.schemaObj = schemaObj;
    this.canonicalString = canonicalString;
    this.references = references;
    this.resolvedReferences = resolvedReferences;
    this.version = version;
  }

  public AvroSchema copy() {
    return new AvroSchema(
        this.schemaObj,
        this.canonicalString,
        this.references,
        this.resolvedReferences,
        this.version,
        this.isNew
    );
  }

  protected Schema.Parser getParser() {
    Schema.Parser parser = new Schema.Parser();
    parser.setValidateDefaults(isNew());
    return parser;
  }

  @Override
  public Schema rawSchema() {
    return schemaObj;
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    if (schemaObj != null && schemaObj.getType() == Schema.Type.RECORD) {
      return schemaObj.getFullName();
    }
    return null;
  }

  @Override
  public String canonicalString() {
    if (schemaObj == null) {
      return null;
    }
    if (canonicalString == null) {
      Schema.Parser parser = getParser();
      List<Schema> schemaRefs = new ArrayList<>();
      for (String schema : resolvedReferences.values()) {
        Schema schemaRef = parser.parse(schema);
        schemaRefs.add(schemaRef);
      }
      canonicalString = schemaObj.toString(schemaRefs, false);
    }
    return canonicalString;
  }

  public Integer version() {
    return version;
  }

  @Override
  public List<SchemaReference> references() {
    return references;
  }

  public Map<String, String> resolvedReferences() {
    return resolvedReferences;
  }

  public boolean isNew() {
    return isNew;
  }

  @Override
  public AvroSchema normalize() {
    String normalized = AvroSchemaUtils.toNormalizedString(this);
    return new AvroSchema(
        normalized,
        this.references.stream().sorted().distinct().collect(Collectors.toList()),
        this.resolvedReferences,
        this.version,
        this.isNew
    );
  }

  @Override
  public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
    if (!schemaType().equals(previousSchema.schemaType())) {
      return Collections.singletonList("Incompatible because of different schema type");
    }
    try {
      SchemaCompatibility.SchemaPairCompatibility result =
          SchemaCompatibility.checkReaderWriterCompatibility(
              this.schemaObj,
              ((AvroSchema) previousSchema).schemaObj);
      return result.getResult().getIncompatibilities().stream()
          .map(Incompatibility::toString)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Unexpected exception during compatibility check", e);
      return Collections.singletonList(
              "Unexpected exception during compatibility check: " + e.getMessage());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AvroSchema that = (AvroSchema) o;
    return Objects.equals(version, that.version)
        && Objects.equals(references, that.references)
        && Objects.equals(schemaObj, that.schemaObj)
        && metaEqual(schemaObj, that.schemaObj, new HashMap<>());
  }

  private boolean metaEqual(
      Schema schema1, Schema schema2, Map<IdentityPair<Schema, Schema>, Boolean> cache) {
    if (schema1 == schema2) {
      return true;
    }

    if (schema1 == null || schema2 == null) {
      return false;
    }

    Schema.Type type1 = schema1.getType();
    Schema.Type type2 = schema2.getType();
    if (type1 != type2) {
      return false;
    }

    switch (type1) {
      case RECORD:
        // Add a temporary value to the cache to avoid cycles.
        // As long as we recurse only at the end of the method, we can safely default to true here.
        // The cache is updated at the end of the method with the actual comparison result.
        IdentityPair<Schema, Schema> sp = new IdentityPair<>(schema1, schema2);
        Boolean cacheHit = cache.putIfAbsent(sp, true);
        if (cacheHit != null) {
          return cacheHit;
        }

        boolean equals = Objects.equals(schema1.getAliases(), schema2.getAliases())
            && Objects.equals(schema1.getDoc(), schema2.getDoc())
            && fieldMetaEqual(schema1.getFields(), schema2.getFields(), cache);

        cache.put(sp, equals);
        return equals;
      case ENUM:
        return Objects.equals(schema1.getAliases(), schema2.getAliases())
            && Objects.equals(schema1.getDoc(), schema2.getDoc())
            && Objects.equals(schema1.getEnumDefault(), schema2.getEnumDefault());
      case FIXED:
        return Objects.equals(schema1.getAliases(), schema2.getAliases())
            && Objects.equals(schema1.getDoc(), schema2.getDoc());
      case UNION:
        List<Schema> types1 = schema1.getTypes();
        List<Schema> types2 = schema2.getTypes();
        if (types1.size() != types2.size()) {
          return false;
        }
        for (int i = 0; i < types1.size(); i++) {
          if (!metaEqual(types1.get(i), types2.get(i), cache)) {
            return false;
          }
        }
        return true;
      default:
        return true;
    }
  }

  private boolean fieldMetaEqual(
      List<Schema.Field> fields1,
      List<Schema.Field> fields2,
      Map<IdentityPair<Schema, Schema>, Boolean> cache) {
    if (fields1.size() != fields2.size()) {
      return false;
    }
    for (int i = 0; i < fields1.size(); i++) {
      Schema.Field field1 = fields1.get(i);
      Schema.Field field2 = fields2.get(i);
      if (field1 == field2) {
        continue;
      }
      if (!Objects.equals(field1.aliases(), field2.aliases())
          || !Objects.equals(field1.doc(), field2.doc())) {
        return false;
      }
      boolean fieldSchemaMetaEqual = metaEqual(field1.schema(), field2.schema(), cache);
      if (!fieldSchemaMetaEqual) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (hashCode == NO_HASHCODE) {
      hashCode = Objects.hash(schemaObj, references, version)
          + metaHash(schemaObj, new IdentityHashMap<>());
    }
    return hashCode;
  }

  private int metaHash(Schema schema, Map<Schema, Integer> cache) {
    if (schema == null) {
      return 0;
    }
    switch (schema.getType()) {
      case RECORD:
        // Add a temporary value to the cache to avoid cycles.
        // As long as we recurse only at the end of the method, we can safely default to 0 here.
        // The cache is updated at the end of the method with the actual comparison result.
        Integer cacheHit = cache.putIfAbsent(schema, 0);
        if (cacheHit != null) {
          return cacheHit;
        }

        int result = Objects.hash(schema.getAliases(), schema.getDoc())
            + fieldMetaHash(schema.getFields(), cache);

        cache.put(schema, result);
        return result;
      case ENUM:
        return Objects.hash(schema.getAliases(), schema.getDoc(), schema.getEnumDefault());
      case FIXED:
        return Objects.hash(schema.getAliases(), schema.getDoc());
      case UNION:
        int hash = 0;
        List<Schema> types = schema.getTypes();
        for (Schema type : types) {
          hash += metaHash(type, cache);
        }
        return hash;
      default:
        return 0;
    }
  }

  private int fieldMetaHash(List<Schema.Field> fields, Map<Schema, Integer> cache) {
    int hash = 0;
    for (Schema.Field field : fields) {
      hash += Objects.hash(field.aliases(), field.doc()) + metaHash(field.schema(), cache);
    }
    return hash;
  }

  @Override
  public String toString() {
    return canonicalString();
  }

  static class IdentityPair<K, V> {
    private final K key;
    private final V value;

    public IdentityPair(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IdentityPair<?, ?> pair = (IdentityPair<?, ?>) o;
      // Only perform identity check
      return key == pair.key && value == pair.value;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(key) + System.identityHashCode(value);
    }

    @Override
    public String toString() {
      return "IdentityPair{"
          + "key=" + key
          + ", value=" + value
          + '}';
    }
  }
}
