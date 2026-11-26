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

package io.confluent.kafka.schemaregistry.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantUtils;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * {@link AvroSchemaUtils#toVariants}, which converts the raw variant records left by a reader
 * that defers the variant conversion until the READ rules have run.
 */
public class AvroSchemaUtilsVariantTest {

  private static final String VARIANT_SCHEMA =
      "{\"type\": \"record\", \"name\": \"confluent.type.Variant\", "
          + "\"logicalType\": \"variant\", \"fields\": ["
          + "{\"name\": \"metadata\", \"type\": \"bytes\"},"
          + "{\"name\": \"value\", \"type\": \"bytes\"}]}";

  private static Variant newVariant() {
    return VariantUtils.fromJson("{\"name\":\"alice\"}");
  }

  private static Schema docSchema(String fieldType) {
    return new AvroSchema("{\"type\": \"record\", \"name\": \"Doc\", \"fields\": ["
        + "{\"name\": \"f\", \"type\": " + fieldType + "}]}").rawSchema();
  }

  private static Schema variantSchema(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
        return "Variant".equals(schema.getName())
            ? schema : variantSchema(schema.getFields().get(1).schema());
      case ARRAY:
        return variantSchema(schema.getElementType());
      default:
        return variantSchema(schema.getTypes().get(schema.getTypes().size() - 1));
    }
  }

  private static GenericRecord rawVariant(Schema docSchema) {
    Variant variant = newVariant();
    GenericRecord record =
        new GenericData.Record(variantSchema(docSchema.getField("f").schema()));
    record.put("metadata", variant.getMetadataBuffer().duplicate());
    record.put("value", variant.getValueBuffer().duplicate());
    return record;
  }

  private static Object convert(Schema docSchema, Object fieldValue) {
    GenericRecord doc = new GenericData.Record(docSchema);
    doc.put("f", fieldValue);
    return ((GenericRecord) AvroSchemaUtils.toVariants(docSchema, doc)).get("f");
  }

  private static final String LOGICAL_MAP = "{\"type\": \"array\", \"logicalType\": \"map\", "
      + "\"items\": {\"type\": \"record\", \"name\": \"Entry\", \"fields\": ["
      + "{\"name\": \"key\", \"type\": \"string\"},"
      + "{\"name\": \"value\", \"type\": " + VARIANT_SCHEMA + "}]}}";

  @Test
  public void testLogicalMapValues() {
    // With converters on, a logical-type map is decoded to a java Map
    Schema schema = docSchema(LOGICAL_MAP);
    Map<Object, Object> map = new LinkedHashMap<>();
    map.put("k", rawVariant(schema));
    Object converted = convert(schema, map);
    assertSame(map, converted);
    assertEquals(newVariant(), map.get("k"));
  }

  @Test
  public void testNullableLogicalMapValues() {
    Schema schema = docSchema("[\"null\", " + LOGICAL_MAP + "]");
    Map<Object, Object> map = new LinkedHashMap<>();
    map.put("k", rawVariant(schema));
    assertSame(map, convert(schema, map));
    assertEquals(newVariant(), map.get("k"));
    assertNull(convert(schema, null));
  }

  @Test
  public void testUnionOfAvroMapAndLogicalMap() {
    // Both decode to a java Map, so a variant in the logical map is converted either way
    Schema schema = docSchema("[\"null\", {\"type\": \"map\", \"values\": \"string\"}, "
        + LOGICAL_MAP + "]");
    Map<Object, Object> logical = new LinkedHashMap<>();
    logical.put("k", rawVariant(schema));
    assertSame(logical, convert(schema, logical));
    assertEquals(newVariant(), logical.get("k"));
    Map<Object, Object> plain = new LinkedHashMap<>();
    plain.put("k", "text");
    assertSame(plain, convert(schema, plain));
    assertEquals("text", plain.get("k"));
  }

  @Test
  public void testUnionOfRawVariant() {
    Schema schema = docSchema("[\"null\", \"string\", " + VARIANT_SCHEMA + "]");
    assertEquals(newVariant(), convert(schema, rawVariant(schema)));
    assertEquals("text", convert(schema, "text"));
  }

  @Test
  public void testUnionOfVariantFromRule() {
    // A READ rule can already have produced a Variant, which is left as it is
    Schema schema = docSchema("[\"null\", \"string\", " + VARIANT_SCHEMA + "]");
    Variant variant = newVariant();
    assertSame(variant, convert(schema, variant));
  }
}
