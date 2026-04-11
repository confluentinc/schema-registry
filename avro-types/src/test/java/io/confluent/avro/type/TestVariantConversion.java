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

package io.confluent.avro.type;

import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantArrayBuilder;
import io.confluent.kafka.schemaregistry.type.VariantBuilder;
import io.confluent.kafka.schemaregistry.type.VariantObjectBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Assert;
import org.junit.Test;

public class TestVariantConversion {

  private final VariantConversion conversion = new VariantConversion();

  // -- VariantLogicalType tests --

  @Test
  public void testValidVariantSchema() {
    Schema schema = createVariantSchema("test");
    // Should not throw
    VariantLogicalType.get().validate(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSchemaWrongFieldCount() {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("metadata", Schema.create(Schema.Type.BYTES)));
    Schema schema = Schema.createRecord("bad", null, null, false, fields);
    VariantLogicalType.get().validate(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSchemaWrongFieldType() {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("metadata", Schema.create(Schema.Type.STRING)));
    fields.add(new Schema.Field("value", Schema.create(Schema.Type.BYTES)));
    Schema schema = Schema.createRecord("bad", null, null, false, fields);
    VariantLogicalType.get().validate(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSchemaWrongFieldNames() {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("meta", Schema.create(Schema.Type.BYTES)));
    fields.add(new Schema.Field("val", Schema.create(Schema.Type.BYTES)));
    Schema schema = Schema.createRecord("bad", null, null, false, fields);
    VariantLogicalType.get().validate(schema);
  }

  @Test
  public void testIsVariantSchema() {
    Assert.assertTrue(VariantLogicalType.isVariantSchema(createVariantSchema("v")));
    Assert.assertFalse(VariantLogicalType.isVariantSchema(Schema.create(Schema.Type.STRING)));
  }

  // -- VariantConversion tests --

  @Test
  public void testGetConvertedType() {
    Assert.assertEquals(Variant.class, conversion.getConvertedType());
  }

  @Test
  public void testGetLogicalTypeName() {
    Assert.assertEquals("variant", conversion.getLogicalTypeName());
  }

  @Test
  public void testRecommendedSchema() {
    Schema schema = conversion.getRecommendedSchema();
    Assert.assertEquals(VariantConversion.RECORD_NAME, schema.getFullName());
    Assert.assertTrue(VariantLogicalType.isVariantSchema(schema));
    Assert.assertNotNull(schema.getLogicalType());
    Assert.assertEquals("variant", schema.getLogicalType().getName());
  }

  @Test
  public void testRoundTripScalar() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendString("hello");
    Variant original = builder.build();

    Schema schema = conversion.getRecommendedSchema();
    IndexedRecord record = conversion.toRecord(original, schema, null);
    Variant decoded = conversion.fromRecord(record, schema, null);

    Assert.assertEquals(original.getString(), decoded.getString());
  }

  @Test
  public void testRoundTripInt() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendInt(42);
    Variant original = builder.build();

    Schema schema = conversion.getRecommendedSchema();
    IndexedRecord record = conversion.toRecord(original, schema, null);
    Variant decoded = conversion.fromRecord(record, schema, null);

    Assert.assertEquals(original.getInt(), decoded.getInt());
  }

  @Test
  public void testRoundTripNull() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendNull();
    Variant original = builder.build();

    Schema schema = conversion.getRecommendedSchema();
    IndexedRecord record = conversion.toRecord(original, schema, null);
    Variant decoded = conversion.fromRecord(record, schema, null);

    Assert.assertEquals(Variant.Type.NULL, decoded.getType());
  }

  @Test
  public void testRoundTripBoolean() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendBoolean(true);
    Variant original = builder.build();

    Schema schema = conversion.getRecommendedSchema();
    IndexedRecord record = conversion.toRecord(original, schema, null);
    Variant decoded = conversion.fromRecord(record, schema, null);

    Assert.assertTrue(decoded.getBoolean());
  }

  @Test
  public void testRoundTripObject() {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder obj = builder.startObject();
    obj.appendKey("name");
    obj.appendString("Alice");
    builder.endObject();
    Variant original = builder.build();

    Schema schema = conversion.getRecommendedSchema();
    IndexedRecord record = conversion.toRecord(original, schema, null);
    Variant decoded = conversion.fromRecord(record, schema, null);

    Assert.assertEquals(Variant.Type.OBJECT, decoded.getType());
    Assert.assertEquals(1, decoded.numObjectElements());
    Assert.assertEquals("Alice", decoded.getFieldByKey("name").getString());
  }

  @Test
  public void testRoundTripNestedObject() {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder outer = builder.startObject();
    outer.appendKey("address");
    VariantObjectBuilder inner = outer.startObject();
    inner.appendKey("city");
    inner.appendString("NY");
    outer.endObject();
    builder.endObject();
    Variant original = builder.build();

    Schema schema = conversion.getRecommendedSchema();
    IndexedRecord record = conversion.toRecord(original, schema, null);
    Variant decoded = conversion.fromRecord(record, schema, null);

    Assert.assertEquals("NY",
        decoded.getFieldByKey("address").getFieldByKey("city").getString());
  }

  @Test
  public void testRoundTripArray() {
    VariantBuilder builder = new VariantBuilder();
    VariantArrayBuilder arr = builder.startArray();
    arr.appendInt(1);
    arr.appendInt(2);
    arr.appendInt(3);
    builder.endArray();
    Variant original = builder.build();

    Schema schema = conversion.getRecommendedSchema();
    IndexedRecord record = conversion.toRecord(original, schema, null);
    Variant decoded = conversion.fromRecord(record, schema, null);

    Assert.assertEquals(Variant.Type.ARRAY, decoded.getType());
    Assert.assertEquals(3, decoded.numArrayElements());
    Assert.assertEquals(1, decoded.getElementAtIndex(0).getInt());
    Assert.assertEquals(2, decoded.getElementAtIndex(1).getInt());
    Assert.assertEquals(3, decoded.getElementAtIndex(2).getInt());
  }

  @Test
  public void testToRecordProducesCorrectFields() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendString("test");
    Variant variant = builder.build();

    Schema schema = conversion.getRecommendedSchema();
    IndexedRecord record = conversion.toRecord(variant, schema, null);

    Assert.assertTrue(record.get(0) instanceof ByteBuffer);
    Assert.assertTrue(record.get(1) instanceof ByteBuffer);
  }

  private static Schema createVariantSchema(String name) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("metadata", Schema.create(Schema.Type.BYTES)));
    fields.add(new Schema.Field("value", Schema.create(Schema.Type.BYTES)));
    return Schema.createRecord(name, null, null, false, fields);
  }
}
