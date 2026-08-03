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

package io.confluent.protobuf.type.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantBuilder;
import io.confluent.kafka.schemaregistry.type.VariantObjectBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestVariantUtils {

  // -- toKafkaVariant(Variant) and fromKafkaVariant round-trip --

  @Test
  public void testRoundTripProtoVariant() {
    VariantBuilder vb = new VariantBuilder();
    VariantObjectBuilder obj = vb.startObject();
    obj.appendKey("name");
    obj.appendString("Alice");
    obj.appendKey("age");
    obj.appendInt(30);
    vb.endObject();
    Variant original = vb.build();

    io.confluent.protobuf.type.Variant protoVariant = VariantUtils.fromKafkaVariant(original);
    Assert.assertFalse(protoVariant.getMetadata().isEmpty());
    Assert.assertFalse(protoVariant.getValue().isEmpty());

    Variant roundTripped = VariantUtils.toKafkaVariant(protoVariant);
    Assert.assertEquals(Variant.Type.OBJECT, roundTripped.getType());
    Assert.assertEquals("Alice", roundTripped.getFieldByKey("name").getString());
    Assert.assertEquals(30, roundTripped.getFieldByKey("age").getInt());
  }

  @Test
  public void testRoundTripScalar() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendString("hello");
    Variant original = vb.build();

    io.confluent.protobuf.type.Variant protoVariant = VariantUtils.fromKafkaVariant(original);
    Variant roundTripped = VariantUtils.toKafkaVariant(protoVariant);
    Assert.assertEquals("hello", roundTripped.getString());
  }

  // -- toKafkaVariant(Value): null --

  @Test
  public void testValueNull() {
    Value value = Value.newBuilder()
        .setNullValue(NullValue.NULL_VALUE)
        .build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(Variant.Type.NULL, variant.getType());
  }

  // -- toKafkaVariant(Value): number --

  @Test
  public void testValueNumber() {
    Value value = Value.newBuilder().setNumberValue(42.5).build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(Variant.Type.DOUBLE, variant.getType());
    Assert.assertEquals(42.5, variant.getDouble(), 0.0);
  }

  @Test
  public void testValueNumberInteger() {
    Value value = Value.newBuilder().setNumberValue(100.0).build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(100.0, variant.getDouble(), 0.0);
  }

  // -- toKafkaVariant(Value): string --

  @Test
  public void testValueString() {
    Value value = Value.newBuilder().setStringValue("hello").build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(Variant.Type.STRING, variant.getType());
    Assert.assertEquals("hello", variant.getString());
  }

  @Test
  public void testValueEmptyString() {
    Value value = Value.newBuilder().setStringValue("").build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals("", variant.getString());
  }

  // -- toKafkaVariant(Value): boolean --

  @Test
  public void testValueBoolTrue() {
    Value value = Value.newBuilder().setBoolValue(true).build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(Variant.Type.BOOLEAN, variant.getType());
    Assert.assertTrue(variant.getBoolean());
  }

  @Test
  public void testValueBoolFalse() {
    Value value = Value.newBuilder().setBoolValue(false).build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertFalse(variant.getBoolean());
  }

  // -- toKafkaVariant(Value): struct --

  @Test
  public void testValueEmptyStruct() {
    Value value = Value.newBuilder()
        .setStructValue(Struct.newBuilder())
        .build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(Variant.Type.OBJECT, variant.getType());
    Assert.assertEquals(0, variant.numObjectElements());
  }

  @Test
  public void testValueStruct() {
    Value value = Value.newBuilder()
        .setStructValue(Struct.newBuilder()
            .putFields("name", Value.newBuilder().setStringValue("Alice").build())
            .putFields("age", Value.newBuilder().setNumberValue(30).build()))
        .build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(Variant.Type.OBJECT, variant.getType());
    Assert.assertEquals("Alice", variant.getFieldByKey("name").getString());
    Assert.assertEquals(30.0, variant.getFieldByKey("age").getDouble(), 0.0);
  }

  // -- toKafkaVariant(Value): list --

  @Test
  public void testValueEmptyList() {
    Value value = Value.newBuilder()
        .setListValue(ListValue.newBuilder())
        .build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(Variant.Type.ARRAY, variant.getType());
    Assert.assertEquals(0, variant.numArrayElements());
  }

  @Test
  public void testValueList() {
    Value value = Value.newBuilder()
        .setListValue(ListValue.newBuilder()
            .addValues(Value.newBuilder().setNumberValue(1))
            .addValues(Value.newBuilder().setStringValue("two"))
            .addValues(Value.newBuilder().setBoolValue(true)))
        .build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(Variant.Type.ARRAY, variant.getType());
    Assert.assertEquals(3, variant.numArrayElements());
    Assert.assertEquals(1.0, variant.getElementAtIndex(0).getDouble(), 0.0);
    Assert.assertEquals("two", variant.getElementAtIndex(1).getString());
    Assert.assertTrue(variant.getElementAtIndex(2).getBoolean());
  }

  // -- toKafkaVariant(Value): nested --

  @Test
  public void testValueNestedStructWithList() {
    Value value = Value.newBuilder()
        .setStructValue(Struct.newBuilder()
            .putFields("items", Value.newBuilder()
                .setListValue(ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                        .putFields("id", Value.newBuilder().setNumberValue(1).build())))
                    .addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                        .putFields("id", Value.newBuilder().setNumberValue(2).build()))))
                .build()))
        .build();
    Variant variant = VariantUtils.toKafkaVariant(value);
    Assert.assertEquals(Variant.Type.OBJECT, variant.getType());
    Variant items = variant.getFieldByKey("items");
    Assert.assertEquals(Variant.Type.ARRAY, items.getType());
    Assert.assertEquals(2, items.numArrayElements());
    Assert.assertEquals(1.0, items.getElementAtIndex(0).getFieldByKey("id").getDouble(), 0.0);
    Assert.assertEquals(2.0, items.getElementAtIndex(1).getFieldByKey("id").getDouble(), 0.0);
  }

  // -- fromValue --

  @Test
  public void testFromValue() {
    Value value = Value.newBuilder()
        .setStructValue(Struct.newBuilder()
            .putFields("key", Value.newBuilder().setStringValue("val").build()))
        .build();
    io.confluent.protobuf.type.Variant protoVariant = VariantUtils.fromValue(value);
    Assert.assertFalse(protoVariant.getMetadata().isEmpty());
    Assert.assertFalse(protoVariant.getValue().isEmpty());

    // Verify by converting back
    Variant variant = VariantUtils.toKafkaVariant(protoVariant);
    Assert.assertEquals("val", variant.getFieldByKey("key").getString());
  }

  // -- toKafkaVariant(Message) --

  @Test
  public void testToKafkaVariantFromMessage() {
    VariantBuilder vb = new VariantBuilder();
    vb.appendInt(42);
    Variant original = vb.build();

    io.confluent.protobuf.type.Variant protoVariant = VariantUtils.fromKafkaVariant(original);

    // Pass as generic Message
    Variant fromMessage = VariantUtils.toKafkaVariant((com.google.protobuf.Message) protoVariant);
    Assert.assertEquals(42, fromMessage.getInt());
  }
}
