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

package io.confluent.kafka.schemaregistry.type;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariantArrayBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TestVariantArrayBuilder.class);

  @Test
  public void testEmptyArrayBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder a = b.startArray();
    b.endArray();
    VariantTestUtils.testVariant(b.build(), v -> {
      VariantTestUtils.checkType(v, VariantFormat.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, v.numArrayElements());
    });
  }

  @Test
  public void testLargeArraySizeBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder a = b.startArray();
    for (int i = 0; i < 511; i++) {
      a.appendInt(i);
    }
    b.endArray();
    VariantTestUtils.testVariant(b.build(), v -> {
      VariantTestUtils.checkType(v, VariantFormat.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(511, v.numArrayElements());
      for (int i = 0; i < 511; i++) {
        VariantTestUtils.checkType(v.getElementAtIndex(i), VariantFormat.PRIMITIVE, Variant.Type.INT);
        Assert.assertEquals(i, v.getElementAtIndex(i).getInt());
      }
    });
  }

  @Test
  public void testMixedArrayBuilder() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arrBuilder = b.startArray();
    arrBuilder.appendBoolean(true);
    VariantObjectBuilder obj = arrBuilder.startObject();
    obj.appendKey("key");
    obj.appendInt(321);
    arrBuilder.endObject();
    arrBuilder.appendLong(1234567890);
    {
      // build a nested array
      VariantArrayBuilder nestedBuilder = arrBuilder.startArray();
      {
        // build a nested empty array
        nestedBuilder.startArray();
        nestedBuilder.endArray();
      }
      nestedBuilder.appendString("variant");
      nestedBuilder.startObject();
      nestedBuilder.endObject();
      arrBuilder.endArray();
    }
    b.endArray();

    VariantTestUtils.testVariant(b.build(), v -> {
      VariantTestUtils.checkType(v, VariantFormat.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(4, v.numArrayElements());
      VariantTestUtils.checkType(v.getElementAtIndex(0), VariantFormat.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getElementAtIndex(0).getBoolean());

      VariantTestUtils.checkType(v.getElementAtIndex(1), VariantFormat.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(1, v.getElementAtIndex(1).numObjectElements());
      VariantTestUtils.checkType(
          v.getElementAtIndex(1).getFieldByKey("key"), VariantFormat.PRIMITIVE, Variant.Type.INT);
      Assert.assertEquals(321, v.getElementAtIndex(1).getFieldByKey("key").getInt());

      VariantTestUtils.checkType(v.getElementAtIndex(2), VariantFormat.PRIMITIVE, Variant.Type.LONG);
      Assert.assertEquals(1234567890, v.getElementAtIndex(2).getLong());

      VariantTestUtils.checkType(v.getElementAtIndex(3), VariantFormat.ARRAY, Variant.Type.ARRAY);
      Variant nested = v.getElementAtIndex(3);
      Assert.assertEquals(3, nested.numArrayElements());
      VariantTestUtils.checkType(nested.getElementAtIndex(0), VariantFormat.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(0, nested.getElementAtIndex(0).numArrayElements());
      VariantTestUtils.checkType(nested.getElementAtIndex(1), VariantFormat.SHORT_STR, Variant.Type.STRING);
      Assert.assertEquals("variant", nested.getElementAtIndex(1).getString());
      VariantTestUtils.checkType(nested.getElementAtIndex(2), VariantFormat.OBJECT, Variant.Type.OBJECT);
      Assert.assertEquals(0, nested.getElementAtIndex(2).numObjectElements());
    });
  }

  private void buildNested(int i, VariantArrayBuilder obj) {
    if (i > 0) {
      obj.appendString("str" + i);
      buildNested(i - 1, obj.startArray());
      obj.endArray();
    }
  }

  @Test
  public void testNestedBuilder() {
    VariantBuilder b = new VariantBuilder();
    buildNested(1000, b.startArray());
    b.endArray();

    VariantTestUtils.testVariant(b.build(), v -> {
      Variant curr = v;
      for (int i = 1000; i >= 0; i--) {
        VariantTestUtils.checkType(curr, VariantFormat.ARRAY, Variant.Type.ARRAY);
        if (i == 0) {
          Assert.assertEquals(0, curr.numArrayElements());
        } else {
          Assert.assertEquals(2, curr.numArrayElements());
          VariantTestUtils.checkType(curr.getElementAtIndex(0), VariantFormat.SHORT_STR, Variant.Type.STRING);
          Assert.assertEquals("str" + i, curr.getElementAtIndex(0).getString());
          curr = curr.getElementAtIndex(1);
        }
      }
    });
  }

  private void testArrayOffsetSizeBuilder(String randomString) {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arrBuilder = b.startArray();
    arrBuilder.appendString(randomString);
    arrBuilder.appendBoolean(true);
    arrBuilder.appendLong(1234567890);
    b.endArray();

    VariantTestUtils.testVariant(b.build(), v -> {
      VariantTestUtils.checkType(v, VariantFormat.ARRAY, Variant.Type.ARRAY);
      Assert.assertEquals(3, v.numArrayElements());
      VariantTestUtils.checkType(v.getElementAtIndex(0), VariantFormat.PRIMITIVE, Variant.Type.STRING);
      Assert.assertEquals(randomString, v.getElementAtIndex(0).getString());
      VariantTestUtils.checkType(v.getElementAtIndex(1), VariantFormat.PRIMITIVE, Variant.Type.BOOLEAN);
      Assert.assertTrue(v.getElementAtIndex(1).getBoolean());
      VariantTestUtils.checkType(v.getElementAtIndex(2), VariantFormat.PRIMITIVE, Variant.Type.LONG);
      Assert.assertEquals(1234567890, v.getElementAtIndex(2).getLong());
    });
  }

  @Test
  public void testArrayTwoByteOffsetBuilder() {
    // a string larger than 255 bytes to push the value offset size above 1 byte
    testArrayOffsetSizeBuilder(VariantTestUtils.randomString(300));
  }

  @Test
  public void testArrayThreeByteOffsetBuilder() {
    // a string larger than 65535 bytes to push the value offset size above 2 bytes
    testArrayOffsetSizeBuilder(VariantTestUtils.randomString(70_000));
  }

  @Test
  public void testArrayFourByteOffsetBuilder() {
    // a string larger than 16777215 bytes to push the value offset size above 3 bytes
    testArrayOffsetSizeBuilder(VariantTestUtils.randomString(16_800_000));
  }

  @Test
  public void testMissingEndArray() {
    VariantBuilder b = new VariantBuilder();
    b.startArray();
    try {
      b.build();
      Assert.fail("Expected Exception when calling build() without endArray()");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testMissingStartArray() {
    VariantBuilder b = new VariantBuilder();
    try {
      b.endArray();
      Assert.fail("Expected Exception when calling endArray() without startArray()");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testInvalidAppendDuringArray() {
    VariantBuilder b = new VariantBuilder();
    b.startArray();
    try {
      b.appendInt(1);
      Assert.fail("Expected Exception when calling append() before endArray()");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testStartArrayEndObject() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder obj = b.startArray();
    try {
      obj.endObject();
      Assert.fail("Expected Exception when calling endObject() while building array");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedObject() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    arr.startObject();
    try {
      b.endArray();
      Assert.fail("Expected Exception when calling endArray() with an open nested object");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedObjectWithKey() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    VariantObjectBuilder nested = arr.startObject();
    nested.appendKey("nested");
    try {
      b.endArray();
      Assert.fail("Expected Exception when calling endArray() with an open nested object");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedObjectWithKeyValue() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    VariantObjectBuilder nested = arr.startObject();
    nested.appendKey("nested");
    nested.appendInt(1);
    try {
      b.endArray();
      Assert.fail("Expected Exception when calling endArray() with an open nested object");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedArray() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    arr.startArray();
    try {
      b.endArray();
      Assert.fail("Expected Exception when calling endArray() with an open nested array");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testOpenNestedArrayWithElement() {
    VariantBuilder b = new VariantBuilder();
    VariantArrayBuilder arr = b.startArray();
    VariantArrayBuilder nested = arr.startArray();
    nested.appendInt(1);
    try {
      b.endArray();
      Assert.fail("Expected Exception when calling endArray() with an open nested array");
    } catch (Exception e) {
      // expected
    }
  }
}
