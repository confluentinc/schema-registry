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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.math.BigDecimal;

import org.junit.Test;

/**
 * Variant equality is over the encoding, not the object and not the value. The contract is
 * shared with the other six clients, so each case here has a twin in each of them.
 */
public class TestVariantEquality {

  private static Variant parse(String json) {
    return VariantUtils.fromJson(json);
  }

  /** The point of having equals() at all: two separately built variants are not two values. */
  @Test
  public void equalBytesAreEqualHoweverTheyWereBuilt() {
    assertEquals(parse("{\"name\":\"alice\"}"), parse("{\"name\":\"alice\"}"));
    assertEquals(parse("{\"name\":\"alice\"}").hashCode(), parse("{\"name\":\"alice\"}").hashCode());
    assertEquals(parse("1"), parse("1"));
    assertEquals(parse("[1,2,3]"), parse("[1,2,3]"));
    assertEquals(parse("null"), parse("null"));
  }

  @Test
  public void differentValuesAreUnequal() {
    assertNotEquals(parse("{\"name\":\"alice\"}"), parse("{\"name\":\"bob\"}"));
    assertNotEquals(parse("1"), parse("2"));
    assertNotEquals(parse("[1,2,3]"), parse("[1,2]"));
    assertNotEquals(parse("null"), parse("0"));
    assertNotEquals(parse("1"), "1");
    assertNotEquals(parse("1"), null);
  }

  private static Variant built(java.util.function.Consumer<VariantBuilder> append) {
    VariantBuilder builder = new VariantBuilder();
    append.accept(builder);
    return builder.build();
  }

  /**
   * The documented incompleteness: one value has many encodings, and byte equality separates
   * them. Sound (equal bytes mean equal values), not complete.
   */
  @Test
  public void oneValueInTwoEncodingsIsUnequal() {
    // The scale is part of the decimal encoding: 2.50 is unscaled 250 at scale 2, 2.5 is
    // unscaled 25 at scale 1.
    assertNotEquals(built(b -> b.appendDecimal(new BigDecimal("2.50"))),
        built(b -> b.appendDecimal(new BigDecimal("2.5"))));
    // The width is part of the integer encoding: the same 1 as int8 and as int32.
    assertNotEquals(built(b -> b.appendByte((byte) 1)), built(b -> b.appendInt(1)));
    // An int and a double are different encodings too - which is what JSON gives, since
    // fromJson reads any fractional number as a double.
    assertNotEquals(parse("1"), parse("1.0"));
    // Two decimals JSON cannot tell apart, because both parse to the same double.
    assertEquals(parse("12.34"), parse("12.340"));
  }

  /**
   * Metadata is compared too, and a navigated variant carries its parent's whole dictionary -
   * so a field holding 1 is not the standalone variant 1 even though their value bytes match.
   * This is why trimming the value to its own length would not make navigation results
   * comparable across documents.
   */
  @Test
  public void theMetadataDictionaryIsPartOfTheComparison() {
    Variant navigated = parse("{\"a\":1}").getFieldByKey("a");
    assertEquals("the value bytes do match", parse("1").getValueBuffer(),
        navigated.getValueBuffer());
    assertNotEquals(parse("1"), navigated);
  }

  /**
   * A navigated variant's buffer starts at the value and runs to the end of the parent, so it
   * equals a sibling reached the same way and nothing else. ByteBuffer.equals compares the bytes
   * remaining from the position, which is what makes the first case work at all.
   */
  @Test
  public void navigationIsEqualOnlyWithinTheSameParent() {
    Variant a = parse("{\"x\":{\"k\":1},\"y\":{\"k\":1}}");
    assertEquals(a.getFieldByKey("x"), a.getFieldByKey("x"));
    Variant b = parse("{\"x\":{\"k\":1},\"y\":{\"k\":1}}");
    assertEquals("two parses of the same document are byte-identical throughout",
        a.getFieldByKey("x"), b.getFieldByKey("x"));
    // x and y hold the same value but sit at different offsets, so different bytes follow them.
    assertNotEquals(a.getFieldByKey("x"), a.getFieldByKey("y"));
  }
}
