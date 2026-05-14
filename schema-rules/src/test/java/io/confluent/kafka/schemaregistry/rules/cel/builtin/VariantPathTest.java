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

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.rules.cel.builtin.VariantPath.FieldSegment;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.VariantPath.IndexSegment;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.VariantPath.Segment;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Parser tests for {@link VariantPath#parse} — the JSONPath subset used by
 * {@code variants.at(v, path)}.
 */
public class VariantPathTest {

  // ---- valid paths ----

  @Test
  void rootOnly_emptySegments() {
    assertEquals(0, VariantPath.parse("$").size());
  }

  @Test
  void singleField() {
    List<Segment> segs = VariantPath.parse("$.foo");
    assertEquals(1, segs.size());
    assertEquals("foo", ((FieldSegment) segs.get(0)).key);
  }

  @Test
  void nestedFields() {
    List<Segment> segs = VariantPath.parse("$.foo.bar.baz");
    assertEquals(3, segs.size());
    assertEquals("foo", ((FieldSegment) segs.get(0)).key);
    assertEquals("bar", ((FieldSegment) segs.get(1)).key);
    assertEquals("baz", ((FieldSegment) segs.get(2)).key);
  }

  @Test
  void singleIndex() {
    List<Segment> segs = VariantPath.parse("$[3]");
    assertEquals(1, segs.size());
    assertEquals(3, ((IndexSegment) segs.get(0)).index);
  }

  @Test
  void negativeIndex() {
    List<Segment> segs = VariantPath.parse("$[-1]");
    assertEquals(-1, ((IndexSegment) segs.get(0)).index);
  }

  @Test
  void mixedFieldAndIndex() {
    List<Segment> segs = VariantPath.parse("$.items[0].name");
    assertEquals(3, segs.size());
    assertEquals("items", ((FieldSegment) segs.get(0)).key);
    assertEquals(0, ((IndexSegment) segs.get(1)).index);
    assertEquals("name", ((FieldSegment) segs.get(2)).key);
  }

  @Test
  void doubleQuotedKey() {
    List<Segment> segs = VariantPath.parse("$[\"foo bar\"]");
    assertEquals("foo bar", ((FieldSegment) segs.get(0)).key);
  }

  @Test
  void singleQuotedKey() {
    List<Segment> segs = VariantPath.parse("$['foo bar']");
    assertEquals("foo bar", ((FieldSegment) segs.get(0)).key);
  }

  @Test
  void identifierWithUnderscores() {
    List<Segment> segs = VariantPath.parse("$.foo_bar.x_123");
    assertEquals("foo_bar", ((FieldSegment) segs.get(0)).key);
    assertEquals("x_123", ((FieldSegment) segs.get(1)).key);
  }

  // ---- malformed paths ----

  @Test
  void empty_throws() {
    assertThrows(IllegalArgumentException.class, () -> VariantPath.parse(""));
  }

  @Test
  void null_throws() {
    assertThrows(IllegalArgumentException.class, () -> VariantPath.parse(null));
  }

  @Test
  void missingRoot_throws() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> VariantPath.parse(".foo"));
    assertTrue(e.getMessage().contains("$"));
  }

  @Test
  void dotWithoutIdentifier_throws() {
    assertThrows(IllegalArgumentException.class, () -> VariantPath.parse("$."));
  }

  @Test
  void unterminatedBracket_throws() {
    assertThrows(IllegalArgumentException.class, () -> VariantPath.parse("$[0"));
  }

  @Test
  void unterminatedQuotedKey_throws() {
    assertThrows(IllegalArgumentException.class, () -> VariantPath.parse("$[\"foo"));
  }

  @Test
  void bracketWithoutContent_throws() {
    assertThrows(IllegalArgumentException.class, () -> VariantPath.parse("$[]"));
  }

  @Test
  void unexpectedCharacter_throws() {
    assertThrows(IllegalArgumentException.class, () -> VariantPath.parse("$+foo"));
  }

  @Test
  void digitLeadingIdent_throws() {
    // `$.123` is not a valid identifier — the documented grammar is
    // [A-Za-z_][A-Za-z0-9_]*. Use the quoted form `$["123"]` for keys that
    // start with a digit.
    assertThrows(IllegalArgumentException.class, () -> VariantPath.parse("$.123abc"));
  }

  @Test
  void digitLeadingKey_quotedForm_works() {
    List<Segment> segs = VariantPath.parse("$[\"123abc\"]");
    assertEquals(1, segs.size());
    assertEquals("123abc", ((FieldSegment) segs.get(0)).key);
  }
}
