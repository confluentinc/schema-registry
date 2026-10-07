/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class WildcardMatcherTest {

  @Test
  public void testMatch() {
    assertFalse(WildcardMatcher.match(null, "Foo"));
    assertFalse(WildcardMatcher.match("Foo", null));
    assertTrue(WildcardMatcher.match(null, null));
    assertTrue(WildcardMatcher.match("Foo", "Foo"));
    assertTrue(WildcardMatcher.match("", ""));
    assertTrue(WildcardMatcher.match("", "*"));
    assertFalse(WildcardMatcher.match("", "?"));
    assertTrue(WildcardMatcher.match("Foo", "Fo*"));
    assertTrue(WildcardMatcher.match("Foo", "Fo?"));
    assertTrue(WildcardMatcher.match("Foo Bar and Catflap", "Fo*"));
    assertTrue(WildcardMatcher.match("New Bookmarks", "N?w ?o?k??r?s"));
    assertFalse(WildcardMatcher.match("Foo", "Bar"));
    assertTrue(WildcardMatcher.match("Foo Bar Foo", "F*o Bar*"));
    assertTrue(WildcardMatcher.match("Adobe Acrobat Installer", "Ad*er"));
    assertTrue(WildcardMatcher.match("Foo", "*Foo"));
    assertTrue(WildcardMatcher.match("BarFoo", "*Foo"));
    assertTrue(WildcardMatcher.match("Foo", "Foo*"));
    assertTrue(WildcardMatcher.match("FooBar", "Foo*"));
    assertFalse(WildcardMatcher.match("FOO", "*Foo"));
    assertFalse(WildcardMatcher.match("BARFOO", "*Foo"));
    assertFalse(WildcardMatcher.match("FOO", "Foo*"));
    assertFalse(WildcardMatcher.match("FOOBAR", "Foo*"));
    assertTrue(WildcardMatcher.match("eve", "eve*"));
    assertTrue(WildcardMatcher.match("alice.bob.eve", "a*.bob.eve"));
    assertTrue(WildcardMatcher.match("alice.bob.eve", "a*.bob.e*"));
    assertFalse(WildcardMatcher.match("alice.bob.eve", "a*"));
    assertTrue(WildcardMatcher.match("alice.bob.eve", "a**"));
    assertFalse(WildcardMatcher.match("alice.bob.eve", "alice.bob*"));
    assertTrue(WildcardMatcher.match("alice.bob.eve", "alice.bob**"));
  }
}
