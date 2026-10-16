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

package io.confluent.kafka.schemaregistry.rules.jsonata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.api.jsonata4java.expressions.regex.RegexFlags;
import com.api.jsonata4java.expressions.regex.RegexMatch;
import com.api.jsonata4java.expressions.regex.RegexPattern;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

/**
 * Unit tests for {@link RE2RegexPattern} that exercise the {@link RegexPattern}
 * implementation directly, without going through {@code Expression.jsonata}.
 */
public class RE2RegexPatternTest {

  private static final RegexFlags NONE = new RegexFlags(false, false);

  @Test
  public void testMatch() {
    RegexPattern pattern = new RE2RegexPattern("o-w", NONE);
    Optional<RegexMatch> result = pattern.findFirst("hello-world", 0);
    assertTrue(result.isPresent());
    assertEquals("o-w", result.get().getMatch());
    assertEquals(4, result.get().getIndex());
  }

  @Test
  public void testMatchCaseInsensitive() {
    RegexPattern pattern = new RE2RegexPattern("hello", new RegexFlags(true, false));
    Optional<RegexMatch> result = pattern.findFirst("HELLO", 0);
    assertTrue(result.isPresent());
    assertEquals("HELLO", result.get().getMatch());
  }

  @Test
  public void testContains() {
    RegexPattern pattern = new RE2RegexPattern("ell", NONE);
    assertTrue(pattern.test("hello"));
    assertFalse(pattern.test("xyz"));
  }

  @Test
  public void testFindAllWithGroups() {
    RegexPattern pattern = new RE2RegexPattern("(\\w+)\\s(\\w+)", NONE);
    List<RegexMatch> matches = pattern.findAll("John Smith");
    assertEquals(1, matches.size());
    RegexMatch match = matches.get(0);
    assertEquals("John Smith", match.getMatch());
    assertEquals(List.of("John", "Smith"), match.getGroups());
  }

  @Test
  public void testSplit() {
    RegexPattern pattern = new RE2RegexPattern("[0-9]", NONE);
    assertArrayEquals(new String[] {"a", "b", "c"}, pattern.split("a1b2c3"));
  }

  @Test
  public void testRejectsBackreferences() {
    // RE2 (and therefore RE2/J) deliberately does not support
    // backreferences, since they make linear-time matching impossible.
    assertThrows(Exception.class, () -> new RE2RegexPattern("(a)\\1", NONE));
  }
}
