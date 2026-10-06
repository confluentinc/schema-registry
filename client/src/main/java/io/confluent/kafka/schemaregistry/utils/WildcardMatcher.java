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

/**
 * A wildcard matcher.
 */
public class WildcardMatcher {

  private static final char SEPARATOR = '.';

  /**
   * Matches fully-qualified names that use dot (.) as the name boundary.
   *
   * <p>A '?' matches a single character.
   * A '*' matches one or more characters within a name boundary.
   * A '**' matches one or more characters across name boundaries.
   *
   * <p>Examples:
   * <pre>
   * wildcardMatch("eve", "eve*")                  --&gt; true
   * wildcardMatch("alice.bob.eve", "a*.bob.eve")  --&gt; true
   * wildcardMatch("alice.bob.eve", "a*.bob.e*")   --&gt; true
   * wildcardMatch("alice.bob.eve", "a*")          --&gt; false
   * wildcardMatch("alice.bob.eve", "a**")         --&gt; true
   * wildcardMatch("alice.bob.eve", "alice.bob*")  --&gt; false
   * wildcardMatch("alice.bob.eve", "alice.bob**") --&gt; true
   * </pre>
   *
   * @param str             the string to match on
   * @param wildcardMatcher the wildcard string to match against
   * @return true if the string matches the wildcard string
   */
  public static boolean match(final String str, final String wildcardMatcher) {
    if (str == null && wildcardMatcher == null) {
      return true;
    }
    if (str == null || wildcardMatcher == null) {
      return false;
    }
    return globMatch(str, wildcardMatcher.replace("**" + SEPARATOR + "*", "**"));
  }

  // matched[j] is true when the pattern consumed so far matches the first j
  // characters of str.
  private static boolean globMatch(String str, String glob) {
    int n = str.length();
    boolean[] matched = new boolean[n + 1];
    matched[0] = true;
    int i = 0;
    while (i < glob.length()) {
      char c = glob.charAt(i++);
      boolean[] next = new boolean[n + 1];
      if (c == '*') {
        // One char lookahead for **
        boolean crossesSeparator = i < glob.length() && glob.charAt(i) == '*';
        if (crossesSeparator) {
          ++i;
        }
        next[0] = matched[0];
        for (int j = 0; j < n; j++) {
          next[j + 1] = matched[j + 1]
              || (next[j] && (crossesSeparator || str.charAt(j) != SEPARATOR));
        }
      } else {
        boolean any = c == '?';
        if (c == '\\') {
          // Emit the next character without special interpretation;
          // a backslash at the very end is treated like an escaped backslash
          if (i < glob.length()) {
            c = glob.charAt(i++);
          }
        }
        for (int j = 0; j < n; j++) {
          char s = str.charAt(j);
          next[j + 1] = matched[j] && (any ? s != SEPARATOR : s == c);
        }
      }
      matched = next;
    }
    return matched[n];
  }
}
