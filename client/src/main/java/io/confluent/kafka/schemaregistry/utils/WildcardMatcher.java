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


import java.util.regex.Pattern;

/**
 * A wildcard matcher.
 */
public class WildcardMatcher {

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
    Pattern wildcardRegexp = Pattern.compile(wildcardToRegexp(wildcardMatcher, '.'));
    return wildcardRegexp.matcher(str).matches();
  }

  private static String wildcardToRegexp(String globExp, char separator) {
    StringBuilder dst = new StringBuilder();
    char[] src = globExp.replace("**" + separator + "*", "**").toCharArray();
    int i = 0;
    while (i < src.length) {
      char c = src[i++];
      switch (c) {
        case '*':
          // One char lookahead for **
          if (i < src.length && src[i] == '*') {
            dst.append(".*");
            ++i;
          } else {
            dst.append("[^");
            dst.append(separator);
            dst.append("]*");
          }
          break;
        case '?':
          dst.append("[^");
          dst.append(separator);
          dst.append("]");
          break;
        case '.':
        case '+':
        case '{':
        case '}':
        case '(':
        case ')':
        case '|':
        case '^':
        case '$':
          // These need to be escaped in regular expressions
          dst.append('\\').append(c);
          break;
        case '\\':
          i = doubleSlashes(dst, src, i);
          break;
        default:
          dst.append(c);
          break;
      }
    }
    return dst.toString();
  }

  private static int doubleSlashes(StringBuilder dst, char[] src, int i) {
    // Emit the next character without special interpretation
    dst.append('\\');
    if ((i + 1) < src.length) {
      dst.append('\\');
      dst.append(src[i]);
      i++;
    } else {
      // A backslash at the very end is treated like an escaped backslash
      dst.append('\\');
    }
    return i;
  }
}

