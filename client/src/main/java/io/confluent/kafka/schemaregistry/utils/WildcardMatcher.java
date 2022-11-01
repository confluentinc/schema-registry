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


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

/**
 * A wildcard matcher, adapted from Apache Commons IO.
 */
public class WildcardMatcher {

  private static final String[] EMPTY_STRING_ARRAY = {};

  private static final int NOT_FOUND = -1;

  /**
   * Splits a string into a number of tokens. The text is split by '?' and '*'. Where multiple '*'
   * occur consecutively they are collapsed into a single '*'.
   *
   * @param text the text to split
   * @return the array of tokens, never null
   */
  static String[] splitOnTokens(final String text) {
    // used by wildcardMatch
    // package level so a unit test may run on this

    if (text.indexOf('?') == NOT_FOUND && text.indexOf('*') == NOT_FOUND) {
      return new String[]{text};
    }

    final char[] array = text.toCharArray();
    final ArrayList<String> list = new ArrayList<>();
    final StringBuilder buffer = new StringBuilder();
    char prevChar = 0;
    for (final char ch : array) {
      if (ch == '?' || ch == '*') {
        if (buffer.length() != 0) {
          list.add(buffer.toString());
          buffer.setLength(0);
        }
        if (ch == '?') {
          list.add("?");
        } else if (prevChar != '*') {  // ch == '*' here; check if previous char was '*'
          list.add("*");
        }
      } else {
        buffer.append(ch);
      }
      prevChar = ch;
    }
    if (buffer.length() != 0) {
      list.add(buffer.toString());
    }

    return list.toArray(EMPTY_STRING_ARRAY);
  }

  /**
   * Checks a string to see if it matches the specified wildcard matcher, always testing
   * case-sensitive.
   *
   * <p>The wildcard matcher uses the characters '?' and '*' to represent a single or multiple
   * (zero or more) wildcard characters. This is the same as often found on DOS/Unix command lines.
   * The check is case-sensitive always.
   * <pre>
   * wildcardMatch("c.txt", "*.txt")      --&gt; true
   * wildcardMatch("c.txt", "*.jpg")      --&gt; false
   * wildcardMatch("a/b/c.txt", "a/b/*")  --&gt; true
   * wildcardMatch("c.txt", "*.???")      --&gt; true
   * wildcardMatch("c.txt", "*.????")     --&gt; false
   * </pre>
   * N.B. the sequence "*?" does not work properly at present in match strings.
   *
   * @param str             the string to match on
   * @param wildcardMatcher the wildcard string to match against
   * @return true if the string matches the wildcard string
   */
  public static boolean match(final String str, final String wildcardMatcher) {
    return match(str, wildcardMatcher, false);
  }

  /**
   * Checks a string to see if it matches the specified wildcard matcher allowing control over
   * case-sensitivity.
   *
   * <p>The wildcard matcher uses the characters '?' and '*' to represent a single or multiple
   * (zero or more) wildcard characters. N.B. the sequence "*?" does not work properly at present
   * in match strings.
   *
   * @param str             the string to match on
   * @param wildcardMatcher the wildcard string to match against
   * @param ignoreCase      whether to ignore case
   * @return true if the string matches the wildcard string
   */
  public static boolean match(final String str, final String wildcardMatcher, boolean ignoreCase) {
    if (str == null && wildcardMatcher == null) {
      return true;
    }
    if (str == null || wildcardMatcher == null) {
      return false;
    }
    final String[] wcs = splitOnTokens(wildcardMatcher);
    boolean anyChars = false;
    int textIdx = 0;
    int wcsIdx = 0;
    final Deque<int[]> backtrack = new ArrayDeque<>(wcs.length);

    // loop around a backtrack stack, to handle complex * matching
    do {
      if (!backtrack.isEmpty()) {
        final int[] array = backtrack.pop();
        wcsIdx = array[0];
        textIdx = array[1];
        anyChars = true;
      }

      // loop whilst tokens and text left to process
      while (wcsIdx < wcs.length) {

        if (wcs[wcsIdx].equals("?")) {
          // ? so move to next text char
          textIdx++;
          if (textIdx > str.length()) {
            break;
          }
          anyChars = false;

        } else if (wcs[wcsIdx].equals("*")) {
          // set any chars status
          anyChars = true;
          if (wcsIdx == wcs.length - 1) {
            textIdx = str.length();
          }

        } else {
          // matching text token
          if (anyChars) {
            // any chars then try to locate text token
            textIdx = checkIndexOf(ignoreCase, str, textIdx, wcs[wcsIdx]);
            if (textIdx == NOT_FOUND) {
              // token not found
              break;
            }
            final int repeat = checkIndexOf(ignoreCase, str, textIdx + 1, wcs[wcsIdx]);
            if (repeat >= 0) {
              backtrack.push(new int[]{wcsIdx, repeat});
            }
          } else if (!checkRegionMatches(ignoreCase, str, textIdx, wcs[wcsIdx])) {
            // matching from current position
            // couldn't match token
            break;
          }

          // matched text token, move text index to end of matched token
          textIdx += wcs[wcsIdx].length();
          anyChars = false;
        }

        wcsIdx++;
      }

      // full match
      if (wcsIdx == wcs.length && textIdx == str.length()) {
        return true;
      }

    } while (!backtrack.isEmpty());

    return false;
  }

  static int checkIndexOf(
      final boolean ignoreCase, final String str, final int strStartIndex, final String search) {
    final int endIndex = str.length() - search.length();
    if (endIndex >= strStartIndex) {
      for (int i = strStartIndex; i <= endIndex; i++) {
        if (checkRegionMatches(ignoreCase, str, i, search)) {
          return i;
        }
      }
    }
    return -1;
  }

  static boolean checkRegionMatches(
      final boolean ignoreCase, final String str, final int strStartIndex, final String search) {
    return str.regionMatches(ignoreCase, strStartIndex, search, 0, search.length());
  }
}

