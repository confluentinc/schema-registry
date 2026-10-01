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

import com.api.jsonata4java.expressions.regex.RegexEngine;
import com.api.jsonata4java.expressions.regex.RegexFlags;
import com.api.jsonata4java.expressions.regex.RegexMatch;
import com.api.jsonata4java.expressions.regex.RegexPattern;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A JSONata regex engine implementation backed by Google's RE2/J
 */
public final class RE2RegexPattern implements RegexPattern {

  private final Pattern pattern;

  public RE2RegexPattern(String regexPattern, RegexFlags flags) {
    int re2Flags = 0;
    if (flags.isCaseInsensitive()) {
      re2Flags |= Pattern.CASE_INSENSITIVE;
    }
    if (flags.isMultiline()) {
      re2Flags |= Pattern.MULTILINE;
    }
    this.pattern = Pattern.compile(regexPattern, re2Flags);
  }

  /**
   * Factory for use with {@code Expressions.parse}/{@code Expression.jsonata},
   * e.g. {@code Expression.jsonata(expr, RE2RegexPattern::new)}.
   */
  public static RegexEngine engine() {
    return RE2RegexPattern::new;
  }

  @Override
  public boolean test(String input) {
    return pattern.matcher(input).find();
  }

  @Override
  public Optional<RegexMatch> findFirst(String input, int fromIndex) {
    if (fromIndex < 0 || fromIndex > input.length()) {
      return Optional.empty();
    }
    Matcher matcher = pattern.matcher(input);
    if (!matcher.find(fromIndex)) {
      return Optional.empty();
    }
    return Optional.of(toRegexMatch(matcher));
  }

  @Override
  public List<RegexMatch> findAll(String input) {
    List<RegexMatch> matches = new ArrayList<>();
    Matcher matcher = pattern.matcher(input);
    while (matcher.find()) {
      matches.add(toRegexMatch(matcher));
    }
    return matches;
  }

  @Override
  public String[] split(String input) {
    return pattern.split(input);
  }

  private static RegexMatch toRegexMatch(Matcher matcher) {
    List<String> groups = new ArrayList<>();
    int groupCount = matcher.groupCount();
    for (int i = 1; i <= groupCount; i++) {
      groups.add(matcher.group(i));
    }
    return new RegexMatch(matcher.group(), matcher.start(), groups);
  }
}