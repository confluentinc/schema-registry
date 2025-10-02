/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest.utils;

import com.google.common.base.CharMatcher;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;

public class RestValidation {

  public static final int NAME_MAX_LENGTH = 256;

  public static void checkName(String name, String propertyName) {
    if (name == null || name.isEmpty()) {
      throw new IllegalPropertyException(propertyName, "cannot be null or empty");
    }
    if (name.length() > NAME_MAX_LENGTH) {
      throw new IllegalPropertyException(propertyName, "exceeds max length of " + NAME_MAX_LENGTH);
    }
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      throw new IllegalPropertyException(propertyName, "must start with a letter or underscore");
    }
    for (int i = 1; i < name.length(); i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_' || c == '-')) {
        throw new IllegalPropertyException(propertyName, "illegal character '" + c + "'");
      }
    }
  }

  public static void checkSubject(String subject) {
    if (subject == null || subject.isEmpty()
        || CharMatcher.javaIsoControl().matchesAnyOf(subject)) {
      throw new IllegalPropertyException("subject", "must not be empty");
    }
  }
}
