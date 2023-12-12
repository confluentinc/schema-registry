/*
 * Copyright 2021 Confluent Inc.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class EnumRecommender implements ConfigDef.Validator, ConfigDef.Recommender {
  private final List<String> canonicalValues;
  private final Set<String> validValues;

  private EnumRecommender(List<String> canonicalValues, Set<String> validValues) {
    this.canonicalValues = canonicalValues;
    this.validValues = validValues;
  }

  @SafeVarargs
  public static <E> EnumRecommender in(E... enumerators) {
    final List<String> canonicalValues = new ArrayList<>(enumerators.length);
    final Set<String> validValues = new HashSet<>(enumerators.length * 2);
    for (E e : enumerators) {
      canonicalValues.add(e.toString().toLowerCase(Locale.ROOT));
      validValues.add(e.toString().toLowerCase(Locale.ROOT));
      validValues.add(e.toString().toUpperCase(Locale.ROOT));
    }
    return new EnumRecommender(canonicalValues, validValues);
  }

  @Override
  public void ensureValid(String key, Object value) {
    // calling toString on itself because IDE complains if the Object is passed.
    if (value != null && !validValues.contains(value.toString())) {
      throw new ConfigException(key, value, "Invalid enumerator");
    }
  }

  @Override
  public String toString() {
    return canonicalValues.toString();
  }

  @Override
  public List<Object> validValues(String name, Map<String, Object> configs) {
    return new ArrayList<>(canonicalValues);
  }

  @Override
  public boolean visible(String name, Map<String, Object> configs) {
    return true;
  }
}
