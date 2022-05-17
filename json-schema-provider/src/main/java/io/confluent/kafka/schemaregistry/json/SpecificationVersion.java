/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.json;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public enum SpecificationVersion {
  DRAFT_4,
  DRAFT_6,
  DRAFT_7,
  DRAFT_2019_09;

  private static final Map<String, SpecificationVersion> lookup = new HashMap<>();

  static {
    for (SpecificationVersion m : EnumSet.allOf(SpecificationVersion.class)) {
      lookup.put(m.toString(), m);
    }
  }

  public static SpecificationVersion get(String name) {
    return lookup.get(name.toLowerCase(Locale.ROOT));
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
