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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public enum SpecificationVersion {
  DRAFT_4(Arrays.asList(
      "http://json-schema.org/draft-04/schema",
      "https://json-schema.org/draft-04/schema",
      "http://json-schema.org/schema",
      "https://json-schema.org/schema"
  )),
  DRAFT_6(Arrays.asList(
      "http://json-schema.org/draft-06/schema",
      "https://json-schema.org/draft-06/schema"
  )),
  DRAFT_7(Arrays.asList(
      "http://json-schema.org/draft-07/schema",
      "https://json-schema.org/draft-07/schema"
  )),
  DRAFT_2019_09(Arrays.asList(
      "http://json-schema.org/draft/2019-09/schema",
      "https://json-schema.org/draft/2019-09/schema"
  )),
  DRAFT_2020_12(Arrays.asList(
      "http://json-schema.org/draft/2020-12/schema",
      "https://json-schema.org/draft/2020-12/schema"
  ));

  private final List<String> urls;

  SpecificationVersion(List<String> urls) {
    this.urls = urls;
  }

  public List<String> getUrls() {
    return this.urls;
  }

  private static final Map<String, SpecificationVersion> lookup = new HashMap<>();

  private static final Map<String, SpecificationVersion> urlLookup = new HashMap<>();

  static {
    for (SpecificationVersion spec : SpecificationVersion.values()) {
      lookup.put(spec.toString(), spec);
      for (String url : spec.getUrls()) {
        urlLookup.put(url, spec);
      }
    }
  }

  public static SpecificationVersion get(String name) {
    return lookup.get(name.toLowerCase(Locale.ROOT));
  }

  public static SpecificationVersion getFromUrl(String url) {
    return urlLookup.get(url);
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
