/**
 * Copyright 2018 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.rest.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * This class exposes functions to parse a {@link java.net.URL}.
 */
public class UrlParser {

  private final URL url;

  public UrlParser(URL url) {
    if (url == null || url.toString().isEmpty()) {
      throw new IllegalArgumentException("Expected at least one URL to be passed in constructor");
    }
    this.url = url;
  }

  public UrlParser(String url) throws MalformedURLException {
    this(new URL(url));
  }

  /**
   * Inspects a multi-value map for a key set to "true" or empty
   * @return true if the first map entry for key is (key, "true") or (key, "")
   */
  public boolean hasBooleanQueryParam(String key) {
    Map<String, List<String>> queryParams = getQueryParams();
    if (queryParams == null || queryParams.size() == 0) {
      return false;
    }
    boolean hasKey = queryParams.containsKey(key);
    List<String> values = queryParams.get(key);
    String value = values != null && values.size() > 0 ? values.get(0) : "";
    if (hasKey && value.isEmpty()) { // for example: http://host:port?pretty
      return true;
    } else if (!Boolean.valueOf(value)) {
      return false;
    }
    return "true".equals(value);
  }

  public Map<String, List<String>> getQueryParams() {
    return UrlParser.getQueryParams(this.url);
  }

  /**
   * Parses the query parameters from a URL and returns them as a multi-value Map
   * @param url {@link java.net.URL} to extract query parameters from
   * @return A multi-value map. Each key can potentially have many values.
   */
  public static Map<String, List<String>> getQueryParams(URL url) {
    Map<String, List<String>> queryMap = new HashMap<>();
    if (url.getQuery() != null) {
      queryMap = Pattern.compile("&").splitAsStream(url.getQuery())
              .map(s -> Arrays.copyOf(s.split("="), 2))
              .collect(groupingBy(s -> s[0],
                      mapping(s -> s[1] == null ? "" : s[1], toList())));
    }
    return queryMap;
  }

}
