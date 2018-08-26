/**
 * Copyright 2018 Confluent Inc.
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

import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class UrlParserTest {

  public static final String QUERY_PARAM_DELETED = "deleted";
  public static final String QUERY_PARAM_PRETTY = "pretty";

  private String getFirstValue(Map<String, List<String>> m, String key) {
    return m.containsKey(key) ? m.get(key).get(0) : null;
  }

  @Test
  public void test_get_query_params() {
    try {
      UrlParser p = new UrlParser("http://foo.com");
      assertEquals(0, p.getQueryParams().size());

      p = new UrlParser("http://foo.com?pretty");
      assertEquals(1, p.getQueryParams().size());

      p = new UrlParser("http://foo.com?deleted=true&pretty=false");
      assertEquals(2, p.getQueryParams().size());
    } catch (MalformedURLException e) {
      // no-op
    }
  }

  @Test
  public void verify_pretty_url_parsers() {
    String[] prettyUrls = {
            "http://foo.com?pretty=true",
            "http://foo.com?pretty",
            "http://foo.com?pretty=false&deleted=true",
            "http://foo.com?deleted=false&pretty",
            "http://foo.com?prety=true",
    };

    List<UrlParser> parsers = Arrays.stream(prettyUrls).map(u -> {
      try {
        return new UrlParser(u);
      } catch (MalformedURLException e) {
        // no-op
      }
      return null;
    }).collect(toList());

    List<Map<String, List<String>>> queryParams = parsers.stream()
            .map(UrlParser::getQueryParams).collect(toList());

    assertEquals("true", getFirstValue(queryParams.get(0), QUERY_PARAM_PRETTY));
    assertEquals("", getFirstValue(queryParams.get(1), QUERY_PARAM_PRETTY));
    assertTrue(parsers.get(1).hasBooleanQueryParam(QUERY_PARAM_PRETTY));

    assertEquals("false", getFirstValue(queryParams.get(2), QUERY_PARAM_PRETTY));
    assertEquals("true", getFirstValue(queryParams.get(2), QUERY_PARAM_DELETED));

    assertEquals("false", getFirstValue(queryParams.get(3), QUERY_PARAM_DELETED));
    assertEquals("", getFirstValue(queryParams.get(3), QUERY_PARAM_PRETTY));

    assertFalse(parsers.get(4).hasBooleanQueryParam(QUERY_PARAM_PRETTY));
  }

}
