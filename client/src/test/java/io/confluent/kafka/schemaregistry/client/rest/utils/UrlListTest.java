/*
 * Copyright 2014-2018 Confluent Inc.
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

import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;

public class UrlListTest {

  @Test
  public void verify_url_failure_rotates_urls() {
    String url1 = "http://foo.com";
    String url2 = "http://bar.com";

    UrlList urls = new UrlList(Arrays.asList(url1, url2));
    assertEquals(2, urls.size());

    if (urls.current().equals(url2)) urls.fail(url2);

    assertEquals(url1, urls.current());

    urls.fail(url1);
    assertEquals(url2, urls.current());

    urls.fail(url1); // No effect if not the current url
    assertEquals(url2, urls.current());

    urls.fail(url2);
    assertEquals(url1, urls.current());
  }



}
