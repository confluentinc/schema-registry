/**
 * Copyright 2015 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.client.rest;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class RestServiceTest {

  @Test
  public void buildRequestUrl_trimNothing() {
    String baseUrl = "http://test.com";
    String path = "some/path";

    assertEquals("http://test.com/some/path", RestService.buildRequestUrl(baseUrl, path));
  }

  @Test
  public void buildRequestUrl_trimBaseUrl() {
    String baseUrl = "http://test.com/";
    String path = "some/path";

    assertEquals("http://test.com/some/path", RestService.buildRequestUrl(baseUrl, path));
  }

  @Test
  public void buildRequestUrl_trimPath() {
    String baseUrl = "http://test.com";
    String path = "/some/path";

    assertEquals("http://test.com/some/path", RestService.buildRequestUrl(baseUrl, path));
  }

  @Test
  public void buildRequestUrl_trimBaseUrlAndPath() {
    String baseUrl = "http://test.com/";
    String path = "/some/path";

    assertEquals("http://test.com/some/path", RestService.buildRequestUrl(baseUrl, path));
  }
}
