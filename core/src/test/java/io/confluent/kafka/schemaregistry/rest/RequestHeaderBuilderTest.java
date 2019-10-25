/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import io.confluent.kafka.schemaregistry.rest.resources.RequestHeaderBuilder;

public class RequestHeaderBuilderTest {

  @Test
  public void testHeaderProperties(){
    HttpHeaders httpHeaders = mockHttpHeaders(ImmutableMap.of(
        "Content-Type", "application/json",
        "Accept", "application/json",
        "Authorization", "test"
    ));

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(
        httpHeaders, Collections.EMPTY_LIST);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("application/json", requestProps.get("Content-Type"));
    Assert.assertEquals("application/json", requestProps.get("Accept"));
    Assert.assertEquals("test", requestProps.get("Authorization"));
  }

  private HttpHeaders mockHttpHeaders(Map<String, String> headers) {
    HttpHeaders httpHeaders = EasyMock.createMock(HttpHeaders.class);
    headers.forEach(
        (headerName, headerValue) -> EasyMock.expect(httpHeaders.getHeaderString(headerName)).andReturn(headerValue));
    EasyMock.replay(httpHeaders);
    return httpHeaders;
  }

  @Test
  public void testEmptyProperty(){
    HttpHeaders httpHeaders = mockHttpHeaders(ImmutableMap.of(
        "Content-Type", "application/json",
        "Accept", "application/json",
        "Authorization", ""
    ));

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(
        httpHeaders, Collections.EMPTY_LIST);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("application/json", requestProps.get("Content-Type"));
    Assert.assertEquals("application/json", requestProps.get("Accept"));
    Assert.assertNull( requestProps.get("Authorization"));
  }

  @Test
  public void testMissingProperty(){
    HttpHeaders httpHeaders = mockHttpHeaders(ImmutableMap.of(
        "Content-Type", "application/json",
        "Accept", "application/json",
        "Authorization", ""));

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(
        httpHeaders, Collections.EMPTY_LIST);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("application/json", requestProps.get("Content-Type"));
    Assert.assertEquals("application/json", requestProps.get("Accept"));
    Assert.assertNull( requestProps.get("Authorization"));
  }

  @Test
  public void testForwardOneHeader(){
    HttpHeaders httpHeaders = mockHttpHeaders(ImmutableMap.of(
        "Content-Type", "",
        "Accept", "",
        "Authorization", "",
        "target-sr-cluster", "sr-xyz",
        "some-other-header", "abc"));

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(
        httpHeaders, Collections.singletonList("target-sr-cluster"));
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("sr-xyz", requestProps.get("target-sr-cluster"));
    Assert.assertNull(requestProps.get("some-other-header"));
  }

  @Test
  public void testForwardMultipleHeaders(){
    HttpHeaders httpHeaders = mockHttpHeaders(ImmutableMap.of(
        "Content-Type", "",
        "Accept", "",
        "Authorization", "",
        "target-sr-cluster", "sr-xyz",
        "some-other-header", "abc"
    ));

    List<String> headersForward = ImmutableList.of("target-sr-cluster", "some-other-header");

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(
        httpHeaders, headersForward);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("sr-xyz", requestProps.get("target-sr-cluster"));
    Assert.assertEquals("abc", requestProps.get("some-other-header"));
  }

  @Test
  public void testDoNotForwardHeaders() {
    HttpHeaders httpHeaders = mockHttpHeaders(ImmutableMap.of(
        "Content-Type", "",
        "Accept", "",
        "Authorization", "",
        "target-sr-cluster", "sr-xyz",
        "some-other-header", "abc"
    ));

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(
        httpHeaders, Collections.EMPTY_LIST);
    Assert.assertNotNull(requestProps);
    Assert.assertNull(requestProps.get("target-sr-cluster"));
    Assert.assertNull(requestProps.get("some-other-header"));
  }

  @Test
  public void testNullWhitelist() {
    HttpHeaders httpHeaders = mockHttpHeaders(ImmutableMap.of(
        "Content-Type", "application/json",
        "Accept", "application/json",
        "Authorization", "test",
        "target-sr-cluster", "sr-xyz",
        "some-other-header", "abc"
    ));

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(
        httpHeaders, null);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("application/json", requestProps.get("Content-Type"));
    Assert.assertEquals("application/json", requestProps.get("Accept"));
    Assert.assertEquals("test", requestProps.get("Authorization"));
    Assert.assertNull(requestProps.get("target-sr-cluster"));
    Assert.assertNull(requestProps.get("some-other-header"));
  }
}
