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

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import io.confluent.kafka.schemaregistry.rest.resources.RequestHeaderBuilder;

public class RequestHeaderBuilderTest {

  @Test
  public void testHeaderProperties(){
    HttpHeaders httpHeaders = EasyMock.createMock(HttpHeaders.class);
    EasyMock.expect(httpHeaders.getHeaderString("Content-Type")).andReturn("application/json");
    EasyMock.expect(httpHeaders.getHeaderString("Accept")).andReturn("application/json");
    EasyMock.expect(httpHeaders.getHeaderString("Authorization")).andReturn("test");
    EasyMock.replay(httpHeaders);

    SchemaRegistryConfig config = EasyMock.createMock(SchemaRegistryConfig.class);
    EasyMock.expect(config.headersForward()).andReturn(Collections.EMPTY_LIST);
    EasyMock.replay(config);

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(httpHeaders, config);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("application/json", requestProps.get("Content-Type"));
    Assert.assertEquals("application/json", requestProps.get("Accept"));
    Assert.assertEquals("test", requestProps.get("Authorization"));
  }

  @Test
  public void testEmptyProperty(){
    HttpHeaders httpHeaders = EasyMock.createMock(HttpHeaders.class);
    EasyMock.expect(httpHeaders.getHeaderString("Content-Type")).andReturn("application/json");
    EasyMock.expect(httpHeaders.getHeaderString("Accept")).andReturn("application/json");
    EasyMock.expect(httpHeaders.getHeaderString("Authorization")).andReturn("");
    EasyMock.replay(httpHeaders);

    SchemaRegistryConfig config = EasyMock.createMock(SchemaRegistryConfig.class);
    EasyMock.expect(config.headersForward()).andReturn(Collections.EMPTY_LIST);
    EasyMock.replay(config);

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(httpHeaders, config);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("application/json", requestProps.get("Content-Type"));
    Assert.assertEquals("application/json", requestProps.get("Accept"));
    Assert.assertNull( requestProps.get("Authorization"));
  }

  @Test
  public void testMissingProperty(){
    HttpHeaders httpHeaders = EasyMock.createMock(HttpHeaders.class);
    EasyMock.expect(httpHeaders.getHeaderString("Content-Type")).andReturn("application/json");
    EasyMock.expect(httpHeaders.getHeaderString("Accept")).andReturn("application/json");
    EasyMock.expect(httpHeaders.getHeaderString("Authorization")).andReturn(null);
    EasyMock.replay(httpHeaders);

    SchemaRegistryConfig config = EasyMock.createMock(SchemaRegistryConfig.class);
    EasyMock.expect(config.headersForward()).andReturn(Collections.EMPTY_LIST);
    EasyMock.replay(config);

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(httpHeaders, config);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("application/json", requestProps.get("Content-Type"));
    Assert.assertEquals("application/json", requestProps.get("Accept"));
    Assert.assertNull( requestProps.get("Authorization"));
  }

  @Test
  public void testForwardOneHeader(){
    HttpHeaders httpHeaders = EasyMock.createMock(HttpHeaders.class);
    EasyMock.expect(httpHeaders.getHeaderString("Content-Type")).andReturn(null);
    EasyMock.expect(httpHeaders.getHeaderString("Accept")).andReturn(null);
    EasyMock.expect(httpHeaders.getHeaderString("Authorization")).andReturn(null);
    EasyMock.expect(httpHeaders.getHeaderString("target-sr-cluster")).andReturn("sr-xyz");
    EasyMock.expect(httpHeaders.getHeaderString("some-other-header")).andReturn("abc");
    EasyMock.replay(httpHeaders);

    SchemaRegistryConfig config = EasyMock.createMock(SchemaRegistryConfig.class);
    EasyMock.expect(config.headersForward()).andReturn(Collections.singletonList("target-sr-cluster"));
    EasyMock.replay(config);

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(httpHeaders, config);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("sr-xyz", requestProps.get("target-sr-cluster"));
    Assert.assertNull(requestProps.get("some-other-header"));
  }

  @Test
  public void testForwardMultipleHeaders(){
    HttpHeaders httpHeaders = EasyMock.createMock(HttpHeaders.class);
    EasyMock.expect(httpHeaders.getHeaderString("Content-Type")).andReturn(null);
    EasyMock.expect(httpHeaders.getHeaderString("Accept")).andReturn(null);
    EasyMock.expect(httpHeaders.getHeaderString("Authorization")).andReturn(null);
    EasyMock.expect(httpHeaders.getHeaderString("target-sr-cluster")).andReturn("sr-xyz");
    EasyMock.expect(httpHeaders.getHeaderString("some-other-header")).andReturn("abc");
    EasyMock.replay(httpHeaders);

    SchemaRegistryConfig config = EasyMock.createMock(SchemaRegistryConfig.class);
    List<String> headersForward = new ArrayList<>();
    headersForward.add("target-sr-cluster");
    headersForward.add("some-other-header");
    EasyMock.expect(config.headersForward()).andReturn(headersForward);
    EasyMock.replay(config);

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(httpHeaders, config);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("sr-xyz", requestProps.get("target-sr-cluster"));
    Assert.assertEquals("abc", requestProps.get("some-other-header"));
  }

  @Test
  public void testDoNotForwardHeaders() {
    HttpHeaders httpHeaders = EasyMock.createMock(HttpHeaders.class);
    EasyMock.expect(httpHeaders.getHeaderString("Content-Type")).andReturn(null);
    EasyMock.expect(httpHeaders.getHeaderString("Accept")).andReturn(null);
    EasyMock.expect(httpHeaders.getHeaderString("Authorization")).andReturn(null);
    EasyMock.expect(httpHeaders.getHeaderString("target-sr-cluster")).andReturn("sr-xyz");
    EasyMock.expect(httpHeaders.getHeaderString("some-other-header")).andReturn("abc");
    EasyMock.replay(httpHeaders);

    SchemaRegistryConfig config = EasyMock.createMock(SchemaRegistryConfig.class);
    EasyMock.expect(config.headersForward()).andReturn(Collections.emptyList());
    EasyMock.replay(config);

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(httpHeaders, config);
    Assert.assertNotNull(requestProps);
    Assert.assertNull(requestProps.get("target-sr-cluster"));
    Assert.assertNull(requestProps.get("some-other-header"));
  }
}
