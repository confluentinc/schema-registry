/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

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

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(httpHeaders);
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

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(httpHeaders);
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

    RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();
    Map<String, String> requestProps = requestHeaderBuilder.buildRequestHeaders(httpHeaders);
    Assert.assertNotNull(requestProps);
    Assert.assertEquals("application/json", requestProps.get("Content-Type"));
    Assert.assertEquals("application/json", requestProps.get("Accept"));
    Assert.assertNull( requestProps.get("Authorization"));
  }

}
