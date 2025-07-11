/*
 * Copyright 2023 Confluent Inc.
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

import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.server.Request;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MutableHttpServletRequestTest {

  MutableRequest mutableRequest;

  @Mock
  Request httpServletRequest;

  @Before
  public void setup(){
    reset(httpServletRequest);
    mutableRequest = new MutableRequest(httpServletRequest);
  }

  @Test
  public void testGetWithNoInitialHeaders() {
    String headerValue;
    Enumeration<String> headerValues;
    Enumeration<String> headerNames;

    // Mock no header return values from HttpServletRequest
    when(httpServletRequest.getHeaders()).thenReturn(HttpFields.build());

    // Validate gets
    headerValue = mutableRequest.getHeader("header-key-0");
    Assert.assertNull(headerValue);

    headerValues = mutableRequest.getHeaders("header-key-0");
    Assert.assertFalse(headerValues.hasMoreElements());

    headerNames = mutableRequest.getHeaderNames();
    Assert.assertFalse(headerNames.hasMoreElements());
  }

  @Test
  public void testGetAndPutWithNoInitialHeaders() {
    Enumeration<String> headerNames;
    List<String> headerValuesList;
    List<String> headerNamesList;

    // Mock no header return values from HttpServletRequest
    when(httpServletRequest.getHeaders()).thenReturn(HttpFields.build());

    // Add a "header-key-0" header
    mutableRequest.putHeader("header-key-0", "new-header-value-98");

    // Validate gets (should be case-insensitive)
    Assert.assertEquals("new-header-value-98", mutableRequest.getHeader("header-Key-0"));
    Assert.assertEquals("new-header-value-98", mutableRequest.getHeader("header-key-0"));
    Assert.assertEquals("new-header-value-98", mutableRequest.getHeader("HEADER-KEY-0"));

    headerValuesList = Collections.list(mutableRequest.getHeaders("header-key-0"));
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-98", headerValuesList.get(0));

    headerValuesList = Collections.list(mutableRequest.getHeaders("header-KEY-0"));
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-98", headerValuesList.get(0));

    headerNames = mutableRequest.getHeaderNames();
    headerNamesList = Collections.list(headerNames);
    Assert.assertEquals(1, headerNamesList.size());
    Assert.assertTrue(headerNamesList.contains("header-key-0"));

    // Replace with a "header-Key-0" header key to test cast-insensitivity
    mutableRequest.putHeader("header-Key-0", "new-header-value-75");

    // Validate gets (should be case-insensitive)
    Assert.assertEquals("new-header-value-75", mutableRequest.getHeader("header-Key-0"));
    Assert.assertEquals("new-header-value-75", mutableRequest.getHeader("header-key-0"));
    Assert.assertEquals("new-header-value-75", mutableRequest.getHeader("HEADER-KEY-0"));

    headerValuesList = Collections.list(mutableRequest.getHeaders("header-key-0"));
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-75", headerValuesList.get(0));

    headerValuesList = Collections.list(mutableRequest.getHeaders("header-KEY-0"));
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-75", headerValuesList.get(0));

    headerNames = mutableRequest.getHeaderNames();
    headerNamesList = Collections.list(headerNames);
    Assert.assertEquals(1, headerNamesList.size());
    Assert.assertTrue(headerNamesList.contains("header-key-0"));
  }

  @Test
  public void testGetAndPutWithInitialHeaders(){
    // A more comprehensive test
    Enumeration<String> headerValues;
    Enumeration<String> headerNames;
    List<String> headerValuesList;
    List<String> headerNamesList;

    // Mock HttpServletRequest with two headers
    HttpFields headers = HttpFields.build()
        .add("header-key-0", "header-value-78.1")
        .add("header-key-0", "header-value-78.2")
        .add("header-key-2", "header-value-62.1")
        .add("header-key-2", "header-value-62.2");
    when(httpServletRequest.getHeaders()).thenReturn(headers);

    // Test getting header values from HttpServletRequest (should match the mock values above)
    Assert.assertEquals("header-value-78.1", mutableRequest.getHeader("header-key-0"));

    headerValues = mutableRequest.getHeaders("header-key-0");
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(2, headerValuesList.size());
    Assert.assertTrue(headerValuesList.contains("header-value-78.1"));
    Assert.assertTrue(headerValuesList.contains("header-value-78.2"));

    Assert.assertEquals("header-value-62.1", mutableRequest.getHeader("header-key-2"));

    headerValues = mutableRequest.getHeaders("header-key-2");
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(2, headerValuesList.size());
    Assert.assertTrue(headerValuesList.contains("header-value-62.1"));
    Assert.assertTrue(headerValuesList.contains("header-value-62.2"));


    // Test putHeader (overwrite and new values) and getHeader
    mutableRequest.putHeader("header-key-0", "new-header-value-98"); // overwrite mock
    Assert.assertEquals("new-header-value-98", mutableRequest.getHeader("header-key-0"));
    Assert.assertEquals("new-header-value-98", mutableRequest.getHeader("header-KEY-0"));

    mutableRequest.putHeader("header-key-0", "new-header-value-100"); // overwrite above
    Assert.assertEquals("new-header-value-100", mutableRequest.getHeader("header-key-0"));
    Assert.assertEquals("new-header-value-100", mutableRequest.getHeader("Header-Key-0"));

    mutableRequest.putHeader("HEADER-KEY-0", "new-header-value-999"); // overwrite above (case-insensitive)
    Assert.assertEquals("new-header-value-999", mutableRequest.getHeader("header-key-0"));
    Assert.assertEquals("new-header-value-999", mutableRequest.getHeader("Header-Key-0"));

    mutableRequest.putHeader("header-key-1", "new-header-value-54"); // new
    Assert.assertEquals("new-header-value-54", mutableRequest.getHeader("header-key-1"));
    Assert.assertEquals("new-header-value-54", mutableRequest.getHeader("HEADER-key-1"));


    // Test getHeaders
    headerValues = mutableRequest.getHeaders("header-key-0");
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-999", headerValuesList.get(0));
    headerValues = mutableRequest.getHeaders("Header-Key-0"); // Test case-insensitivity
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-999", headerValuesList.get(0));

    headerValues = mutableRequest.getHeaders("header-key-1");
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-54", headerValuesList.get(0));
    headerValues = mutableRequest.getHeaders("HEADER-key-1"); // Test case-insensitivity
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-54", headerValuesList.get(0));


    // Test getHeaderNames
    headerNames = mutableRequest.getHeaderNames();
    headerNamesList = Collections.list(headerNames);
    Assert.assertEquals(3, headerNamesList.size());
    Assert.assertTrue(headerNamesList.contains("header-key-0"));
    Assert.assertTrue(headerNamesList.contains("header-key-1"));
    Assert.assertTrue(headerNamesList.contains("header-key-2"));
  }
}
