/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.rest;

import org.eclipse.jetty.server.Request;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MutableHttpServletRequestTest {

  MutableHttpServletRequest mutableRequest;

  @Mock
  HttpServletRequest httpServletRequest;

  @Before
  public void setup(){
    reset(httpServletRequest);
    mutableRequest = new MutableHttpServletRequest(httpServletRequest);
  }

  @Test
  public void testGetWithNoInitialHeaders() {
    String headerValue;
    Enumeration<String> headerValues;
    Enumeration<String> headerNames;

    // Mock no header return values from HttpServletRequest
    when(httpServletRequest.getHeader("header-key-0")).thenReturn(null);
    when(httpServletRequest.getHeaders("header-key-0")).thenReturn(Collections.emptyEnumeration());
    when(httpServletRequest.getHeaderNames()).thenReturn(Collections.emptyEnumeration());

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
    String headerValue;
    Enumeration<String> headerValues;
    Enumeration<String> headerNames;
    List<String> headerValuesList;
    List<String> headerNamesList;

    // Mock no header return values from HttpServletRequest
    when(httpServletRequest.getHeader("header-key-0")).thenReturn(null);
    when(httpServletRequest.getHeaders("header-key-0")).thenReturn(Collections.emptyEnumeration());
    when(httpServletRequest.getHeaderNames()).thenReturn(Collections.emptyEnumeration());

    // Add a "header-key-0" header
    mutableRequest.putHeader("header-key-0", "new-header-value-98");
    headerValue = mutableRequest.getHeader("header-key-0");
    Assert.assertEquals("new-header-value-98", headerValue);

    // Validate gets
    headerValue = mutableRequest.getHeader("header-key-0");
    Assert.assertEquals("new-header-value-98", headerValue);

    headerValues = mutableRequest.getHeaders("header-key-0");
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-98", headerValuesList.get(0));

    headerNames = mutableRequest.getHeaderNames();
    headerNamesList = Collections.list(headerNames);
    Assert.assertEquals(1, headerNamesList.size());
    Assert.assertTrue(headerNamesList.contains("header-key-0"));
  }

  @Test
  public void testGetAndPutWithInitialHeaders(){
    // A more comprehensive test
    String headerValue;
    Enumeration<String> headerValues;
    Enumeration<String> headerNames;
    List<String> headerValuesList;
    List<String> headerNamesList;

    // Mock HttpServletRequest with two headers
    when(httpServletRequest.getHeader("header-key-0")).thenReturn("header-value-78.1");
    when(httpServletRequest.getHeaders("header-key-0")).thenReturn(
        Collections.enumeration(Arrays.asList("header-value-78.1", "header-value-78.2")));
    when(httpServletRequest.getHeaderNames()).thenReturn(
        Collections.enumeration(Collections.singletonList("header-key-0")));

    when(httpServletRequest.getHeader("header-key-2")).thenReturn("header-value-62.1");
    when(httpServletRequest.getHeaders("header-key-2")).thenReturn(
        Collections.enumeration(Arrays.asList("header-value-62.1", "header-value-62.2")));
    when(httpServletRequest.getHeaderNames()).thenReturn(
        Collections.enumeration(Collections.singletonList("header-key-2")));


    // Test getting header values from HttpServletRequest (should match the mock values above)
    headerValue = mutableRequest.getHeader("header-key-0");
    Assert.assertEquals("header-value-78.1", headerValue);

    headerValues = mutableRequest.getHeaders("header-key-0");
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(2, headerValuesList.size());
    Assert.assertTrue(headerValuesList.contains("header-value-78.1"));
    Assert.assertTrue(headerValuesList.contains("header-value-78.2"));

    headerValue = mutableRequest.getHeader("header-key-2");
    Assert.assertEquals("header-value-62.1", headerValue);

    headerValues = mutableRequest.getHeaders("header-key-2");
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(2, headerValuesList.size());
    Assert.assertTrue(headerValuesList.contains("header-value-62.1"));
    Assert.assertTrue(headerValuesList.contains("header-value-62.2"));


    // Test putHeader (overwrite and new values) and getHeader
    mutableRequest.putHeader("header-key-0", "new-header-value-98"); // overwrite mock
    headerValue = mutableRequest.getHeader("header-key-0");
    Assert.assertEquals("new-header-value-98", headerValue);

    mutableRequest.putHeader("header-key-0", "new-header-value-100"); // overwrite above
    headerValue = mutableRequest.getHeader("header-key-0");
    Assert.assertEquals("new-header-value-100", headerValue);

    mutableRequest.putHeader("header-key-1", "new-header-value-54"); // new
    headerValue = mutableRequest.getHeader("header-key-1");
    Assert.assertEquals("new-header-value-54", headerValue);


    // Test getHeaders
    headerValues = mutableRequest.getHeaders("header-key-0");
    headerValuesList = Collections.list(headerValues);
    Assert.assertEquals(1, headerValuesList.size());
    Assert.assertEquals("new-header-value-100", headerValuesList.get(0));

    headerValues = mutableRequest.getHeaders("header-key-1");
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
