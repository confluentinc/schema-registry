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
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.MDC;

import static io.confluent.kafka.schemaregistry.client.rest.RestService.X_FORWARD_HEADER;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RequestHeaderHandlerTest {
  RequestHeaderHandler requestHeaderHandler = new RequestHeaderHandler();

  @Mock
  Request baseRequest;
  @Mock
  HttpServletRequest request;

  @Mock
  HttpServletResponse response;

  @Before
  public void setup(){
    reset(baseRequest, request, response);
    MDC.clear();
  }

  @Test
  public void testRequestHandlerWith1RequestId() throws IOException, ServletException {
    RequestHeaderHandler requestHeaderHandlerSpy = spy(new RequestHeaderHandler());
    Enumeration<String> headers = Collections.enumeration(Collections.singletonList("request-ID-4329"));
    when(baseRequest.getHeaders(RequestHeaderHandler.X_REQUEST_ID_HEADER)).thenReturn(headers);

    requestHeaderHandlerSpy.handle("/subjects/subject-2/versions", baseRequest, request, response);

    verify(requestHeaderHandlerSpy, times(1)).getRequestId(Collections.singletonList("request-ID-4329"));
    verify(baseRequest, times(1)).getHeaders(RequestHeaderHandler.X_REQUEST_ID_HEADER);
    verify(response, times(1)).setHeader(RequestHeaderHandler.X_REQUEST_ID_HEADER, "request-ID-4329");

    // Validate that the MDC.requestId was set
    Assert.assertEquals("request-ID-4329", MDC.get("requestId"));
  }

  @Test
  public void testRequestHandlerWithoutRequestId() throws IOException, ServletException {
    ArgumentCaptor<String> requestIdCaptor = ArgumentCaptor.forClass(String.class);;

    RequestHeaderHandler requestHeaderHandlerSpy = spy(new RequestHeaderHandler());
    Enumeration<String> headers = Collections.enumeration(Collections.emptyList());
    when(baseRequest.getHeaders(RequestHeaderHandler.X_REQUEST_ID_HEADER)).thenReturn(headers);

    requestHeaderHandlerSpy.handle("/subjects/subject-2/versions", baseRequest, request, response);

    verify(requestHeaderHandlerSpy, times(1)).getRequestId(Collections.emptyList());
    verify(baseRequest, times(1)).getHeaders(RequestHeaderHandler.X_REQUEST_ID_HEADER);
    verify(response, times(1)).setHeader(eq(RequestHeaderHandler.X_REQUEST_ID_HEADER), requestIdCaptor.capture());

    String generatedRequestId = requestIdCaptor.getValue();
    // Validate that the MDC.requestId was set
    Assert.assertEquals(generatedRequestId, MDC.get("requestId"));
    validateUuid(generatedRequestId);
  }

  @Test
  public void testRequestHandlerWithMultipleRequestId() throws IOException, ServletException {
    ArgumentCaptor<String> requestIdCaptor = ArgumentCaptor.forClass(String.class);;

    RequestHeaderHandler requestHeaderHandlerSpy = spy(new RequestHeaderHandler());
    Enumeration<String> headers = Collections.enumeration(Arrays.asList("request-ID6", "request-ID4"));
    when(baseRequest.getHeaders(RequestHeaderHandler.X_REQUEST_ID_HEADER)).thenReturn(headers);

    requestHeaderHandlerSpy.handle("/subjects/subject-2/versions", baseRequest, request, response);

    verify(requestHeaderHandlerSpy, times(1)).getRequestId(Arrays.asList("request-ID6", "request-ID4"));
    verify(baseRequest, times(1)).getHeaders(RequestHeaderHandler.X_REQUEST_ID_HEADER);
    verify(response, times(1)).setHeader(eq(RequestHeaderHandler.X_REQUEST_ID_HEADER), requestIdCaptor.capture());

    String generatedRequestId = requestIdCaptor.getValue();
    // Validate that the MDC.requestId was set
    Assert.assertEquals(generatedRequestId, MDC.get("requestId"));
    validateUuid(generatedRequestId);
  }

  @Test
  public void testAddXRequestIdToRequest() {
    MutableHttpServletRequest mutableRequest = new MutableHttpServletRequest(request);

    Enumeration<String> headers = Collections.enumeration(Collections.singletonList("request-ID-4329"));
    when(baseRequest.getHeaders(RequestHeaderHandler.X_REQUEST_ID_HEADER)).thenReturn(headers);

    RequestHeaderHandler requestHeaderHandler = new RequestHeaderHandler();
    requestHeaderHandler.addXRequestIdToRequest(baseRequest, mutableRequest, response);

    verify(baseRequest, times(1)).getHeaders(RequestHeaderHandler.X_REQUEST_ID_HEADER);
    verify(response, times(1)).setHeader(RequestHeaderHandler.X_REQUEST_ID_HEADER, "request-ID-4329");
    String requestId = mutableRequest.getHeader(RequestHeaderHandler.X_REQUEST_ID_HEADER);
    Assert.assertEquals("request-ID-4329", requestId);

    // Validate that the MDC.requestId was set
    Assert.assertEquals("request-ID-4329", MDC.get("requestId"));
  }

  @Test
  public void testAddXForwardedForToRequest() {
    MutableHttpServletRequest mutableRequest = new MutableHttpServletRequest(request);
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    RequestHeaderHandler requestHeaderHandler = new RequestHeaderHandler();

    // Forwarded request
    when(baseRequest.getHeader(X_FORWARD_HEADER)).thenReturn(null);
    requestHeaderHandler.addXForwardedForToRequest(baseRequest, mutableRequest, request);
    String callerIp = mutableRequest.getHeader(RequestHeaderHandler.X_FORWARDED_FOR_HEADER);
    Assert.assertEquals("127.0.0.1", callerIp);

    // Not forwarded request
    when(baseRequest.getHeader(X_FORWARD_HEADER)).thenReturn("true");
    Mockito.lenient().when(request.getRemoteAddr()).thenReturn("192.168.1.10");
    requestHeaderHandler.addXForwardedForToRequest(baseRequest, mutableRequest, request);
    callerIp = mutableRequest.getHeader(RequestHeaderHandler.X_FORWARDED_FOR_HEADER);
    Assert.assertEquals("127.0.0.1", callerIp);
  }

  @Test
  public void testAddXForwardedForToRequestOverridesHeader() {
    MutableHttpServletRequest mutableRequest = new MutableHttpServletRequest(request);
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    mutableRequest.putHeader(RequestHeaderHandler.X_FORWARDED_FOR_HEADER, "192.168.1.10");

    requestHeaderHandler.addXForwardedForToRequest(baseRequest, mutableRequest, request);
    String callerIp = mutableRequest.getHeader(RequestHeaderHandler.X_FORWARDED_FOR_HEADER);
    Assert.assertEquals("127.0.0.1", callerIp);
  }

  @Test
  public void testGetRequestIdWith1RequestId() {
    // A single non-blank request id should be taken as is
    String requestId = requestHeaderHandler.getRequestId(Collections.singletonList("request-ID-1234"));

    Assert.assertEquals(
        "Request ID must not change",
        "request-ID-1234",
        requestId
    );
  }

  @Test
  public void testGetRequestIdWithoutRequestId() {
    // No request id should result in a new request UUID being generated
    String requestId = requestHeaderHandler.getRequestId(Collections.emptyList());
    validateUuid(requestId);
  }

  @Test
  public void testGetRequestIdWithMultipleRequestId() {
    // Multiple request ids in the request header should result in a new request UUID being generated
    String requestId = requestHeaderHandler.getRequestId(Arrays.asList("request-ID1", "request-ID2"));
    validateUuid(requestId);
  }

  @Test
  public void testGetRequestIdWithBlankRequestId() {
    // Blank request ids should result in a new request UUID being generated
    // A single non-blank request id should be taken as is
    String requestId = requestHeaderHandler.getRequestId(Collections.singletonList("   "));
    validateUuid(requestId);
  }

  private void validateUuid(String requestId) {
    Assert.assertEquals(
        "Request ID is not a valid UUID",
        UUID.fromString(requestId).toString(),
        requestId
    );
  }
}
