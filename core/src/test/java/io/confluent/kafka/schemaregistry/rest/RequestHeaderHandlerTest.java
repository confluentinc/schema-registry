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
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Blocker.Callback;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.MDC;

import static io.confluent.kafka.schemaregistry.client.rest.RestService.X_FORWARD_HEADER;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RequestHeaderHandlerTest {
  RequestHeaderHandler requestHeaderHandler = new RequestHeaderHandler();

  @Mock
  Request request;
  @Mock
  Response response;
  @Mock
  Callback callback;

  @Before
  public void setup(){
    reset(request, response);
    MDC.clear();
  }

  @Test
  public void testRequestHandlerWith1RequestId() throws Exception {
    RequestHeaderHandler requestHeaderHandlerSpy = spy(new RequestHeaderHandler());
    HttpFields.Mutable headers = HttpFields.build().add(RequestHeaderHandler.X_REQUEST_ID_HEADER, "request-ID-4329");
    when(request.getHeaders()).thenReturn(headers);
    headers = HttpFields.build();
    when(response.getHeaders()).thenReturn(headers);
    doReturn("127.0.0.1").when(requestHeaderHandlerSpy).getRemoteAddr(request);

    requestHeaderHandlerSpy.handle(request, response, callback);

    verify(requestHeaderHandlerSpy, times(1)).getRequestId(Collections.singletonList("request-ID-4329"));
    verify(request, times(2)).getHeaders();
    //verify(response, times(1)).setHeader(RequestHeaderHandler.X_REQUEST_ID_HEADER, "request-ID-4329");

    // Validate that the MDC.requestId was set
    Assert.assertEquals("request-ID-4329", MDC.get("requestId"));
  }

  @Test
  public void testRequestHandlerWithoutRequestId() throws Exception {
    ArgumentCaptor<String> requestIdCaptor = ArgumentCaptor.forClass(String.class);;

    RequestHeaderHandler requestHeaderHandlerSpy = spy(new RequestHeaderHandler());
    HttpFields.Mutable headers = HttpFields.build();
    when(request.getHeaders()).thenReturn(headers);
    when(response.getHeaders()).thenReturn(headers);
    doReturn("127.0.0.1").when(requestHeaderHandlerSpy).getRemoteAddr(request);

    requestHeaderHandlerSpy.handle(request, response, callback);

    verify(requestHeaderHandlerSpy, times(1)).getRequestId(Collections.emptyList());
    verify(request, times(2)).getHeaders();
    //verify(response, times(1)).setHeader(eq(RequestHeaderHandler.X_REQUEST_ID_HEADER), requestIdCaptor.capture());

    /*
    String generatedRequestId = requestIdCaptor.getValue();
    // Validate that the MDC.requestId was set
    Assert.assertEquals(generatedRequestId, MDC.get("requestId"));
    validateUuid(generatedRequestId);
    */
  }

  @Test
  public void testRequestHandlerWithMultipleRequestId() throws Exception {
    ArgumentCaptor<String> requestIdCaptor = ArgumentCaptor.forClass(String.class);;

    RequestHeaderHandler requestHeaderHandlerSpy = spy(new RequestHeaderHandler());
    HttpFields.Mutable headers = HttpFields.build()
        .add(RequestHeaderHandler.X_REQUEST_ID_HEADER, "request-ID6")
        .add(RequestHeaderHandler.X_REQUEST_ID_HEADER, "request-ID4");
    when(request.getHeaders()).thenReturn(headers);
    headers = HttpFields.build();
    when(response.getHeaders()).thenReturn(headers);
    doReturn("127.0.0.1").when(requestHeaderHandlerSpy).getRemoteAddr(request);

    requestHeaderHandlerSpy.handle(request, response, callback);

    verify(requestHeaderHandlerSpy, times(1)).getRequestId(Arrays.asList("request-ID6", "request-ID4"));
    verify(request, times(2)).getHeaders();
    //verify(response, times(1)).setHeader(eq(RequestHeaderHandler.X_REQUEST_ID_HEADER), requestIdCaptor.capture());

    /*
    String generatedRequestId = requestIdCaptor.getValue();
    // Validate that the MDC.requestId was set
    Assert.assertEquals(generatedRequestId, MDC.get("requestId"));
    validateUuid(generatedRequestId);
    */
  }

  @Test
  public void testAddXRequestIdToRequest() {
    MutableRequest mutableRequest = new MutableRequest(request);

    HttpFields.Mutable headers = HttpFields.build().add(RequestHeaderHandler.X_REQUEST_ID_HEADER, "request-ID-4329");
    when(request.getHeaders()).thenReturn(headers);
    headers = HttpFields.build();
    when(response.getHeaders()).thenReturn(headers);

    RequestHeaderHandler requestHeaderHandler = new RequestHeaderHandler();
    requestHeaderHandler.addXRequestIdToRequest(mutableRequest, response);

    verify(request, times(1)).getHeaders();
    //verify(response, times(1)).setHeader(RequestHeaderHandler.X_REQUEST_ID_HEADER, "request-ID-4329");
    String requestId = mutableRequest.getHeader(RequestHeaderHandler.X_REQUEST_ID_HEADER);
    Assert.assertEquals("request-ID-4329", requestId);

    // Validate that the MDC.requestId was set
    Assert.assertEquals("request-ID-4329", MDC.get("requestId"));
  }

  @Test
  public void testAddXForwardedForToRequest() {
    MutableRequest mutableRequest = new MutableRequest(request);
    RequestHeaderHandler requestHeaderHandlerSpy = spy(new RequestHeaderHandler());
    doReturn("127.0.0.1").when(requestHeaderHandlerSpy).getRemoteAddr(request);

    // Forwarded request
    HttpFields headers = HttpFields.build();
    when(request.getHeaders()).thenReturn(headers);
    requestHeaderHandlerSpy.addXForwardedForToRequest(mutableRequest, request);
    String callerIp = mutableRequest.getHeader(RequestHeaderHandler.X_FORWARDED_FOR_HEADER);
    Assert.assertEquals("127.0.0.1", callerIp);

    // Not forwarded request
    headers = HttpFields.build().add(X_FORWARD_HEADER, "true");
    when(request.getHeaders()).thenReturn(headers);
    requestHeaderHandlerSpy.addXForwardedForToRequest(mutableRequest, request);
    callerIp = mutableRequest.getHeader(RequestHeaderHandler.X_FORWARDED_FOR_HEADER);
    Assert.assertEquals("127.0.0.1", callerIp);
  }

  @Test
  public void testAddXForwardedForToRequestOverridesHeader() {
    MutableRequest mutableRequest = new MutableRequest(request);
    RequestHeaderHandler requestHeaderHandlerSpy = spy(new RequestHeaderHandler());
    doReturn("127.0.0.1").when(requestHeaderHandlerSpy).getRemoteAddr(request);
    HttpFields headers = HttpFields.build();
    when(request.getHeaders()).thenReturn(headers);
    mutableRequest.putHeader(RequestHeaderHandler.X_FORWARDED_FOR_HEADER, "192.168.1.10");

    requestHeaderHandlerSpy.addXForwardedForToRequest(mutableRequest, request);
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
