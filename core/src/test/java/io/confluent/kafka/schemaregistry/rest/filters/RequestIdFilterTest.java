/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.rest.filters;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RequestIdFilterTest {
  RequestIdFilter requestIdFilter = new RequestIdFilter();

  @Mock
  ContainerRequestContext requestContext;

  @Mock
  ContainerResponseContext responseContext;

  @Before
  public void setup(){
    reset(requestContext, responseContext);
    MultivaluedMap<String, String> requestHeadersMap = new MultivaluedHashMap<>();
    when(requestContext.getHeaders()).thenReturn(requestHeadersMap);
    MultivaluedMap<String, Object> responseHeadersMap = new MultivaluedHashMap<>();
    when(responseContext.getHeaders()).thenReturn(responseHeadersMap);
  }

  @Test
  public void testRequestFilterWithoutRequestId() throws IOException {
    // No request id should result in a new request UUID being generated
    requestIdFilter.filter(requestContext);

    validateUuidRequestId(requestContext);
  }

  @Test
  public void testRequestFilterWith1RequestId() throws IOException {
    // A single non-blank request id should be taken as is
    requestContext.getHeaders().add(RequestIdFilter.REQUEST_ID_HEADER, "request-ID-1234");

    requestIdFilter.filter(requestContext);

    List<String> requestIdList = requestContext.getHeaders().get(RequestIdFilter.REQUEST_ID_HEADER);
    Assert.assertEquals(
      "There should only be one request ID",
      1,
      requestIdList.size()
    );
    Assert.assertEquals(
      "Request ID must not change",
      "request-ID-1234",
      requestIdList.get(0)
    );
  }

  @Test
  public void testRequestFilterWithMultipleRequestId() throws IOException {
    // Multiple request ids in the request header should result in a new request UUID being generated
    requestContext.getHeaders().add(RequestIdFilter.REQUEST_ID_HEADER, "request-ID1");
    requestContext.getHeaders().add(RequestIdFilter.REQUEST_ID_HEADER, "request-ID2");

    requestIdFilter.filter(requestContext);

    validateUuidRequestId(requestContext);
  }

  @Test
  public void testRequestFilterWithBlankRequestId() throws IOException {
    // Blank request ids should result in a new request UUID being generated
    requestContext.getHeaders().add(RequestIdFilter.REQUEST_ID_HEADER, "  ");

    requestIdFilter.filter(requestContext);

    validateUuidRequestId(requestContext);
  }

  @Test
  public void testResponseFilter() throws IOException {
    requestContext.getHeaders().add(RequestIdFilter.REQUEST_ID_HEADER, "request-ID-5432");

    requestIdFilter.filter(requestContext, responseContext);

    // Validate that the request id now exists in the responseContext
    List<Object> requestIdList = responseContext.getHeaders().get(RequestIdFilter.REQUEST_ID_HEADER);
    Assert.assertEquals(
            "There should only be one request ID",
            1,
            requestIdList.size()
    );
    Assert.assertEquals(
            "Request ID must not change",
            "request-ID-5432",
            requestIdList.get(0)
    );
  }

  @Test
  public void testResponseFilterUuid() throws IOException {
    requestContext.getHeaders().add(RequestIdFilter.REQUEST_ID_HEADER, "24b9afef-3cc7-4e32-944d-f89253c1b64b");

    requestIdFilter.filter(requestContext, responseContext);

    // Validate that the request id now exists in the responseContext
    List<Object> requestIdList = responseContext.getHeaders().get(RequestIdFilter.REQUEST_ID_HEADER);
    Assert.assertEquals(
            "There should only be one request ID",
            1,
            requestIdList.size()
    );
    Assert.assertEquals(
            "Request ID must not change",
            "24b9afef-3cc7-4e32-944d-f89253c1b64b",
            requestIdList.get(0)
    );
  }

  private void validateUuidRequestId(ContainerRequestContext requestContext){
    // Validate that one UUID request id string exists in the requestContext
    List<String> requestIdList = requestContext.getHeaders().get(RequestIdFilter.REQUEST_ID_HEADER);
    Assert.assertEquals(
            "There should only be one request ID",
            1,
            requestIdList.size()
    );
    String requestId = requestIdList.get(0);
    Assert.assertEquals(
            "Request ID is not a valid UUID",
            UUID.fromString(requestId).toString(),
            requestId
    );
  }
}
