/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.rest.filters;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Priority;
import javax.ws.rs.container.*;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import org.eclipse.jetty.util.StringUtil;
import java.util.UUID;


@PreMatching
@Priority(0) // assign request IDs before anything else
public class RequestIdFilter implements ContainerRequestFilter, ContainerResponseFilter {

  public static final String REQUEST_ID_HEADER = "X-Request-ID";
  private static final Logger log = LoggerFactory.getLogger(RequestIdFilter.class);

  public RequestIdFilter() {
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    MultivaluedMap<String, String> headersMap = requestContext.getHeaders();
    String requestId;
    if (headersMap.containsKey(REQUEST_ID_HEADER)
        && headersMap.get(REQUEST_ID_HEADER).size() == 1
        && StringUtil.isNotBlank(headersMap.get(REQUEST_ID_HEADER).get(0))
    ){
      requestId = headersMap.get(REQUEST_ID_HEADER).get(0);
    } else {
      // generate a V4 UUID
      requestId = UUID.randomUUID().toString();
    }
    headersMap.putSingle(REQUEST_ID_HEADER, requestId);
    MDC.put("requestId", requestId);
  }

  @Override
  public void filter(final ContainerRequestContext containerRequestContext,
                     final ContainerResponseContext containerResponseContext) throws IOException {
    // Copy the request id value from the containerRequestContext to the containerResponseContext.
    // The request id in the containerRequestContext should be valid at this point.
    containerResponseContext.getHeaders().putSingle(
            REQUEST_ID_HEADER, containerRequestContext.getHeaders().get(REQUEST_ID_HEADER).get(0));
  }
}
