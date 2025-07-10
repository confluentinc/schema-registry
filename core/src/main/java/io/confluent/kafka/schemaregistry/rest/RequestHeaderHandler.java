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


import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static io.confluent.kafka.schemaregistry.client.rest.RestService.X_FORWARD_HEADER;

public class RequestHeaderHandler extends Handler.Wrapper {
  public static final String X_REQUEST_ID_HEADER = "X-Request-ID";
  public static final String X_FORWARDED_FOR_HEADER = "X-Forwarded-For";
  private static final Logger log = LoggerFactory.getLogger(RequestHeaderHandler.class);

  @Override
  public boolean handle(Request request, Response response, Callback callback) throws Exception {
    // Clear MDC at the beginning of each request to remove stale values
    MDC.clear();
    MutableRequest mutableRequest = new MutableRequest(request);
    addXRequestIdToRequest(mutableRequest, response);
    addXForwardedForToRequest(mutableRequest, request);

    // Call the next handler in the chain
    return super.handle(mutableRequest, response, callback);
  }

  protected void addXRequestIdToRequest(MutableRequest mutableRequest,
                                        Response response) {
    List<String> inputHeaders = Collections.list(mutableRequest.getHeaders(X_REQUEST_ID_HEADER));
    String requestId = getRequestId(inputHeaders);

    // Add request ID to request and response header and MDC
    mutableRequest.putHeader(X_REQUEST_ID_HEADER, requestId);
    response.getHeaders().add(X_REQUEST_ID_HEADER, requestId);
    MDC.put("requestId", requestId);
  }

  protected void addXForwardedForToRequest(MutableRequest mutableRequest,
                                           Request request) {
    // Do not propagate on leader call, or it would override follower IP
    if (request.getHeaders().get(X_FORWARD_HEADER) == null) {
      mutableRequest.putHeader(X_FORWARDED_FOR_HEADER, getRemoteAddr(request));
    }
    log.info("Forwarded for header in RequestHeaderHandler: {}",
        mutableRequest.getHeader(X_FORWARDED_FOR_HEADER));
  }

  protected String getRequestId(List<String> headers) {
    if (headers.size() == 1 && StringUtil.isNotBlank(headers.get(0))) {
      return headers.get(0);
    }
    // generate a V4 UUID
    return UUID.randomUUID().toString();
  }

  protected String getRemoteAddr(Request request) {
    return Request.getRemoteAddr(request);
  }
}
