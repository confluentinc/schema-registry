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


import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;

import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.server.Request;
import org.slf4j.MDC;

public class RequestIdHandler extends HandlerWrapper {
  public static final String REQUEST_ID_HEADER = "X-Request-ID";

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request,
                     HttpServletResponse response) throws IOException, ServletException {
    // Clear MDC at beginning of each request to remove stale values
    MDC.clear();
    MutableHttpServletRequest mutableRequest = new MutableHttpServletRequest(request);
    List<String> inputHeaders = Collections.list(baseRequest.getHeaders(REQUEST_ID_HEADER));

    String requestId = getRequestId(inputHeaders);

    // Add request ID to request and response header and MDC
    mutableRequest.putHeader(REQUEST_ID_HEADER, requestId);
    response.setHeader(REQUEST_ID_HEADER, requestId);
    MDC.put("requestId", requestId);

    // Call the next handler in the chain
    super.handle(target, baseRequest, mutableRequest, response);
  }

  protected String getRequestId(List<String> headers) {
    if (headers.size() == 1 && StringUtil.isNotBlank(headers.get(0))) {
      return headers.get(0);
    }
    // generate a V4 UUID
    return UUID.randomUUID().toString();
  }
}
