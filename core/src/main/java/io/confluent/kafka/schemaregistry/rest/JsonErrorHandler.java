/*
 * Copyright 2019-2020 Confluent Inc.
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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.ErrorHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;

public class JsonErrorHandler extends ErrorHandler {
  @Override
  public void handle(final String target,
                     final Request baseRequest,
                     final HttpServletRequest request,
                     final HttpServletResponse response) throws IOException {

    final String method = request.getMethod();
    if (!HttpMethod.GET.is(method) && !HttpMethod.POST.is(method) && !HttpMethod.PUT.is(method)
        && !HttpMethod.HEAD.is(method)) {
      baseRequest.setHandled(true);
      return;
    }

    response.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());

    final String reason = (response instanceof Response) ? ((Response) response).getReason() : null;

    handleErrorPage(request, getAcceptableWriter(baseRequest, request, response), response
        .getStatus(), reason);

    baseRequest.setHandled(true);
    baseRequest.getResponse().closeOutput();
  }

  @Override
  protected void writeErrorPage(HttpServletRequest request, Writer writer, int code, String
      message, boolean showStacks) throws IOException {
    final String error = message == null ? HttpStatus.getMessage(code) : message;
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    root.put("error_code", code);
    root.put("message", error);
    writer.write(root.toString());
  }
}
