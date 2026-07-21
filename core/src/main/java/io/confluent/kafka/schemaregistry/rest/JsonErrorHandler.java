/*
 * Copyright 2019 Confluent Inc.
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
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.List;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.MimeTypes.Type;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.util.Callback;

public class JsonErrorHandler extends ErrorHandler {
  @Override
  public boolean handle(Request request, Response response, Callback callback) throws Exception {

    final String method = request.getMethod();
    if (!HttpMethod.GET.is(method) && !HttpMethod.POST.is(method) && !HttpMethod.PUT.is(method)
        && !HttpMethod.HEAD.is(method)) {
      callback.succeeded();
      return true;
    }

    return super.handle(request, response, callback);
  }

  @Override
  protected boolean generateAcceptableResponse(Request request, Response response,
      Callback callback, String contentType, List<Charset> charsets, int code,
      String message, Throwable cause) throws IOException {
    return super.generateAcceptableResponse(request, response, callback,
        Type.APPLICATION_JSON.asString(), charsets, code, message, cause);
  }

  @Override
  protected void writeErrorJson(Request request, PrintWriter writer, int code,
      String message, Throwable cause, boolean showStacks) {
    final String error = message == null ? HttpStatus.getMessage(code) : message;
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    root.put("error_code", code);
    root.put("message", error);
    writer.write(root.toString());
  }
}
