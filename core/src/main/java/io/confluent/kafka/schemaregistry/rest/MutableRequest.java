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
import java.util.Enumeration;
import java.util.List;

import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.server.Request;
import org.glassfish.jersey.internal.util.collection.StringKeyIgnoreCaseMultivaluedMap;

/**
 * Allows for adding and replacing custom headers in the HttpServletRequest object.
 * Only one value per custom header is allowed.
 */
public final class MutableRequest extends Request.Wrapper {
  private final StringKeyIgnoreCaseMultivaluedMap<String> customHeaders;

  public MutableRequest(Request request) {
    super(request);
    this.customHeaders = new StringKeyIgnoreCaseMultivaluedMap<String>();
  }

  @Override
  public HttpFields getHeaders() {
    HttpFields.Mutable httpFields = HttpFields.build(super.getHeaders());
    for (String key : customHeaders.keySet()) {
      List<String> values = customHeaders.get(key);
      for (String value : values) {
        httpFields.put(key, value);
      }
    }
    return httpFields;
  }

  public Enumeration<String> getHeaders(String name) {
    // check the custom headers first and return the value if it exists.
    List<String> headerValues = customHeaders.get(name);

    if (headerValues != null) {
      return Collections.enumeration(headerValues);
    }
    // else return from into the original wrapped object
    List<String> fields = super.getHeaders()
        .getFields(name)
        .stream()
        .map(HttpField::getValue)
        .toList();
    return Collections.enumeration(fields);
  }

  public String getHeader(String name) {
    // check the custom headers first and return the value if it exists.
    String headerValue = customHeaders.getFirst(name);

    if (headerValue != null) {
      return headerValue;
    }
    // else return from into the original wrapped object
    return super.getHeaders().get(name);
  }

  /**
   * Get the unique (case-insensitive) header names in customHeaders and HttpServletRequest.
   */
  public Enumeration<String> getHeaderNames() {
    return Collections.enumeration(getHeaders().getFieldNamesCollection());
  }

  /**
   * Puts a new header will take precedence over existing header in the HttpServletRequest object.
   */
  public void putHeader(String name, String value) {
    // Value will also overwrite any existing custom header value.
    this.customHeaders.putSingle(name, value);
  }
}
