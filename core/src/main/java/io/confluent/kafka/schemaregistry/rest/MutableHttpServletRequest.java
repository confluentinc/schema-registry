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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

final class MutableHttpServletRequest extends HttpServletRequestWrapper {
  // Allows for adding and replacing custom headers in the HttpServletRequest object.
  private final Map<String, String> customHeaders;

  public MutableHttpServletRequest(HttpServletRequest request) {
    super(request);
    this.customHeaders = new HashMap<String, String>();
  }

  public void putHeader(String name, String value) {
    // Putting a new header will take precedence over existing values in HttpServletRequest
    this.customHeaders.put(name, value);
  }

  public String getHeader(String name) {
    // check the custom headers first
    String headerValue = customHeaders.get(name);

    if (headerValue != null) {
      return headerValue;
    }
    // else return from into the original wrapped object
    return ((HttpServletRequest) getRequest()).getHeader(name);
  }

  public Enumeration<String> getHeaders(String name) {
    // check the custom headers first
    String headerValue = customHeaders.get(name);

    if (headerValue != null) {
      return Collections.enumeration(Collections.singletonList(headerValue));
    }
    // else return from into the original wrapped object
    return ((HttpServletRequest) getRequest()).getHeaders(name);
  }

  public Enumeration<String> getHeaderNames() {
    // create a set of the custom header names
    Set<String> set = new HashSet<String>(customHeaders.keySet());

    // now add the headers from the wrapped request object
    Enumeration<String> e = ((HttpServletRequest) getRequest()).getHeaderNames();
    while (e.hasMoreElements()) {
      // add the names of the request headers into the list
      String n = e.nextElement();
      set.add(n);
    }

    // create an enumeration from the set and return
    return Collections.enumeration(set);
  }
}