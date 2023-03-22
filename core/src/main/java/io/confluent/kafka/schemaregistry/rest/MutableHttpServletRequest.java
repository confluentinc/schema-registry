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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.glassfish.jersey.internal.util.collection.StringKeyIgnoreCaseMultivaluedMap;

final class MutableHttpServletRequest extends HttpServletRequestWrapper {
  // Allows for adding and replacing custom headers in the HttpServletRequest object.
  // Only one value per custom header is allowed.
  private final StringKeyIgnoreCaseMultivaluedMap<String> customHeaders;

  public MutableHttpServletRequest(HttpServletRequest request) {
    super(request);
    this.customHeaders = new StringKeyIgnoreCaseMultivaluedMap<String>();
  }

  public void putHeader(String name, String value) {
    // Putting a new header will take precedence over existing values in HttpServletRequest.
    // Value will also overwrite any existing custom header value.
    this.customHeaders.putSingle(name, value);
  }

  public String getHeader(String name) {
    // check the custom headers first and return the value if it exists.
    String headerValue = customHeaders.getFirst(name);

    if (headerValue != null) {
      return headerValue;
    }
    // else return from into the original wrapped object
    return ((HttpServletRequest) getRequest()).getHeader(name);
  }

  public Enumeration<String> getHeaders(String name) {
    // check the custom headers first and return the value if it exists.
    List<String> headerValues = customHeaders.get(name);

    if (headerValues != null) {
      return Collections.enumeration(headerValues);
    }
    // else return from into the original wrapped object
    return ((HttpServletRequest) getRequest()).getHeaders(name);
  }

  public Enumeration<String> getHeaderNames() {
    // Return the unique (case-insensitive) header names in customHeaders and HttpServletRequest
    HttpServletRequest request = (HttpServletRequest)getRequest();
    Set<String> set = new HashSet<String>();
    Set<String> usedHeaders = new HashSet<String>();

    // add the custom headers
    for (String key : customHeaders.keySet()) {
      // Only add custom header it hasn't already been added (case-insensitive)
      String keyLower = key.toLowerCase();
      if (!usedHeaders.contains(keyLower)) {
        set.add(key);
        usedHeaders.add(keyLower);
      }
    }

    // add the HttpServletRequest headers
    Enumeration<String> e = request.getHeaderNames();
    while (e.hasMoreElements()) {
      // add the names of the request headers into the list
      String key = e.nextElement();
      String keyLower = key.toLowerCase();
      // Only add custom header it hasn't already been added (case insensitive)
      if (!usedHeaders.contains(keyLower)) {
        set.add(key);
        usedHeaders.add(keyLower);
      }
    }

    // create an enumeration from the set and return
    return Collections.enumeration(set);
  }
}