/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.resources;

import org.eclipse.jetty.util.StringUtil;

import javax.ws.rs.core.HttpHeaders;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequestHeaderBuilder {

  public Map<String, String> buildRequestHeaders(
      HttpHeaders httpHeaders,
      List<String> whitelistedHeaders
  ) {
    Map<String, String> headerProperties = new HashMap<>();
    addStaticHeaders(headerProperties, httpHeaders);
    addWhitelistedHeaders(headerProperties, httpHeaders, whitelistedHeaders);
    return headerProperties;
  }

  private void addWhitelistedHeaders(
      Map<String, String> headerProperties,
      HttpHeaders httpHeaders,
      List<String> whitelistedHeaders
  ) {
    if (whitelistedHeaders != null) {
      whitelistedHeaders
          .stream()
          .forEach(
              headerToForward -> addIfNotEmpty(httpHeaders, headerProperties, headerToForward));
    }
  }

  private void addStaticHeaders(Map<String, String> headerProperties, HttpHeaders httpHeaders) {
    addIfNotEmpty(httpHeaders, headerProperties, "Content-Type");
    addIfNotEmpty(httpHeaders, headerProperties, "Accept");
    addIfNotEmpty(httpHeaders, headerProperties, "Authorization");
    addIfNotEmpty(httpHeaders, headerProperties, "X-Request-ID");
  }

  private void addIfNotEmpty(
      HttpHeaders incomingHeaders, Map<String, String> headerProperties,
      String header) {
    String headerString = incomingHeaders.getHeaderString(header);
    if (StringUtil.isNotBlank(headerString)) {
      headerProperties.put(header, headerString);
    }
  }
}
