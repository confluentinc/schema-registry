/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

public class RequestHeaderBuilder {

  public Map<String, String> buildRequestHeaders(HttpHeaders httpHeaders) {
    Map<String, String> headerProperties = new HashMap<>();
    addIfNotEmpty(httpHeaders, headerProperties, "Content-Type");
    addIfNotEmpty(httpHeaders, headerProperties, "Accept");
    addIfNotEmpty(httpHeaders, headerProperties, "Authorization");
    return headerProperties;
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
