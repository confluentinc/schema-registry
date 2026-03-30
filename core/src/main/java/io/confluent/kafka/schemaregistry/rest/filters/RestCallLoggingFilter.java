/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.filters;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Priority(Priorities.USER)
public class RestCallLoggingFilter implements ContainerRequestFilter {
  private static final Logger log =
      LoggerFactory.getLogger(RestCallLoggingFilter.class);
  private static final String EXPORTER_HEADER = "Confluent-Schema-Exporter-Name";

  @Override
  public void filter(ContainerRequestContext requestContext) {
    String exporterName = requestContext.getHeaderString(EXPORTER_HEADER);
    if (exporterName != null && !exporterName.isEmpty()) {
      log.info("Request from exporter: {}, {} {}",
          exporterName,
          requestContext.getMethod(),
          requestContext.getUriInfo().getPath());
    }
  }
}
