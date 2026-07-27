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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.UriInfo;
import org.junit.Test;

public class RestCallLoggingFilterTest {

  private final RestCallLoggingFilter filter = new RestCallLoggingFilter();

  @Test
  public void testFilterWithExporterHeader() {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);

    when(requestContext.getHeaderString("Confluent-Schema-Exporter-Name"))
        .thenReturn("test-exporter");
    when(requestContext.getMethod()).thenReturn("POST");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/subjects/test-subject/versions");

    // Should not throw any exceptions
    filter.filter(requestContext);
  }

  @Test
  public void testFilterWithoutExporterHeader() {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);

    when(requestContext.getHeaderString("Confluent-Schema-Exporter-Name"))
        .thenReturn(null);

    // Should not throw any exceptions
    filter.filter(requestContext);
  }

  @Test
  public void testFilterWithEmptyExporterHeader() {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);

    when(requestContext.getHeaderString("Confluent-Schema-Exporter-Name"))
        .thenReturn("");

    // Should not throw any exceptions
    filter.filter(requestContext);
  }
}
