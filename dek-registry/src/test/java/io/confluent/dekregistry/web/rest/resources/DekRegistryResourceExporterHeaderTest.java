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

package io.confluent.dekregistry.web.rest.resources;

import io.confluent.dekregistry.storage.AbstractDekRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.HttpHeaders;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class DekRegistryResourceExporterHeaderTest {

  @Test
  public void testExporterHeaderIsChecked() throws Exception {
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    AbstractDekRegistry dekRegistry = mock(AbstractDekRegistry.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    DekRegistryResource resource = new DekRegistryResource(schemaRegistry, dekRegistry);

    when(headers.getHeaderString("X-Exporter-Name")).thenReturn("test-exporter");
    when(dekRegistry.getKek(anyString(), anyBoolean())).thenReturn(null);

    resource.deleteKek(asyncResponse, headers, "test-kek", false);

    verify(headers).getHeaderString("X-Exporter-Name");
  }
}
