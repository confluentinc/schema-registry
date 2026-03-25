/*
 * Copyright 2026 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.HttpHeaders;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class SubjectVersionsResourceExporterHeaderTest {

  @Test
  public void testExporterHeaderIsChecked() throws Exception {
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    AsyncResponse asyncResponse = mock(AsyncResponse.class);

    Properties props = new Properties();
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    when(schemaRegistry.config()).thenReturn(config);

    SubjectVersionsResource resource = new SubjectVersionsResource(schemaRegistry);

    when(headers.getHeaderString("X-Schema-Exporter-Name")).thenReturn("test-exporter");
    when(schemaRegistry.schemaVersionExists(anyString(), any(), anyBoolean())).thenReturn(true);
    when(schemaRegistry.get(anyString(), anyInt(), anyBoolean()))
        .thenReturn(new Schema("test", 1, 1, "avro", Collections.emptyList(), "{}"));

    resource.deleteSchemaVersion(asyncResponse, headers, "test-subject", "1", false);

    verify(headers).getHeaderString("X-Schema-Exporter-Name");
  }
}
