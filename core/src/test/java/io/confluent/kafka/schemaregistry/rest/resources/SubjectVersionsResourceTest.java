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

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SubjectVersionsResourceTest {

  private SchemaRegistry mockSchemaRegistry;
  private SubjectVersionsResource resource;
  private AsyncResponse mockAsyncResponse;
  private HttpHeaders mockHeaders;

  @BeforeEach
  public void setUp() {
    mockSchemaRegistry = mock(SchemaRegistry.class);
    resource = new SubjectVersionsResource(mockSchemaRegistry);
    mockAsyncResponse = mock(AsyncResponse.class);
    mockHeaders = mock(HttpHeaders.class);
  }

  @Test
  public void testDeleteSchemaVersion_followerPath_forwardsWithoutValidation() throws Exception {
    // Given: follower node
    when(mockSchemaRegistry.isLeader()).thenReturn(false);
    when(mockSchemaRegistry.tenant()).thenReturn(null);
    when(mockHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(mockSchemaRegistry.config()).thenReturn(mock(io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.class));
    when(mockSchemaRegistry.config().whitelistHeaders()).thenReturn(Collections.emptyList());

    // When: deleteSchemaVersion is called
    resource.deleteSchemaVersion(mockAsyncResponse, mockHeaders, "testSubject", "5", false);

    // Then: should forward without validation
    verify(mockSchemaRegistry).forwardDeleteSchemaVersion(anyMap(), eq("testSubject"), eq(5), eq(false));

    // Then: should NOT call validation methods
    verify(mockSchemaRegistry, never()).schemaVersionExists(anyString(), any(), anyBoolean());
    verify(mockSchemaRegistry, never()).get(anyString(), any(Integer.class), anyBoolean());
    verify(mockSchemaRegistry, never()).hasSubjects(anyString(), anyBoolean());

    // Then: should resume with version ID
    verify(mockAsyncResponse).resume(5);
  }

  @Test
  public void testDeleteSchemaVersion_followerPath_withLatest_forwardsWithoutValidation() throws Exception {
    // Given: follower node receiving "latest" version request
    when(mockSchemaRegistry.isLeader()).thenReturn(false);
    when(mockSchemaRegistry.tenant()).thenReturn(null);
    when(mockHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(mockSchemaRegistry.config()).thenReturn(mock(io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.class));
    when(mockSchemaRegistry.config().whitelistHeaders()).thenReturn(Collections.emptyList());

    // When: deleteSchemaVersion is called with "latest"
    resource.deleteSchemaVersion(mockAsyncResponse, mockHeaders, "testSubject", "latest", false);

    // Then: should forward without validation
    verify(mockSchemaRegistry).forwardDeleteSchemaVersion(anyMap(), eq("testSubject"), eq(-1), eq(false));

    // Then: should NOT call validation methods
    verify(mockSchemaRegistry, never()).schemaVersionExists(anyString(), any(), anyBoolean());
    verify(mockSchemaRegistry, never()).get(anyString(), any(Integer.class), anyBoolean());

    // Then: should resume with -1 (the internal representation of "latest")
    verify(mockAsyncResponse).resume(-1);
  }

  @Test
  public void testDeleteSchemaVersion_leaderPath_performsValidation() throws Exception {
    // Given: leader node with existing schema
    when(mockSchemaRegistry.isLeader()).thenReturn(true);
    when(mockSchemaRegistry.tenant()).thenReturn(null);
    when(mockHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(mockSchemaRegistry.config()).thenReturn(mock(io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.class));
    when(mockSchemaRegistry.config().whitelistHeaders()).thenReturn(Collections.emptyList());

    Schema mockSchema = new Schema("testSubject", 5, 100, null, "AVRO", null, null, null, "{}", null, null, null);
    when(mockSchemaRegistry.schemaVersionExists(eq("testSubject"), any(), eq(true))).thenReturn(true);
    when(mockSchemaRegistry.schemaVersionExists(eq("testSubject"), any(), eq(false))).thenReturn(true);
    when(mockSchemaRegistry.get(eq("testSubject"), eq(5), eq(true))).thenReturn(mockSchema);

    // When: deleteSchemaVersion is called on leader
    resource.deleteSchemaVersion(mockAsyncResponse, mockHeaders, "testSubject", "5", false);

    // Then: should perform validation
    verify(mockSchemaRegistry).schemaVersionExists(eq("testSubject"), any(), eq(true));
    verify(mockSchemaRegistry).get(eq("testSubject"), eq(5), eq(true));

    // Then: should call deleteSchemaVersion with the schema object
    ArgumentCaptor<Schema> schemaCaptor = ArgumentCaptor.forClass(Schema.class);
    verify(mockSchemaRegistry).deleteSchemaVersion(eq("testSubject"), schemaCaptor.capture(), eq(false));
    assertEquals(mockSchema, schemaCaptor.getValue());

    // Then: should resume with schema version
    verify(mockAsyncResponse).resume(5);
  }

  @Test
  public void testDeleteSchemaVersion_leaderPath_permanentDelete() throws Exception {
    // Given: leader node performing permanent delete
    when(mockSchemaRegistry.isLeader()).thenReturn(true);
    when(mockSchemaRegistry.tenant()).thenReturn(null);
    when(mockHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(mockSchemaRegistry.config()).thenReturn(mock(io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.class));
    when(mockSchemaRegistry.config().whitelistHeaders()).thenReturn(Collections.emptyList());

    Schema mockSchema = new Schema("testSubject", 5, 100, null, "AVRO", null, null, null, "{}", null, null, null);
    when(mockSchemaRegistry.schemaVersionExists(eq("testSubject"), any(), anyBoolean())).thenReturn(true);
    when(mockSchemaRegistry.get(eq("testSubject"), eq(5), eq(true))).thenReturn(mockSchema);

    // When: deleteSchemaVersion is called with permanentDelete = true
    resource.deleteSchemaVersion(mockAsyncResponse, mockHeaders, "testSubject", "5", true);

    // Then: should call deleteSchemaVersion with permanentDelete = true
    verify(mockSchemaRegistry).deleteSchemaVersion(eq("testSubject"), any(Schema.class), eq(true));

    // Then: should resume with schema version
    verify(mockAsyncResponse).resume(5);
  }

  @Test
  public void testDeleteSchemaVersion_followerPath_permanentDelete() throws Exception {
    // Given: follower node performing permanent delete
    when(mockSchemaRegistry.isLeader()).thenReturn(false);
    when(mockSchemaRegistry.tenant()).thenReturn(null);
    when(mockHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(mockSchemaRegistry.config()).thenReturn(mock(io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.class));
    when(mockSchemaRegistry.config().whitelistHeaders()).thenReturn(Collections.emptyList());

    // When: deleteSchemaVersion is called with permanentDelete = true on follower
    resource.deleteSchemaVersion(mockAsyncResponse, mockHeaders, "testSubject", "5", true);

    // Then: should forward without validation with permanentDelete = true
    verify(mockSchemaRegistry).forwardDeleteSchemaVersion(anyMap(), eq("testSubject"), eq(5), eq(true));

    // Then: should NOT call validation methods
    verify(mockSchemaRegistry, never()).schemaVersionExists(anyString(), any(), anyBoolean());
    verify(mockSchemaRegistry, never()).get(anyString(), any(Integer.class), anyBoolean());

    // Then: should resume with version ID
    verify(mockAsyncResponse).resume(5);
  }

  @Test
  public void testDeleteSchemaVersion_leaderPath_withLatest_resolvesToActualVersion() throws Exception {
    // Given: leader node receiving "latest" version request
    when(mockSchemaRegistry.isLeader()).thenReturn(true);
    when(mockSchemaRegistry.tenant()).thenReturn(null);
    when(mockHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(mockSchemaRegistry.config()).thenReturn(mock(io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.class));
    when(mockSchemaRegistry.config().whitelistHeaders()).thenReturn(Collections.emptyList());

    // Schema with version 42 (the actual latest version)
    Schema mockSchema = new Schema("testSubject", 42, 100, null, "AVRO", null, null, null, "{}", null, null, null);
    when(mockSchemaRegistry.schemaVersionExists(eq("testSubject"), any(), anyBoolean())).thenReturn(true);
    when(mockSchemaRegistry.get(eq("testSubject"), eq(-1), eq(true))).thenReturn(mockSchema);

    // When: deleteSchemaVersion is called with "latest"
    resource.deleteSchemaVersion(mockAsyncResponse, mockHeaders, "testSubject", "latest", false);

    // Then: should perform validation with -1 (internal representation of "latest")
    verify(mockSchemaRegistry).get(eq("testSubject"), eq(-1), eq(true));

    // Then: should call deleteSchemaVersion with the schema object
    verify(mockSchemaRegistry).deleteSchemaVersion(eq("testSubject"), any(Schema.class), eq(false));

    // Then: should resume with actual version from schema (42), not -1
    verify(mockAsyncResponse).resume(42);
  }
}
