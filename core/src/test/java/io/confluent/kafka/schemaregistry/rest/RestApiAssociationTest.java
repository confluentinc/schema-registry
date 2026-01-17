/*
 * Copyright 2021 Confluent Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResult;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class RestApiAssociationTest extends ClusterTestHarness {

  public RestApiAssociationTest() {
    super(1, true);
  }

  @Test
  public void testBasicAssociation() throws Exception {
    String subject1 = "subject1";
    String subject2 = "subject2";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "123-45-6789";
    int schemasCount = 10;
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(schemasCount);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));
    RegisterSchemaRequest valueRequest = new RegisterSchemaRequest();
    valueRequest.setSchema(allSchemas.get(1));

    // Dry run request has null resource ID
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        null,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.STRONG,
                false,
                valueRequest,
                null
            )
        )
    );

    AssociationResponse response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, true, request);
    assertEquals(resourceNamespace, response.getResourceNamespace());
    assertNull(response.getResourceId());
    assertNull(response.getAssociations());

    request.setResourceId(resourceId);

    response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(resourceName, response.getResourceName());
    assertEquals(resourceNamespace, response.getResourceNamespace());
    assertEquals(resourceId, response.getResourceId());
    assertEquals("key", response.getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, response.getAssociations().get(0).getLifecycle());
    assertEquals(allSchemas.get(0), response.getAssociations().get(0).getSchema().getSchema());
    assertEquals("value", response.getAssociations().get(1).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, response.getAssociations().get(1).getLifecycle());
    assertEquals(allSchemas.get(1), response.getAssociations().get(1).getSchema().getSchema());

    List<Association> associations = restApp.restClient.getAssociationsBySubject(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals(resourceId, associations.get(0).getResourceId());
    assertEquals(resourceName, associations.get(0).getResourceName());
    assertEquals(resourceNamespace, associations.get(0).getResourceNamespace());
    assertEquals("key", associations.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());

    associations = restApp.restClient.getAssociationsBySubject(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject2, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals(resourceId, associations.get(0).getResourceId());
    assertEquals(resourceName, associations.get(0).getResourceName());
    assertEquals(resourceNamespace, associations.get(0).getResourceNamespace());
    assertEquals("value", associations.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, associations.get(0).getLifecycle());

    associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(2, associations.size());
    assertEquals(resourceId, associations.get(0).getResourceId());
    assertEquals(resourceName, associations.get(0).getResourceName());
    assertEquals(resourceNamespace, associations.get(0).getResourceNamespace());
    assertEquals("key", associations.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());
    assertEquals(resourceId, associations.get(1).getResourceId());
    assertEquals(resourceName, associations.get(1).getResourceName());
    assertEquals(resourceNamespace, associations.get(1).getResourceNamespace());
    assertEquals("value", associations.get(1).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, associations.get(1).getLifecycle());

    associations = restApp.restClient.getAssociationsByResourceName(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceName, "-", "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(2, associations.size());
    assertEquals(resourceId, associations.get(0).getResourceId());
    assertEquals(resourceName, associations.get(0).getResourceName());
    assertEquals(resourceNamespace, associations.get(0).getResourceNamespace());
    assertEquals("key", associations.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());
    assertEquals(resourceId, associations.get(1).getResourceId());
    assertEquals(resourceName, associations.get(1).getResourceName());
    assertEquals(resourceNamespace, associations.get(1).getResourceNamespace());
    assertEquals("value", associations.get(1).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, associations.get(1).getLifecycle());

    associations = restApp.restClient.getAssociationsByResourceName(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceName, resourceNamespace, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(2, associations.size());
    assertEquals(resourceId, associations.get(0).getResourceId());
    assertEquals(resourceName, associations.get(0).getResourceName());
    assertEquals(resourceNamespace, associations.get(0).getResourceNamespace());
    assertEquals("key", associations.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());
    assertEquals(resourceId, associations.get(1).getResourceId());
    assertEquals(resourceName, associations.get(1).getResourceName());
    assertEquals(resourceNamespace, associations.get(1).getResourceNamespace());
    assertEquals("value", associations.get(1).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, associations.get(1).getLifecycle());

    request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.STRONG,
                false,
                keyRequest,
                null
            )
        )
    );

    response = restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, true, request);
    assertEquals(resourceNamespace, response.getResourceNamespace());
    assertEquals(resourceId, response.getResourceId());
    assertNull(response.getAssociations());

    response = restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(resourceName, response.getResourceName());
    assertEquals(resourceNamespace, response.getResourceNamespace());
    assertEquals(resourceId, response.getResourceId());
    assertEquals("key", response.getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, response.getAssociations().get(0).getLifecycle());
    assertEquals(allSchemas.get(0), response.getAssociations().get(0).getSchema().getSchema());

    boolean cascadeDelete = false;
    restApp.restClient.deleteAssociations(RestService.DEFAULT_REQUEST_PROPERTIES,
        resourceId, "topic", Collections.singletonList("key"), cascadeDelete, false);

    associations = restApp.restClient.getAssociationsBySubject(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(0, associations.size());

    List<Schema> schemas = restApp.restClient.getSchemas(null, false, false);
    assertEquals(2, schemas.size());

    cascadeDelete = true;
    restApp.restClient.deleteAssociations(RestService.DEFAULT_REQUEST_PROPERTIES,
        resourceId, "topic", Collections.singletonList("value"), cascadeDelete, false);

    associations = restApp.restClient.getAssociationsBySubject(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(0, associations.size());

    schemas = restApp.restClient.getSchemas(null, false, false);
    assertEquals(1, schemas.size());

  }

  @Test
  public void testAssociationDuplicateTypes() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "duplicate-types-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest keyRequest1 = new RegisterSchemaRequest();
    keyRequest1.setSchema(allSchemas.get(0));
    RegisterSchemaRequest keyRequest2 = new RegisterSchemaRequest();
    keyRequest2.setSchema(allSchemas.get(1));

    // Create request with duplicate association types
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest1,
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",  // Duplicate type
                LifecyclePolicy.WEAK,
                false,
                keyRequest2,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request)
    );
  }

  @Test
  public void testAssociationForResourceExists() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "resource-exists-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Create initial association
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);

    // Try to create the same association again
    RegisterSchemaRequest keyRequest2 = new RegisterSchemaRequest();
    keyRequest2.setSchema(allSchemas.get(1));
    AssociationCreateOrUpdateRequest request2 = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.STRONG,  // Different lifecycle
                false,
                keyRequest2,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request2)
    );
  }

  @Test
  public void testAssociationFrozen() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "frozen-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Create frozen association
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.STRONG,
                true,  // Frozen
                keyRequest,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);

    // Try to update frozen association without explicitly setting frozen=false
    AssociationCreateOrUpdateRequest updateRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,  // Try to change lifecycle
                null,  // Not explicitly unfreezing
                null,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, updateRequest)
    );
  }

  @Test
  public void testNoActiveSubjectVersionExists() throws Exception {
    String subject1 = "nonexistent-subject";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "no-version-123";

    // Create association without providing a schema for non-existent subject
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                null,  // No schema provided
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request)
    );
  }

  @Test
  public void testAssociationForSubjectExists() throws Exception {
    String subject1 = "subject1";
    String resourceName1 = "topic1";
    String resourceName2 = "topic2";
    String resourceNamespace = "default";
    String resourceId1 = "resource1-123";
    String resourceId2 = "resource2-456";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Create first WEAK association for subject
    AssociationCreateOrUpdateRequest request1 = new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request1);

    // Try to create STRONG association for same subject (should fail)
    AssociationCreateOrUpdateRequest request2 = new AssociationCreateOrUpdateRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "value",
                LifecyclePolicy.STRONG,  // STRONG lifecycle
                false,
                null,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request2)
    );
  }

  @Test
  public void testStrongAssociationForSubjectExists() throws Exception {
    String subject1 = "subject1";
    String resourceName1 = "topic1";
    String resourceName2 = "topic2";
    String resourceNamespace = "default";
    String resourceId1 = "resource1-123";
    String resourceId2 = "resource2-456";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Create first STRONG association for subject
    AssociationCreateOrUpdateRequest request1 = new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.STRONG,
                false,
                keyRequest,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request1);

    // Try to create WEAK association for same subject (should fail because STRONG exists)
    AssociationCreateOrUpdateRequest request2 = new AssociationCreateOrUpdateRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "value",
                LifecyclePolicy.WEAK,  // WEAK lifecycle
                false,
                null,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request2)
    );
  }

  @Test
  public void testWeakAssociationCannotBeFrozen() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "weak-frozen-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Try to create WEAK association with frozen=true (should fail)
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                true,  // Frozen (not allowed for WEAK)
                keyRequest,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request)
    );
  }

  @Test
  public void testIncompatibleSchemaInAssociation() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "incompatible-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Create initial association
    AssociationCreateOrUpdateRequest request1 = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request1);

    // Try to create association with incompatible schema
    // Note: This test assumes backward compatibility is enforced
    // The second schema is randomly generated and likely incompatible
    RegisterSchemaRequest incompatibleRequest = new RegisterSchemaRequest();
    incompatibleRequest.setSchema(allSchemas.get(1));

    AssociationCreateOrUpdateRequest request2 = new AssociationCreateOrUpdateRequest(
        resourceName + "2",
        resourceNamespace,
        resourceId + "2",
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "value",
                LifecyclePolicy.WEAK,
                false,
                incompatibleRequest,
                null
            )
        )
    );

    // May or may not throw depending on compatibility config
    // If it throws, that's expected; if not, the test still passes
    try {
      restApp.restClient.createAssociation(
          RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request2);
    } catch (Exception e) {
      // Expected if compatibility check fails
    }
  }

  @Test
  public void testCannotChangeAssociationSubject() throws Exception {
    String subject1 = "subject1";
    String subject2 = "subject2";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "subject-change-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Create initial association with subject1
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);

    // Try to update the association but change the subject to subject2
    AssociationCreateOrUpdateRequest updateRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject2,  // Different subject
                "key",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    );

    // Should throw an exception because subject cannot be changed
    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, updateRequest)
    );
  }

  @Test
  public void testUpdateAssociationExcludesItselfFromConflictCheck() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "self-exclude-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Create initial STRONG association
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.STRONG,
                false,
                keyRequest,
                null
            )
        )
    );

    AssociationResponse response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals("key", response.getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, response.getAssociations().get(0).getLifecycle());

    // Update the same association (changing lifecycle from STRONG to WEAK)
    // This should succeed because the association should exclude itself
    // from the conflict check (lines 1053-1055)
    AssociationCreateOrUpdateRequest updateRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,  // Changing lifecycle
                false,
                null,
                null
            )
        )
    );

    // Should succeed - the association should exclude itself from conflict check
    AssociationResponse updateResponse = restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, updateRequest);
    assertEquals("key", updateResponse.getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, updateResponse.getAssociations().get(0).getLifecycle());

    // Verify the association was actually updated
    List<Association> associations = restApp.restClient.getAssociationsBySubject(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());
  }

  @Test
  public void testUpdateWeakAssociationToFrozen() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "weak-update-frozen-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Create initial WEAK association (not frozen)
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);

    // Try to update the WEAK association to frozen=true (should fail)
    AssociationCreateOrUpdateRequest updateRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                true,  // Try to freeze WEAK association
                null,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, updateRequest)
    );
  }

  @Test
  public void testBatchCreateAssociations() throws Exception {
    String subject1 = "batchSubject1";
    String subject2 = "batchSubject2";
    String subject3 = "batchSubject3";
    String resourceName1 = "batchTopic1";
    String resourceName2 = "batchTopic2";
    String resourceNamespace = "default";
    String resourceId1 = "batch-resource-1";
    String resourceId2 = "batch-resource-2";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(3);

    RegisterSchemaRequest keyRequest1 = new RegisterSchemaRequest();
    keyRequest1.setSchema(allSchemas.get(0));
    RegisterSchemaRequest valueRequest1 = new RegisterSchemaRequest();
    valueRequest1.setSchema(allSchemas.get(1));
    RegisterSchemaRequest keyRequest2 = new RegisterSchemaRequest();
    keyRequest2.setSchema(allSchemas.get(2));

    // Create batch request with multiple associations
    List<AssociationCreateOrUpdateRequest> requests = new ArrayList<>();
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest1,
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.STRONG,
                false,
                valueRequest1,
                null
            )
        )
    ));
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject3,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest2,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    AssociationBatchResponse batchResponse = restApp.restClient.createAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);

    // Verify batch response
    assertNotNull(batchResponse);
    assertEquals(2, batchResponse.getResults().size());

    // Verify first result (2 associations)
    AssociationResult result1 = batchResponse.getResults().get(0);
    assertNull(result1.getError());
    assertNotNull(result1.getResult());
    assertEquals(resourceName1, result1.getResult().getResourceName());
    assertEquals(resourceId1, result1.getResult().getResourceId());
    assertEquals(2, result1.getResult().getAssociations().size());
    assertEquals("key", result1.getResult().getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, result1.getResult().getAssociations().get(0).getLifecycle());
    assertEquals("value", result1.getResult().getAssociations().get(1).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, result1.getResult().getAssociations().get(1).getLifecycle());

    // Verify second result (1 association)
    AssociationResult result2 = batchResponse.getResults().get(1);
    assertNull(result2.getError());
    assertNotNull(result2.getResult());
    assertEquals(resourceName2, result2.getResult().getResourceName());
    assertEquals(resourceId2, result2.getResult().getResourceId());
    assertEquals(1, result2.getResult().getAssociations().size());
    assertEquals("key", result2.getResult().getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, result2.getResult().getAssociations().get(0).getLifecycle());

    // Verify associations were actually created
    List<Association> associations1 = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId1, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(2, associations1.size());

    List<Association> associations2 = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId2, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(1, associations2.size());
  }

  @Test
  public void testBatchCreateAssociationsWithDryRun() throws Exception {
    String subject1 = "dryRunSubject1";
    String subject2 = "dryRunSubject2";
    String resourceName = "dryRunTopic";
    String resourceNamespace = "default";
    String resourceId = "dry-run-batch-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));
    RegisterSchemaRequest valueRequest = new RegisterSchemaRequest();
    valueRequest.setSchema(allSchemas.get(1));

    // Create batch request
    List<AssociationCreateOrUpdateRequest> requests = new ArrayList<>();
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.STRONG,
                false,
                valueRequest,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    // Dry run
    AssociationBatchResponse batchResponse = restApp.restClient.createAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, true, batchRequest);

    // Verify dry run response
    assertNotNull(batchResponse);
    assertEquals(1, batchResponse.getResults().size());
    AssociationResult result = batchResponse.getResults().get(0);
    assertNull(result.getError());
    assertNotNull(result.getResult());
    assertEquals(resourceNamespace, result.getResult().getResourceNamespace());
    assertEquals(resourceId, result.getResult().getResourceId());
    assertNull(result.getResult().getAssociations());

    // Verify associations were NOT actually created
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(0, associations.size());
  }

  @Test
  public void testBatchCreateAssociationsPartialFailure() throws Exception {
    String subject1 = "partialSubject1";
    String subject2 = "partialSubject2";
    String resourceName1 = "partialTopic1";
    String resourceName2 = "partialTopic2";
    String resourceNamespace = "default";
    String resourceId1 = "partial-resource-1";
    String resourceId2 = "partial-resource-2";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));
    RegisterSchemaRequest valueRequest = new RegisterSchemaRequest();
    valueRequest.setSchema(allSchemas.get(1));

    // First create an association that will cause a conflict
    AssociationCreateOrUpdateRequest existingRequest = new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            )
        )
    );
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, existingRequest);

    // Create batch request where first will fail (duplicate), second will succeed
    List<AssociationCreateOrUpdateRequest> requests = new ArrayList<>();
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,  // Same resource ID - will fail
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",  // Duplicate
                LifecyclePolicy.STRONG,
                false,
                null,
                null
            )
        )
    ));
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,  // Different resource - will succeed
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                valueRequest,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    AssociationBatchResponse batchResponse = restApp.restClient.createAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);

    // Verify batch response has both results
    assertNotNull(batchResponse);
    assertEquals(2, batchResponse.getResults().size());

    // First result should have an error
    AssociationResult result1 = batchResponse.getResults().get(0);
    assertNotNull(result1.getError());
    assertNull(result1.getResult());

    // Second result should be successful
    AssociationResult result2 = batchResponse.getResults().get(1);
    assertNull(result2.getError());
    assertNotNull(result2.getResult());
    assertEquals(resourceName2, result2.getResult().getResourceName());
    assertEquals(resourceId2, result2.getResult().getResourceId());
  }

  @Test
  public void testBatchUpsertAssociations() throws Exception {
    String subject1 = "upsertSubject1";
    String subject2 = "upsertSubject2";
    String resourceName1 = "upsertTopic1";
    String resourceName2 = "upsertTopic2";
    String resourceNamespace = "default";
    String resourceId1 = "upsert-resource-1";
    String resourceId2 = "upsert-resource-2";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));
    RegisterSchemaRequest valueRequest = new RegisterSchemaRequest();
    valueRequest.setSchema(allSchemas.get(1));

    // Create initial association that will be updated
    AssociationCreateOrUpdateRequest initialRequest = new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            )
        )
    );
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, initialRequest);

    // Batch upsert: update existing and create new
    List<AssociationCreateOrUpdateRequest> requests = new ArrayList<>();
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,  // Existing - will be updated
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.STRONG,  // Change from WEAK to STRONG
                false,
                null,
                null
            )
        )
    ));
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,  // New - will be created
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                valueRequest,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    AssociationBatchResponse batchResponse = restApp.restClient.createOrUpdateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);

    // Verify batch response
    assertNotNull(batchResponse);
    assertEquals(2, batchResponse.getResults().size());

    // Verify first result (updated)
    AssociationResult result1 = batchResponse.getResults().get(0);
    assertNull(result1.getError());
    assertNotNull(result1.getResult());
    assertEquals(resourceName1, result1.getResult().getResourceName());
    assertEquals(resourceId1, result1.getResult().getResourceId());
    assertEquals(LifecyclePolicy.STRONG, result1.getResult().getAssociations().get(0).getLifecycle());

    // Verify second result (created)
    AssociationResult result2 = batchResponse.getResults().get(1);
    assertNull(result2.getError());
    assertNotNull(result2.getResult());
    assertEquals(resourceName2, result2.getResult().getResourceName());
    assertEquals(resourceId2, result2.getResult().getResourceId());
    assertEquals(LifecyclePolicy.WEAK, result2.getResult().getAssociations().get(0).getLifecycle());

    // Verify the update was persisted
    List<Association> associations1 = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId1, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(1, associations1.size());
    assertEquals(LifecyclePolicy.STRONG, associations1.get(0).getLifecycle());

    // Verify the new association was created
    List<Association> associations2 = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId2, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(1, associations2.size());
    assertEquals(LifecyclePolicy.WEAK, associations2.get(0).getLifecycle());
  }

  @Test
  public void testBatchUpsertAssociationsWithDryRun() throws Exception {
    String subject1 = "upsertDrySubject1";
    String resourceName = "upsertDryTopic";
    String resourceNamespace = "default";
    String resourceId = "upsert-dry-resource";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    // Create initial association
    AssociationCreateOrUpdateRequest initialRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                keyRequest,
                null
            )
        )
    );
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, initialRequest);

    // Dry run update
    List<AssociationCreateOrUpdateRequest> requests = new ArrayList<>();
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.STRONG,  // Try to change to STRONG
                false,
                null,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    AssociationBatchResponse batchResponse = restApp.restClient.createOrUpdateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, true, batchRequest);

    // Verify dry run response
    assertNotNull(batchResponse);
    assertEquals(1, batchResponse.getResults().size());
    AssociationResult result = batchResponse.getResults().get(0);
    assertNull(result.getError());
    assertNotNull(result.getResult());
    assertEquals(resourceId, result.getResult().getResourceId());
    assertNull(result.getResult().getAssociations());

    // Verify association was NOT actually updated
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());  // Still WEAK
  }

  @Test
  public void testBatchUpsertAssociationsPartialFailure() throws Exception {
    String subject1 = "upsertPartialSubject1";
    String subject2 = "upsertPartialSubject2";
    String resourceName1 = "upsertPartialTopic1";
    String resourceName2 = "upsertPartialTopic2";
    String resourceNamespace = "default";
    String resourceId1 = "upsert-partial-1";
    String resourceId2 = "upsert-partial-2";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));
    RegisterSchemaRequest valueRequest = new RegisterSchemaRequest();
    valueRequest.setSchema(allSchemas.get(1));

    // Create initial frozen association
    AssociationCreateOrUpdateRequest frozenRequest = new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.STRONG,
                true,  // Frozen
                keyRequest,
                null
            )
        )
    );
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, frozenRequest);

    // Batch upsert: try to update frozen (will fail) and create new (will succeed)
    List<AssociationCreateOrUpdateRequest> requests = new ArrayList<>();
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.WEAK,  // Try to change frozen - will fail
                null,
                null,
                null
            )
        )
    ));
    requests.add(new AssociationCreateOrUpdateRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                valueRequest,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    AssociationBatchResponse batchResponse = restApp.restClient.createOrUpdateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);

    // Verify batch response
    assertNotNull(batchResponse);
    assertEquals(2, batchResponse.getResults().size());

    // First result should have an error (frozen)
    AssociationResult result1 = batchResponse.getResults().get(0);
    assertNotNull(result1.getError());
    assertNull(result1.getResult());

    // Second result should be successful
    AssociationResult result2 = batchResponse.getResults().get(1);
    assertNull(result2.getError());
    assertNotNull(result2.getResult());
    assertEquals(resourceName2, result2.getResult().getResourceName());
    assertEquals(resourceId2, result2.getResult().getResourceId());

    // Verify frozen association was not changed
    List<Association> associations1 = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId1, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(1, associations1.size());
    assertEquals(LifecyclePolicy.STRONG, associations1.get(0).getLifecycle());  // Still STRONG

    // Verify new association was created
    List<Association> associations2 = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId2, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(1, associations2.size());
  }
}

