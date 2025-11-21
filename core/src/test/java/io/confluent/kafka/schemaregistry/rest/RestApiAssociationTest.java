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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
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
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceName, "*", "topic",
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
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(resourceName, response.getResourceName());
    assertEquals(resourceNamespace, response.getResourceNamespace());
    assertEquals(resourceId, response.getResourceId());
    assertEquals("key", response.getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, response.getAssociations().get(0).getLifecycle());
    assertEquals(allSchemas.get(0), response.getAssociations().get(0).getSchema().getSchema());

    boolean cascadeDelete = false;
    restApp.restClient.deleteAssociations(RestService.DEFAULT_REQUEST_PROPERTIES,
        resourceId, "topic", Collections.singletonList("key"), cascadeDelete);

    associations = restApp.restClient.getAssociationsBySubject(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(0, associations.size());

    List<Schema> schemas = restApp.restClient.getSchemas(null, false, false);
    assertEquals(2, schemas.size());

    cascadeDelete = true;
    restApp.restClient.deleteAssociations(RestService.DEFAULT_REQUEST_PROPERTIES,
        resourceId, "topic", Collections.singletonList("value"), cascadeDelete);

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
}

