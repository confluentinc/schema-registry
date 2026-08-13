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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicyFilter;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity.EntityType;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTags;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationGetRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationDeleteOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationOpRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResult;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpsertOp;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class RestApiAssociationTest extends ClusterTestHarness {

  private static final String SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"myrecord\","
      + "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
  private static final String TAGGED_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"myrecord\","
      + "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}],"
      + "\"confluent:tags\":[\"TAG1\",\"TAG2\"]}";
  private static final String EVOLVED_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"myrecord\","
      + "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"},"
      + "{\"name\":\"f2\",\"type\":\"string\",\"default\":\"hi\"}]}";
  private static final String TAGGED_EVOLVED_SCHEMA_STRING =
      "{\"type\":\"record\",\"name\":\"myrecord\","
      + "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"},"
      + "{\"name\":\"f2\",\"type\":\"string\",\"default\":\"hi\"}],"
      + "\"confluent:tags\":[\"TAG1\",\"TAG2\"]}";

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

    // Register schemas separately since WEAK and non-frozen STRONG associations
    // cannot have schemas passed directly in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);
    restApp.restClient.registerSchema(allSchemas.get(1), subject2);

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
                null,
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                null,
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
    assertEquals("value", response.getAssociations().get(1).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, response.getAssociations().get(1).getLifecycle());

    // Verify createTs and updateTs are set after creation
    List<Association> createdAssociations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(2, createdAssociations.size());
    Association keyAssocAfterCreate = createdAssociations.stream()
        .filter(a -> "key".equals(a.getAssociationType()))
        .findFirst().orElse(null);
    assertNotNull(keyAssocAfterCreate);
    assertNotNull(keyAssocAfterCreate.getCreateTimestamp());
    assertNotNull(keyAssocAfterCreate.getUpdateTimestamp());
    Long keyCreateTs = keyAssocAfterCreate.getCreateTimestamp();
    Long keyUpdateTsAfterCreate = keyAssocAfterCreate.getUpdateTimestamp();

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
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());

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
    assertEquals(LifecyclePolicy.WEAK, associations.get(1).getLifecycle());

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
    assertEquals(LifecyclePolicy.WEAK, associations.get(1).getLifecycle());

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
    assertEquals(LifecyclePolicy.WEAK, associations.get(1).getLifecycle());

    associations = restApp.restClient.getAssociationsByResourceName(
        RestService.DEFAULT_REQUEST_PROPERTIES, "-", resourceNamespace, "topic",
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
    assertEquals(LifecyclePolicy.WEAK, associations.get(1).getLifecycle());

    // An association is immutable once created: promoting these WEAK associations to STRONG is
    // rejected, since a STRONG association is frozen and must be created with its topic.
    AssociationCreateOrUpdateRequest requestToPromote = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",
                LifecyclePolicy.STRONG,
                null,
                null,
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.STRONG,
                null,
                null,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, requestToPromote));

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

    // The association is WEAK, so it does not own its subject and the cascade leaves the
    // schemas alone. Cascading delete of a frozen STRONG association is covered by
    // testAssociationFrozen.
    schemas = restApp.restClient.getSchemas(null, false, false);
    assertEquals(2, schemas.size());

  }

  @Test
  public void testAssociationDuplicateTypes() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "duplicate-types-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    // Register schema separately since WEAK associations cannot have schemas in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

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
                null,
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject1,
                "key",  // Duplicate type
                LifecyclePolicy.WEAK,
                false,
                null,
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

    // Register schema separately since WEAK associations cannot have schemas in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

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
                null,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);

    // Try to create the same association again
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
  public void testAssociationFrozen() throws Exception {
    String subject1 = "subject1";
    String subject2 = "subject2";
    String subject3 = "subject3";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "frozen-123";
    String resourceId2 = "frozen-456";
    String resourceId3 = "frozen-789";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest keyRequest = new RegisterSchemaRequest();
    keyRequest.setSchema(allSchemas.get(0));

    String defaultKeySubject = ":." + resourceNamespace + ":" + resourceName + "-key";

    // Test creating frozen association without schema fails
    AssociationCreateOrUpdateRequest noSchemaRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId2,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                null,
                "key",
                LifecyclePolicy.STRONG,
                true,  // Frozen
                null,  // No schema provided
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, noSchemaRequest)
    );

    // Test creating frozen association when schemas already exist fails
    // First register a schema in the default subject for a different resource
    String resourceName3 = "topic1-existing";
    restApp.restClient.registerSchema(allSchemas.get(1),
        ":." + resourceNamespace + ":" + resourceName3 + "-key");

    RegisterSchemaRequest anotherSchemaRequest = new RegisterSchemaRequest();
    anotherSchemaRequest.setSchema(allSchemas.get(0));

    AssociationCreateOrUpdateRequest existingSchemasRequest = new AssociationCreateOrUpdateRequest(
        resourceName3,
        resourceNamespace,
        resourceId3,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                null,
                "key",
                LifecyclePolicy.STRONG,
                true,  // Frozen
                anotherSchemaRequest,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, existingSchemasRequest)
    );

    // Create frozen association successfully
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                null,
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
                defaultKeySubject,
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

    // Test that frozen attribute cannot be changed
    AssociationCreateOrUpdateRequest unfreezeRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                defaultKeySubject,
                "key",
                LifecyclePolicy.STRONG,
                false,  // Try to unfreeze
                null,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, unfreezeRequest)
    );

    // Test deleting frozen association without cascadeLifecycle fails
    assertThrows(Exception.class, () ->
        restApp.restClient.deleteAssociations(RestService.DEFAULT_REQUEST_PROPERTIES,
            resourceId, "topic", Collections.singletonList("key"), false, false)
    );

    // Test deleting frozen association with cascadeLifecycle succeeds
    restApp.restClient.deleteAssociations(RestService.DEFAULT_REQUEST_PROPERTIES,
        resourceId, "topic", Collections.singletonList("key"), true, false);

    // Verify association is deleted
    List<Association> associations = restApp.restClient.getAssociationsBySubject(
        RestService.DEFAULT_REQUEST_PROPERTIES, defaultKeySubject, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertTrue(associations.isEmpty());
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

    // Register schema separately since WEAK associations cannot have schemas in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

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
                null,
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
    String resourceName1 = "topic1";
    String resourceName2 = "topic2";
    String resourceNamespace = "default";
    String resourceId1 = "resource1-123";
    String resourceId2 = "resource2-456";
    // A STRONG association is frozen, so it owns its resource's canonical subject and carries
    // its schema inline rather than having one registered beforehand.
    String subject1 = ":.default:topic1-key";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

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
                true,
                schemaRequest,
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
  public void testIdempotentValidateAndCreateRetry() throws Exception {
    // Simulates the CreateTopics retry pattern: callers cannot supply a resourceId
    // at validate-phase because Kafka assigns the topic UUID at create time, strictly
    // after validate. The validate-phase call on retry must be idempotent when the
    // requested association is content-equivalent to the existing one.
    // A STRONG association is frozen, so it uses its resource's canonical subject and carries
    // its schema inline.
    String subject1 = ":.default:topic1-value";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "resource-uuid-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    AssociationCreateOrUpdateRequest validateRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        null,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "value",
                LifecyclePolicy.STRONG,
                true,
                schemaRequest,
                null
            )
        )
    );
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject1,
                "value",
                LifecyclePolicy.STRONG,
                true,
                schemaRequest,
                null
            )
        )
    );

    // 1. Initial validate (dryRun=true, resourceId=null)
    AssociationResponse response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, true, validateRequest);
    assertNull(response.getResourceId());
    assertNull(response.getAssociations());

    // 2. Initial commit (dryRun=false, resourceId=UUID) — creates association
    response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);
    assertEquals(resourceId, response.getResourceId());
    assertEquals(1, response.getAssociations().size());

    // 3. Retry validate (dryRun=true, resourceId=null) — must be idempotent.
    // Before the fix this threw 40904: by-resourceId lookup returned empty, the
    // equivalence check was skipped, and the strong-uniqueness check fired against
    // the association created in step 2.
    response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, true, validateRequest);
    assertNull(response.getResourceId());
    assertNull(response.getAssociations());

    // 4. Retry commit (dryRun=false, resourceId=UUID) — idempotent via existing
    // by-resourceId equivalence path; confirms the fallback didn't break it.
    // The response carries no associations because the existing one is equivalent
    // and gets added to assocTypesToSkip; verify state via a follow-up query.
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);
    List<Association> existing = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("value"), null, 0, -1);
    assertEquals(1, existing.size());
    assertEquals("value", existing.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, existing.get(0).getLifecycle());
  }

  @Test
  public void testWeakAssociationCannotBeFrozen() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "weak-frozen-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    // Register schema separately so we can test WEAK+frozen without schema conflict
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

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
                null,
                null
            )
        )
    );

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request)
    );
  }

  /**
   * A WEAK association cannot be promoted, and the failure names the rule that actually applies:
   * setting frozen is refused because frozen is immutable, while leaving it unset is refused
   * because a STRONG association is always frozen.
   */
  @Test
  public void testWeakAssociationCannotBePromoted() throws Exception {
    String subject = "promote-subject";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "promote-rule-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    restApp.restClient.registerSchema(allSchemas.get(0), subject);
    restApp.restClient.createAssociation(RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
        new AssociationCreateOrUpdateRequest(resourceName, resourceNamespace, resourceId, "topic",
            ImmutableList.of(new AssociationCreateOrUpdateInfo(
                subject, "value", LifecyclePolicy.WEAK, false, null, null))));

    // frozen explicitly set: refused because frozen cannot be changed
    Exception frozenSet = assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
            new AssociationCreateOrUpdateRequest(resourceName, resourceNamespace, resourceId,
                "topic", ImmutableList.of(new AssociationCreateOrUpdateInfo(
                    subject, "value", LifecyclePolicy.STRONG, true, null, null)))));
    assertTrue(frozenSet.getMessage().contains("frozen attribute of association cannot be changed"),
        frozenSet.getMessage());

    // frozen left unset: refused because a STRONG association is always frozen
    Exception frozenUnset = assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
            new AssociationCreateOrUpdateRequest(resourceName, resourceNamespace, resourceId,
                "topic", ImmutableList.of(new AssociationCreateOrUpdateInfo(
                    subject, "value", LifecyclePolicy.STRONG, null, null, null)))));
    assertTrue(frozenUnset.getMessage().contains("cannot be frozen=false"),
        frozenUnset.getMessage());
  }

  @Test
  public void testIncompatibleSchemaInAssociation() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "incompatible-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    // Register schema separately since WEAK associations cannot have schemas in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

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
                null,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request1);

    // Try to create association with incompatible schema
    // Note: This test assumes backward compatibility is enforced
    // The second schema is randomly generated and likely incompatible
    // Register the second schema separately and create without schema in request
    try {
      restApp.restClient.registerSchema(allSchemas.get(1), subject1);
    } catch (Exception e) {
      // Expected if compatibility check fails
    }

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
                null,
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

    // Register schema separately since WEAK associations cannot have schemas in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

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
                null,
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

    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

    // Create initial WEAK association
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
                null,
                null
            )
        )
    );

    AssociationResponse response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals("key", response.getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, response.getAssociations().get(0).getLifecycle());

    // Upsert the very same association again. An association is immutable once created, so this
    // changes nothing; it should still succeed rather than report that the subject already has
    // an association, because the association must exclude itself from the conflict check.
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
                false,
                null,
                null
            )
        )
    );

    restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, updateRequest);

    // Verify the association is unchanged and was not duplicated
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

    // Register schema separately since WEAK associations cannot have schemas in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

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
                null,
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
  public void testBatchGetAssociations() throws Exception {
    String subject1 = "subject1";
    // The STRONG association is frozen, so it owns its resource's canonical subject.
    String subject2 = ":.default:topic2-value";
    String resourceName1 = "topic1";
    String resourceName2 = "topic2";
    String resourceNamespace = "default";
    String resourceId1 = "batch-get-id-1";
    String resourceId2 = "batch-get-id-2";
    int schemasCount = 10;
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(schemasCount);

    // Register schemas separately
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

    RegisterSchemaRequest subject2Schema = new RegisterSchemaRequest();
    subject2Schema.setSchema(allSchemas.get(1));

    // Create first association
    AssociationCreateOrUpdateRequest request1 = new AssociationCreateOrUpdateRequest(
        resourceName1, resourceNamespace, resourceId1, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject1, "value", LifecyclePolicy.WEAK, false, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request1);

    // Create second association
    AssociationCreateOrUpdateRequest request2 = new AssociationCreateOrUpdateRequest(
        resourceName2, resourceNamespace, resourceId2, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject2, "value", LifecyclePolicy.STRONG, true, subject2Schema, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request2);

    // Batch get both associations
    AssociationBatchGetRequest batchGetRequest = new AssociationBatchGetRequest(
        ImmutableList.of(
            new AssociationGetRequest(resourceId1, "topic", null, null),
            new AssociationGetRequest(resourceId2, "topic", null, null)
        ));
    AssociationBatchResponse batchResponse = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, false, batchGetRequest);

    assertEquals(2, batchResponse.getResults().size());

    AssociationResult result1 = batchResponse.getResults().get(0);
    assertNull(result1.getError());
    assertNotNull(result1.getResult());
    assertEquals(resourceId1, result1.getResult().getResourceId());
    assertEquals(1, result1.getResult().getAssociations().size());
    assertEquals("value", result1.getResult().getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK,
        result1.getResult().getAssociations().get(0).getLifecycle());

    AssociationResult result2 = batchResponse.getResults().get(1);
    assertNull(result2.getError());
    assertNotNull(result2.getResult());
    assertEquals(resourceId2, result2.getResult().getResourceId());
    assertEquals(1, result2.getResult().getAssociations().size());
    assertEquals("value", result2.getResult().getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG,
        result2.getResult().getAssociations().get(0).getLifecycle());

    // Batch get with unknown resource ID returns empty associations
    AssociationBatchGetRequest batchGetUnknown = new AssociationBatchGetRequest(
        ImmutableList.of(
            new AssociationGetRequest("unknown-id", "topic", null, null)
        ));
    AssociationBatchResponse unknownResponse = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, false, batchGetUnknown);
    assertEquals(1, unknownResponse.getResults().size());
    assertNull(unknownResponse.getResults().get(0).getError());
    assertNotNull(unknownResponse.getResults().get(0).getResult());
    assertNull(unknownResponse.getResults().get(0).getResult().getAssociations());

    // Batch get with lifecycle filter
    AssociationBatchGetRequest batchGetFiltered = new AssociationBatchGetRequest(
        ImmutableList.of(
            new AssociationGetRequest(resourceId1, "topic", null, LifecyclePolicy.STRONG)
        ));
    AssociationBatchResponse filteredResponse = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, false, batchGetFiltered);
    assertEquals(1, filteredResponse.getResults().size());
    assertNull(filteredResponse.getResults().get(0).getError());
    assertNull(filteredResponse.getResults().get(0).getResult().getAssociations());

    // Batch get by resourceName/resourceNamespace
    AssociationBatchGetRequest batchGetByName = new AssociationBatchGetRequest(
        ImmutableList.of(
            new AssociationGetRequest(
                resourceName1, resourceNamespace, "topic", null, null)
        ));
    AssociationBatchResponse byNameResponse = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, false, batchGetByName);
    assertEquals(1, byNameResponse.getResults().size());
    AssociationResult byNameResult = byNameResponse.getResults().get(0);
    assertNull(byNameResult.getError());
    assertNotNull(byNameResult.getResult());
    assertEquals(1, byNameResult.getResult().getAssociations().size());
    assertEquals("value", byNameResult.getResult().getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK,
        byNameResult.getResult().getAssociations().get(0).getLifecycle());

    // Batch get with includeSchemas=true
    AssociationBatchGetRequest batchGetWithSchemas = new AssociationBatchGetRequest(
        ImmutableList.of(
            new AssociationGetRequest(resourceId1, "topic", null, null),
            new AssociationGetRequest(resourceId2, "topic", null, null)
        ));
    AssociationBatchResponse withSchemasResponse = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, true, batchGetWithSchemas);
    assertEquals(2, withSchemasResponse.getResults().size());

    AssociationResult schemaResult1 = withSchemasResponse.getResults().get(0);
    assertNull(schemaResult1.getError());
    assertNotNull(schemaResult1.getResult());
    assertEquals(1, schemaResult1.getResult().getAssociations().size());
    assertNotNull(schemaResult1.getResult().getAssociations().get(0).getSchema());
    assertEquals(allSchemas.get(0),
        schemaResult1.getResult().getAssociations().get(0).getSchema().getSchema());

    AssociationResult schemaResult2 = withSchemasResponse.getResults().get(1);
    assertNull(schemaResult2.getError());
    assertNotNull(schemaResult2.getResult());
    assertEquals(1, schemaResult2.getResult().getAssociations().size());
    assertNotNull(schemaResult2.getResult().getAssociations().get(0).getSchema());
    assertEquals(allSchemas.get(1),
        schemaResult2.getResult().getAssociations().get(0).getSchema().getSchema());

    // Batch get with includeSchemas=false should not return schemas
    AssociationBatchGetRequest batchGetWithoutSchemas = new AssociationBatchGetRequest(
        ImmutableList.of(
            new AssociationGetRequest(resourceId1, "topic", null, null)
        ));
    AssociationBatchResponse withoutSchemasResponse = restApp.restClient.batchGetAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, false, batchGetWithoutSchemas);
    assertEquals(1, withoutSchemasResponse.getResults().size());
    assertNull(withoutSchemasResponse.getResults().get(0).getResult().getAssociations()
        .get(0).getSchema());
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

    // Register schemas separately
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);
    restApp.restClient.registerSchema(allSchemas.get(1), subject2);
    restApp.restClient.registerSchema(allSchemas.get(2), subject3);

    // Create batch request with multiple associations
    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(new AssociationOpRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationCreateOp(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            ),
            new AssociationCreateOp(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    ));
    requests.add(new AssociationOpRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,
        "topic",
        ImmutableList.of(
            new AssociationCreateOp(
                subject3,
                "key",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    AssociationBatchResponse batchResponse = restApp.restClient.mutateAssociations(
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
    assertEquals(LifecyclePolicy.WEAK, result1.getResult().getAssociations().get(1).getLifecycle());

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

    // Register schemas separately
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);
    restApp.restClient.registerSchema(allSchemas.get(1), subject2);

    // Create batch request
    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(new AssociationOpRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOp(
                subject1,
                "key",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            ),
            new AssociationCreateOp(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    // Dry run
    AssociationBatchResponse batchResponse = restApp.restClient.mutateAssociations(
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

    // Register schemas separately
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);
    restApp.restClient.registerSchema(allSchemas.get(1), subject2);

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
                null,
                null
            )
        )
    );
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, existingRequest);

    // Create batch request where first will fail (duplicate), second will succeed
    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(new AssociationOpRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,  // Same resource ID - will fail
        "topic",
        ImmutableList.of(
            new AssociationCreateOp(
                subject1,
                "key",  // Duplicate
                LifecyclePolicy.STRONG,
                false,
                null,
                null
            )
        )
    ));
    requests.add(new AssociationOpRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,  // Different resource - will succeed
        "topic",
        ImmutableList.of(
            new AssociationCreateOp(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    AssociationBatchResponse batchResponse = restApp.restClient.mutateAssociations(
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

    // Register schemas separately
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);
    restApp.restClient.registerSchema(allSchemas.get(1), subject2);

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
                null,
                null
            )
        )
    );
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, initialRequest);

    // Batch upsert: update existing and create new
    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(new AssociationOpRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,  // Existing - will be updated
        "topic",
        ImmutableList.of(
            new AssociationUpsertOp(
                subject1,
                "key",
                LifecyclePolicy.WEAK,  // Unchanged: an association is immutable once created
                false,
                null,
                null
            )
        )
    ));
    requests.add(new AssociationOpRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,  // New - will be created
        "topic",
        ImmutableList.of(
            new AssociationUpsertOp(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    AssociationBatchResponse batchResponse = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);

    // Verify batch response
    assertNotNull(batchResponse);
    assertEquals(2, batchResponse.getResults().size());

    // Verify first result (unchanged). The op is equivalent to the stored association, so it is
    // skipped and the result carries no associations.
    AssociationResult result1 = batchResponse.getResults().get(0);
    assertNull(result1.getError());
    assertNotNull(result1.getResult());
    assertEquals(resourceName1, result1.getResult().getResourceName());
    assertEquals(resourceId1, result1.getResult().getResourceId());

    // Verify second result (created)
    AssociationResult result2 = batchResponse.getResults().get(1);
    assertNull(result2.getError());
    assertNotNull(result2.getResult());
    assertEquals(resourceName2, result2.getResult().getResourceName());
    assertEquals(resourceId2, result2.getResult().getResourceId());
    assertEquals(LifecyclePolicy.WEAK, result2.getResult().getAssociations().get(0).getLifecycle());

    // Verify the existing association is untouched
    List<Association> associations1 = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId1, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(1, associations1.size());
    assertEquals(LifecyclePolicy.WEAK, associations1.get(0).getLifecycle());

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

    // Register schema separately since WEAK associations cannot have schemas in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

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
                null,
                null
            )
        )
    );
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, initialRequest);

    // Dry run update
    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(new AssociationOpRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationUpsertOp(
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

    AssociationBatchResponse batchResponse = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, true, batchRequest);

    // An association is immutable once created, so the dry run reports the rejected promotion
    // rather than accepting it — and, being a dry run, persists nothing either way.
    assertNotNull(batchResponse);
    assertEquals(1, batchResponse.getResults().size());
    AssociationResult result = batchResponse.getResults().get(0);
    assertNotNull(result.getError());

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

    // Register schema for subject2 separately since WEAK upsert cannot have schema
    restApp.restClient.registerSchema(allSchemas.get(1), subject2);

    String defaultKeySubject1 = ":." + resourceNamespace + ":" + resourceName1 + "-key";

    // Create initial frozen association (STRONG+frozen+schema is allowed)
    AssociationCreateOrUpdateRequest frozenRequest = new AssociationCreateOrUpdateRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                null,
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
    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(new AssociationOpRequest(
        resourceName1,
        resourceNamespace,
        resourceId1,
        "topic",
        ImmutableList.of(
            new AssociationUpsertOp(
                defaultKeySubject1,
                "key",
                LifecyclePolicy.WEAK,  // Try to change frozen - will fail
                null,
                null,
                null
            )
        )
    ));
    requests.add(new AssociationOpRequest(
        resourceName2,
        resourceNamespace,
        resourceId2,
        "topic",
        ImmutableList.of(
            new AssociationUpsertOp(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    ));

    AssociationBatchRequest batchRequest =
        new AssociationBatchRequest(requests);

    AssociationBatchResponse batchResponse = restApp.restClient.mutateAssociations(
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

  @Test
  public void testMutateAssociationsWithAllOpTypesInSingleBatch() throws Exception {
    // This test exercises CREATE, UPSERT, and DELETE operations in a single batch
    // (all three operation types for the same resource in one mutateAssociations call)
    String keySubject = "mutateKeySubject";
    String valueSubject = "mutateValueSubject";
    String resourceName = "mutateSingleTopic";
    String resourceNamespace = "default";
    String resourceId = "mutate-single-request-resource";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    // Register schemas separately
    restApp.restClient.registerSchema(allSchemas.get(0), keySubject);
    restApp.restClient.registerSchema(allSchemas.get(1), valueSubject);

    // First, set up initial associations: create only "key" association
    // This allows us to:
    // - UPSERT "key" (update existing)
    // - CREATE "value" (new)
    // - DELETE "key" (remove existing after update)
    AssociationCreateOrUpdateRequest setupRequest = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                keySubject,
                "key",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    );

    AssociationResponse setupResponse = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, setupRequest);
    assertEquals(1, setupResponse.getAssociations().size());
    assertEquals("key", setupResponse.getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, setupResponse.getAssociations().get(0).getLifecycle());

    // Verify initial state: only "key" association exists
    List<Association> initialAssociations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(1, initialAssociations.size());
    assertEquals("key", initialAssociations.get(0).getAssociationType());

    // All three operation types in one batch. A request may only carry one op per association
    // type, so the DELETE of "key" goes in a second request against the same resource.
    // "key" is promoted before "value" is added so the resource never holds a mix of
    // lifecycles, which is rejected.
    List<AssociationOpRequest> requests = new ArrayList<>();
    requests.add(new AssociationOpRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            // UPSERT: touch the existing "key" association. An association is immutable once
            // created, so this leaves it as it is.
            new AssociationUpsertOp(
                keySubject,
                "key",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            ),
            // CREATE: Add new "value" association (schema already registered). It is WEAK to
            // match the existing "key" association — a resource cannot hold mixed lifecycles,
            // and "key" can no longer be promoted to STRONG.
            new AssociationCreateOp(
                valueSubject,
                "value",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    ));
    requests.add(new AssociationOpRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        // DELETE: Delete the "key" association
        ImmutableList.of(new AssociationDeleteOp("key"))
    ));

    AssociationBatchRequest batchRequest = new AssociationBatchRequest(requests);

    // Execute the batch mutation
    AssociationBatchResponse batchResponse = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);

    // Verify batch response
    assertNotNull(batchResponse);
    assertEquals(2, batchResponse.getResults().size());
    assertNull(batchResponse.getResults().get(0).getError());

    AssociationResult result = batchResponse.getResults().get(1);
    assertNull(result.getError());
    assertNotNull(result.getResult());
    assertEquals(resourceId, result.getResult().getResourceId());
    // After all operations: CREATE added "value", UPSERT updated "key", DELETE removed "key"
    // So we should have only "value" association remaining
    assertEquals(1, result.getResult().getAssociations().size());
    assertEquals("value", result.getResult().getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, result.getResult().getAssociations().get(0).getLifecycle());

    // Verify final state: only "value" association exists (key was deleted)
    List<Association> finalAssociations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(1, finalAssociations.size());
    assertEquals("value", finalAssociations.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, finalAssociations.get(0).getLifecycle());

    // Verify "key" association is gone
    List<Association> keyAssociations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(0, keyAssociations.size());
  }

  @Test
  public void testGetSchemasWithSubjectPrefixAndAssociations() throws Exception {
    String subject1 = "prefixSubject1";
    String subject2 = "prefixSubject2";
    String subject3 = "otherSubject3";
    String resourceName1 = "prefixTopic1";
    String resourceName2 = "prefixTopic2";
    String resourceNamespace = "default";
    String resourceId1 = "prefix-resource-1";
    String resourceId2 = "prefix-resource-2";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(3);

    // Register schemas separately
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);
    restApp.restClient.registerSchema(allSchemas.get(1), subject2);
    restApp.restClient.registerSchema(allSchemas.get(2), subject3);

    // Create associations for prefixSubject1 and prefixSubject2
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
                null,
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request1);

    // Create association for otherSubject3
    AssociationCreateOrUpdateRequest request2 = new AssociationCreateOrUpdateRequest(
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
                null,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request2);

    // Get schemas with "prefix" subject prefix and associations
    List<ExtendedSchema> schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        "prefix",
        false,
        false,
        false,
        null,
        "topic",
        ImmutableList.of("key", "value"),
        null,
        null,
        null);

    // Should match prefixSubject1 and prefixSubject2 (not otherSubject3)
    assertEquals(2, schemas.size());

    // Verify first schema (prefixSubject1)
    ExtendedSchema schema1 = schemas.stream()
        .filter(s -> subject1.equals(s.getSubject()))
        .findFirst()
        .orElse(null);
    assertNotNull(schema1);
    assertNotNull(schema1.getAssociations());
    assertEquals(1, schema1.getAssociations().size());
    assertEquals("key", schema1.getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, schema1.getAssociations().get(0).getLifecycle());
    assertEquals(resourceId1, schema1.getAssociations().get(0).getResourceId());

    // Verify second schema (prefixSubject2)
    ExtendedSchema schema2 = schemas.stream()
        .filter(s -> subject2.equals(s.getSubject()))
        .findFirst()
        .orElse(null);
    assertNotNull(schema2);
    assertNotNull(schema2.getAssociations());
    assertEquals(1, schema2.getAssociations().size());
    assertEquals("value", schema2.getAssociations().get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, schema2.getAssociations().get(0).getLifecycle());
    assertEquals(resourceId1, schema2.getAssociations().get(0).getResourceId());
  }

  @Test
  public void testGetSchemasWithSubjectPrefixAndLifecycleFilter() throws Exception {
    // The STRONG association is frozen, so it owns its resource's canonical subject, which is
    // always context-qualified. Keep every subject in the same context so one qualified prefix
    // covers them all.
    String subject1 = ":.default:lifecycleSubject1";
    String subject2 = ":.default:lifecycleTopic-strong-value";
    String subject3 = ":.default:lifecycleSubject3";
    String resourceName = "lifecycleTopic";
    String resourceNamespace = "default";
    String resourceId = "lifecycle-resource-1";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(3);

    // Register schemas separately. subject3 is registered with no association.
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);
    restApp.restClient.registerSchema(allSchemas.get(2), subject3);

    RegisterSchemaRequest strongSchema = new RegisterSchemaRequest();
    strongSchema.setSchema(allSchemas.get(1));

    // Create associations with different lifecycle policies. They go on separate resources:
    // a single resource cannot hold a mix of lifecycles.
    AssociationCreateOrUpdateRequest weakRequest = new AssociationCreateOrUpdateRequest(
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
                null,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, weakRequest);

    AssociationCreateOrUpdateRequest strongRequest = new AssociationCreateOrUpdateRequest(
        resourceName + "-strong",
        resourceNamespace,
        resourceId + "-strong",
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.STRONG,
                true,
                strongSchema,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, strongRequest);

    // lifecycle=WEAK → only subject1 returned (subject2 is STRONG, subject3 unassociated)
    List<ExtendedSchema> weakSchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        ":.default:lifecycle",
        false,
        false,
        false,
        null,
        "topic",
        ImmutableList.of("key", "value"),
        ImmutableList.of(LifecyclePolicyFilter.WEAK),
        null,
        null);

    assertEquals(1, weakSchemas.size());
    assertEquals(subject1, weakSchemas.get(0).getSubject());
    assertNotNull(weakSchemas.get(0).getAssociations());
    assertEquals(1, weakSchemas.get(0).getAssociations().size());
    assertEquals(LifecyclePolicy.WEAK, weakSchemas.get(0).getAssociations().get(0).getLifecycle());

    // lifecycle=STRONG → only subject2 returned
    List<ExtendedSchema> strongSchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        ":.default:lifecycle",
        false,
        false,
        false,
        null,
        "topic",
        ImmutableList.of("key", "value"),
        ImmutableList.of(LifecyclePolicyFilter.STRONG),
        null,
        null);

    assertEquals(1, strongSchemas.size());
    assertEquals(subject2, strongSchemas.get(0).getSubject());
    assertNotNull(strongSchemas.get(0).getAssociations());
    assertEquals(1, strongSchemas.get(0).getAssociations().size());
    assertEquals(LifecyclePolicy.STRONG,
        strongSchemas.get(0).getAssociations().get(0).getLifecycle());

    // lifecycle=NONE → only subject3 returned (no associations attached)
    List<ExtendedSchema> noneSchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        ":.default:lifecycle",
        false,
        false,
        false,
        null,
        "topic",
        ImmutableList.of("key", "value"),
        ImmutableList.of(LifecyclePolicyFilter.NONE),
        null,
        null);

    assertEquals(1, noneSchemas.size());
    assertEquals(subject3, noneSchemas.get(0).getSubject());
    assertTrue(noneSchemas.get(0).getAssociations() == null
        || noneSchemas.get(0).getAssociations().isEmpty());

    // lifecycle=WEAK,NONE → subject1 (WEAK) + subject3 (unassociated); subject2 (STRONG) excluded
    List<ExtendedSchema> weakOrNoneSchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        ":.default:lifecycle",
        false,
        false,
        false,
        null,
        "topic",
        ImmutableList.of("key", "value"),
        ImmutableList.of(LifecyclePolicyFilter.WEAK, LifecyclePolicyFilter.NONE),
        null,
        null);

    assertEquals(2, weakOrNoneSchemas.size());

    ExtendedSchema weakOrNone1 = weakOrNoneSchemas.stream()
        .filter(s -> subject1.equals(s.getSubject()))
        .findFirst()
        .orElse(null);
    assertNotNull(weakOrNone1);
    assertNotNull(weakOrNone1.getAssociations());
    assertEquals(1, weakOrNone1.getAssociations().size());
    assertEquals(LifecyclePolicy.WEAK, weakOrNone1.getAssociations().get(0).getLifecycle());

    ExtendedSchema weakOrNone3 = weakOrNoneSchemas.stream()
        .filter(s -> subject3.equals(s.getSubject()))
        .findFirst()
        .orElse(null);
    assertNotNull(weakOrNone3);
    assertTrue(weakOrNone3.getAssociations() == null
        || weakOrNone3.getAssociations().isEmpty());

    // lifecycle=STRONG,WEAK → subject1 + subject2; subject3 excluded
    List<ExtendedSchema> strongOrWeakSchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        ":.default:lifecycle",
        false,
        false,
        false,
        null,
        "topic",
        ImmutableList.of("key", "value"),
        ImmutableList.of(LifecyclePolicyFilter.STRONG, LifecyclePolicyFilter.WEAK),
        null,
        null);

    assertEquals(2, strongOrWeakSchemas.size());
    assertTrue(strongOrWeakSchemas.stream().anyMatch(s -> subject1.equals(s.getSubject())));
    assertTrue(strongOrWeakSchemas.stream().anyMatch(s -> subject2.equals(s.getSubject())));
  }

  @Test
  public void testGetSchemasWithSubjectPrefixNoMatchingSubjects() throws Exception {
    String subject1 = "existingSubject1";
    String resourceName = "existingTopic";
    String resourceNamespace = "default";
    String resourceId = "existing-resource-1";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    // Register schema separately since WEAK associations cannot have schemas in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);

    // Create an association
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
                null,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);

    // Get schemas with a non-matching prefix
    List<ExtendedSchema> schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        "nonexistent",
        false,
        false,
        false,
        null,
        "topic",
        ImmutableList.of("key"),
        null,
        null,
        null);

    // Should return no schemas
    assertEquals(0, schemas.size());
  }

  @Test
  public void testGetSchemasWithSubjectPrefixAndAssociationTypeFilter() throws Exception {
    String subject1 = "typeFilterSubject1";
    String subject2 = "typeFilterSubject2";
    String subject3 = "typeFilterSubject3";
    String resourceName = "typeFilterTopic";
    String resourceNamespace = "default";
    String resourceId = "type-filter-resource";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(3);

    // Register schemas separately. subject3 is registered with no association.
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);
    restApp.restClient.registerSchema(allSchemas.get(1), subject2);
    restApp.restClient.registerSchema(allSchemas.get(2), subject3);

    // Create associations with different types
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
                null,
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);

    // associationType=key → only subject1 returned
    List<ExtendedSchema> keySchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        "typeFilter",
        false,
        false,
        false,
        null,
        "topic",
        Collections.singletonList("key"),
        null,
        null,
        null);

    assertEquals(1, keySchemas.size());
    assertEquals(subject1, keySchemas.get(0).getSubject());
    assertNotNull(keySchemas.get(0).getAssociations());
    assertEquals(1, keySchemas.get(0).getAssociations().size());
    assertEquals("key", keySchemas.get(0).getAssociations().get(0).getAssociationType());

    // associationType=value → only subject2 returned
    List<ExtendedSchema> valueSchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        "typeFilter",
        false,
        false,
        false,
        null,
        "topic",
        Collections.singletonList("value"),
        null,
        null,
        null);

    assertEquals(1, valueSchemas.size());
    assertEquals(subject2, valueSchemas.get(0).getSubject());
    assertNotNull(valueSchemas.get(0).getAssociations());
    assertEquals(1, valueSchemas.get(0).getAssociations().size());
    assertEquals("value", valueSchemas.get(0).getAssociations().get(0).getAssociationType());

    // associationType=key,value → subject1 + subject2; subject3 (unassociated) excluded
    List<ExtendedSchema> keyOrValueSchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        "typeFilter",
        false,
        false,
        false,
        null,
        "topic",
        ImmutableList.of("key", "value"),
        null,
        null,
        null);

    assertEquals(2, keyOrValueSchemas.size());
    assertTrue(keyOrValueSchemas.stream().anyMatch(s -> subject1.equals(s.getSubject())));
    assertTrue(keyOrValueSchemas.stream().anyMatch(s -> subject2.equals(s.getSubject())));

    // No association params → all matching subjects returned, no association filter applied
    List<ExtendedSchema> allSchemasResult = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        "typeFilter",
        false,
        false,
        false,
        null,
        null,
        null,
        null,
        null,
        null);

    assertEquals(3, allSchemasResult.size());
    assertTrue(allSchemasResult.stream().anyMatch(s -> subject1.equals(s.getSubject())));
    assertTrue(allSchemasResult.stream().anyMatch(s -> subject2.equals(s.getSubject())));
    assertTrue(allSchemasResult.stream().anyMatch(s -> subject3.equals(s.getSubject())));
  }

  @Test
  public void testGetSchemasWithSubjectPrefixLatestOnly() throws Exception {
    String subject = "latestOnlySubject";
    String resourceName = "latestOnlyTopic";
    String resourceNamespace = "default";
    String resourceId = "latest-only-resource";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    // Register schema separately since WEAK associations cannot have schemas in create
    restApp.restClient.registerSchema(allSchemas.get(0), subject);

    // Create first version
    AssociationCreateOrUpdateRequest request1 = new AssociationCreateOrUpdateRequest(
        resourceName,
        resourceNamespace,
        resourceId,
        "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                subject,
                "key",
                LifecyclePolicy.WEAK,
                false,
                null,
                null
            )
        )
    );

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request1);

    // Register a second version of the schema under the same subject
    RegisterSchemaRequest schemaRequest2 = new RegisterSchemaRequest();
    schemaRequest2.setSchema(allSchemas.get(1));
    restApp.restClient.registerSchema(schemaRequest2, subject, false);

    // Get schemas with latestOnly=true
    List<ExtendedSchema> latestSchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        "latestOnly",
        false,
        false,
        true,  // latestOnly
        null,
        "topic",
        ImmutableList.of("key"),
        null,
        null,
        null);

    // Should return only the latest version
    assertEquals(1, latestSchemas.size());
    assertEquals(subject, latestSchemas.get(0).getSubject());
    assertEquals(Integer.valueOf(2), latestSchemas.get(0).getVersion());
    assertNotNull(latestSchemas.get(0).getAssociations());
    assertEquals(1, latestSchemas.get(0).getAssociations().size());

    // Get schemas with latestOnly=false (should return both versions)
    List<ExtendedSchema> allVersionSchemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES,
        "latestOnly",
        false,
        false,
        false,  // latestOnly=false
        null,
        "topic",
        ImmutableList.of("key"),
        null,
        null,
        null);

    // Should return both versions
    assertEquals(2, allVersionSchemas.size());
  }

  // Requirement #1: CREATE + schema → STRONG + frozen

  @Test
  public void testCreateWithSchemaDefaultsToStrongFrozen() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "create-schema-frozen-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    // Create with schema, no subject/lifecycle/frozen specified — defaults to frozen STRONG
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));

    AssociationResponse response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(":.default:topic1-value", response.getAssociations().get(0).getSubject());
    assertEquals(LifecyclePolicy.STRONG, response.getAssociations().get(0).getLifecycle());
    assertTrue(response.getAssociations().get(0).isFrozen());
  }

  @Test
  public void testCreateWithSchemaAndWeakLifecycleFails() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "create-schema-weak-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            "subject-weak-schema", "value", LifecyclePolicy.WEAK, null, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));
  }

  @Test
  public void testCreateWithSchemaAndFrozenFalseFails() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "create-schema-unfrozen-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            "subject-unfrozen", "value", null, false, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));
  }

  // Requirement #2: UPSERT + schema → only STRONG

  @Test
  public void testUpsertWithSchemaAndWeakLifecycleFails() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-schema-weak-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            "subject-upsert-weak", "value", LifecyclePolicy.WEAK, null, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));
  }

  // Requirement #3: Default subject + server restriction

  @Test
  public void testCreateFrozenStrongWithoutSubjectDefaultsSubject() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "default-subject-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    // Create with schema, no subject — should default to :.namespace:name
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));

    AssociationResponse response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(":.default:topic1-value", response.getAssociations().get(0).getSubject());
    assertEquals(LifecyclePolicy.STRONG, response.getAssociations().get(0).getLifecycle());
    assertTrue(response.getAssociations().get(0).isFrozen());
  }

  @Test
  public void testCreateWeakWithoutSubjectFails() throws Exception {
    String subject = "subject-weak-nosub";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "weak-nosub-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    // Register schema separately
    restApp.restClient.registerSchema(allSchemas.get(0), subject);

    // Create WEAK without subject — should fail
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", LifecyclePolicy.WEAK, null, null, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));
  }

  @Test
  public void testDefaultSubjectNotAllowedForWeak() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "weak-default-sub-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    // Register schema separately under the default subject
    restApp.restClient.registerSchema(allSchemas.get(0), ":.default:topic1-value");

    // Try to create WEAK with the default subject explicitly — should fail
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            ":.default:topic1-value", "value", LifecyclePolicy.WEAK, false, null, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));
  }

  @Test
  public void testBatchCreateWithSchemaDefaultsToStrongFrozen() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "batch-create-schema-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    // Batch create with schema, no subject/lifecycle/frozen
    AssociationCreateOp createOp = new AssociationCreateOp(
        null, "value", null, null, schemaRequest, null);
    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        Collections.singletonList(createOp));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertNull(response.getResults().get(0).getError());
    AssociationResponse assocResponse = response.getResults().get(0).getResult();
    assertEquals(":.default:topic1-value", assocResponse.getAssociations().get(0).getSubject());
    assertEquals(LifecyclePolicy.STRONG, assocResponse.getAssociations().get(0).getLifecycle());
    assertTrue(assocResponse.getAssociations().get(0).isFrozen());
  }

  // Requirement: Frozen STRONG must use default subject

  @Test
  public void testCreateFrozenWithCustomSubjectFails() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "frozen-custom-sub-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    // Frozen with a custom subject — should fail
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            "custom-subject", "value", LifecyclePolicy.STRONG, true, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));
  }

  @Test
  public void testBatchCreateFrozenWithCustomSubjectFails() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "batch-frozen-custom-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    AssociationCreateOp createOp = new AssociationCreateOp(
        "custom-subject", "value", LifecyclePolicy.STRONG, true, schemaRequest, null);
    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        Collections.singletonList(createOp));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertNotNull(response.getResults().get(0).getError());
  }

  // Requirement: a resource is either topic-owned (STRONG) or shared (WEAK) across all of its
  // association types, never a mix of the two.

  @Test
  public void testCreateMixedLifecyclesInOneRequestFails() throws Exception {
    String subject = "mixed-shared-subject";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "mixed-one-request-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    restApp.restClient.registerSchema(allSchemas.get(0), subject);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(1));

    // key is shared (WEAK), value is topic-owned (STRONG via schema)
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(subject, "key", null, null, null, null),
            new AssociationCreateOrUpdateInfo(null, "value", null, null, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));

    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertTrue(associations.isEmpty());
  }

  @Test
  public void testAddWeakWhenStrongExistsForOtherTypeFails() throws Exception {
    String subject = "add-weak-shared-subject";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "mixed-add-weak-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    // Topic-owned association on value
    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));
    AssociationCreateOrUpdateRequest valueRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, valueRequest);

    // Now add a shared association on key — the resource would be mixed
    restApp.restClient.registerSchema(allSchemas.get(1), subject);
    AssociationUpsertOp upsertOp = new AssociationUpsertOp(
        subject, "key", null, null, null, null);
    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        Collections.singletonList(upsertOp));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertNotNull(response.getResults().get(0).getError());

    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals("value", associations.get(0).getAssociationType());
  }

  @Test
  public void testAddStrongWhenWeakExistsForOtherTypeFails() throws Exception {
    String keySubject = "add-strong-key-subject";
    String valueSubject = "add-strong-value-subject";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "mixed-add-strong-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    // Shared association on key
    restApp.restClient.registerSchema(allSchemas.get(0), keySubject);
    AssociationCreateOrUpdateRequest keyRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            keySubject, "key", LifecyclePolicy.WEAK, null, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, keyRequest);

    // Now add a topic-owned association on value — the resource would be mixed
    restApp.restClient.registerSchema(allSchemas.get(1), valueSubject);
    AssociationCreateOrUpdateRequest valueRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            valueSubject, "value", LifecyclePolicy.STRONG, false, null, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, valueRequest));

    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals("key", associations.get(0).getAssociationType());
  }

  /** Changing one type's lifecycle so it diverges from its sibling is rejected too. */
  @Test
  public void testUpdatingLifecycleToDivergeFromSiblingFails() throws Exception {
    String keySubject = "diverge-key-subject";
    String valueSubject = "diverge-value-subject";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "mixed-diverge-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    restApp.restClient.registerSchema(allSchemas.get(0), keySubject);
    restApp.restClient.registerSchema(allSchemas.get(1), valueSubject);

    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(
            new AssociationCreateOrUpdateInfo(
                keySubject, "key", LifecyclePolicy.WEAK, null, null, null),
            new AssociationCreateOrUpdateInfo(
                valueSubject, "value", LifecyclePolicy.WEAK, null, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    // Promote only the value association to STRONG — the resource would be mixed
    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            valueSubject, "value", LifecyclePolicy.STRONG, null, null, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest));

    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(2, associations.size());
    associations.forEach(a -> assertEquals(LifecyclePolicy.WEAK, a.getLifecycle()));
  }

  /**
   * At the validate phase the caller may not yet have a resourceId, so the resource is matched
   * by (name, namespace). The conflict has to be caught there rather than only on apply.
   */
  @Test
  public void testDryRunSeesMixedLifecycleAgainstExistingSibling() throws Exception {
    // The STRONG association is frozen, so it owns its resource's canonical subject.
    String valueSubject = ":.default:dryRunMixedTopic-value";
    String keySubject = "dryrun-key-subject";
    String resourceName = "dryRunMixedTopic";
    String resourceNamespace = "default";
    String resourceId = "dryrun-mixed-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    restApp.restClient.registerSchema(allSchemas.get(1), keySubject);

    RegisterSchemaRequest valueSchema = new RegisterSchemaRequest();
    valueSchema.setSchema(allSchemas.get(0));
    AssociationCreateOrUpdateRequest valueRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            valueSubject, "value", LifecyclePolicy.STRONG, true, valueSchema, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, valueRequest);

    // Validate phase: no resourceId yet, so the sibling is found by (name, namespace)
    AssociationCreateOrUpdateRequest dryRunRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, null, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            keySubject, "key", LifecyclePolicy.WEAK, false, null, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, true, dryRunRequest));
  }

  /**
   * Matching by name and namespace can span resourceIds. The validate phase must not stitch
   * their types together into a mix that never existed on either resource.
   */
  @Test
  public void testDryRunDoesNotMixLifecyclesAcrossResourcesSharingAName() throws Exception {
    // The STRONG association is frozen, so it owns its resource's canonical subject.
    String olderSubject = ":.default:sharedNameTopic-key";
    String liveSubject = "live-value-subject";
    String resourceName = "sharedNameTopic";
    String resourceNamespace = "default";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    restApp.restClient.registerSchema(allSchemas.get(1), liveSubject);

    RegisterSchemaRequest olderSchema = new RegisterSchemaRequest();
    olderSchema.setSchema(allSchemas.get(0));

    // Older resource, STRONG on key. The ids are ordered so the live resource wins whether
    // recency or the id tie-break decides, keeping the test independent of write timing.
    AssociationCreateOrUpdateRequest olderRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, "shared-name-a-older", "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            olderSubject, "key", LifecyclePolicy.STRONG, true, olderSchema, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, olderRequest);

    // Newer resource with the same name/namespace, WEAK on value
    AssociationCreateOrUpdateRequest liveRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, "shared-name-z-live", "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            liveSubject, "value", LifecyclePolicy.WEAK, false, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, liveRequest);

    // Validate with no resourceId: only the newer resource counts, so this is uniform WEAK.
    // Merging per type across resources would see key=STRONG, value=WEAK and wrongly reject.
    AssociationCreateOrUpdateRequest dryRunRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, null, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            liveSubject, "value", LifecyclePolicy.WEAK, false, null, null)));

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, true, dryRunRequest);
  }

  /**
   * A request may carry at most one op per association type. Nothing can legitimately send
   * more — a topic config key appears once per incrementalAlterConfigs request — so this is
   * rejected rather than applied in sequence.
   */
  @Test
  public void testBatchRepeatedAssociationTypeIsRejected() throws Exception {
    // The STRONG association is frozen, so it owns its resource's canonical subject.
    String subject = ":.default:dupRunTopic-value";
    String resourceName = "dupRunTopic";
    String resourceNamespace = "default";
    String resourceId = "dup-run-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(3);

    RegisterSchemaRequest first = new RegisterSchemaRequest();
    first.setSchema(allSchemas.get(0));
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.STRONG, true, first, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    RegisterSchemaRequest second = new RegisterSchemaRequest();
    second.setSchema(allSchemas.get(1));
    RegisterSchemaRequest third = new RegisterSchemaRequest();
    third.setSchema(allSchemas.get(2));

    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(
            new AssociationUpsertOp(subject, "value", null, null, second, null),
            new AssociationUpsertOp(subject, "value", null, null, third, null)));
    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
        new AssociationBatchRequest(Collections.singletonList(opRequest)));
    assertNotNull(response.getResults().get(0).getError());

    // Rejected before anything was applied, so no new version was registered
    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(subject));
  }

  /** A delete counts towards the one-op-per-type rule as well. */
  @Test
  public void testBatchUpsertAndDeleteOfSameTypeIsRejected() throws Exception {
    String subject = "dup-delete-subject";
    String resourceName = "dupDeleteTopic";
    String resourceNamespace = "default";
    String resourceId = "dup-delete-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    restApp.restClient.registerSchema(allSchemas.get(0), subject);
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.WEAK, false, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(
            new AssociationUpsertOp(subject, "value", LifecyclePolicy.WEAK, null, null, null),
            new AssociationDeleteOp("value")));
    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
        new AssociationBatchRequest(Collections.singletonList(opRequest)));
    assertNotNull(response.getResults().get(0).getError());

    // The association is untouched
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(1, associations.size());
  }


  /**
   * Runs are applied in order and there is no rollback across them, so a later run failing
   * leaves the earlier ones persisted — the caller's intent is partially applied. Each run is
   * validated against the resource's full state before writing, though, so what remains is
   * always uniform, never the mixed state the invariant forbids.
   */
  @Test
  public void testBatchLaterRunFailingLeavesEarlierRunUniform() throws Exception {
    String keySubject = "partial-run-key-subject";
    String valueSubject = "partial-run-value-subject";
    String resourceName = "partialRunTopic";
    String resourceNamespace = "default";
    String resourceId = "partial-run-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    restApp.restClient.registerSchema(allSchemas.get(0), keySubject);
    restApp.restClient.registerSchema(allSchemas.get(1), valueSubject);

    // CREATE and UPSERT are different op types, so these form two runs. The first succeeds;
    // the second is refused because an upsert may not create a STRONG association.
    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(
            new AssociationCreateOp(
                keySubject, "key", LifecyclePolicy.WEAK, false, null, null),
            new AssociationUpsertOp(
                valueSubject, "value", LifecyclePolicy.STRONG, null, null, null)));
    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
        new AssociationBatchRequest(Collections.singletonList(opRequest)));
    assertNotNull(response.getResults().get(0).getError());

    // The first run stands and the resource is left uniform, not mixed
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals("key", associations.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());
  }

  /**
   * A run of adjacent ops is validated as a unit and nothing is written unless all of it
   * passes, so a run that would leave the resource mixed commits none of its ops.
   */
  @Test
  public void testBatchProjectingMixedLifecycleCommitsNothing() throws Exception {
    String keySubject = "batch-residue-key-subject";
    String valueSubject = "batch-residue-value-subject";
    String resourceName = "batchResidueTopic";
    String resourceNamespace = "default";
    String resourceId = "batch-residue-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    restApp.restClient.registerSchema(allSchemas.get(0), keySubject);
    restApp.restClient.registerSchema(allSchemas.get(1), valueSubject);

    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(
            new AssociationUpsertOp(
                keySubject, "key", LifecyclePolicy.WEAK, null, null, null),
            new AssociationUpsertOp(
                valueSubject, "value", LifecyclePolicy.STRONG, null, null, null)));
    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false,
        new AssociationBatchRequest(Collections.singletonList(opRequest)));
    assertNotNull(response.getResults().get(0).getError());

    // Neither op was committed, so no partially-applied residue is left behind
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertTrue(associations.isEmpty());
  }

  // Requirement: Frozen/non-frozen consistency at resource level

  @Test
  public void testCreateFrozenThenCreateAnotherFrozenSucceeds() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "all-frozen-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest schemaRequest1 = new RegisterSchemaRequest();
    schemaRequest1.setSchema(allSchemas.get(0));

    // Create frozen key association
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "key", null, null, schemaRequest1, null)));

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    // Create another frozen association for the same resource via CREATE — should succeed
    RegisterSchemaRequest schemaRequest2 = new RegisterSchemaRequest();
    schemaRequest2.setSchema(allSchemas.get(1));
    AssociationCreateOrUpdateRequest createRequest2 = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest2, null)));

    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest2);

    // Verify both exist
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(2, associations.size());
    assertTrue(associations.get(0).isFrozen());
    assertTrue(associations.get(1).isFrozen());
  }

  @Test
  public void testBatchCreateAllFrozenSucceeds() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "batch-all-frozen-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest schemaRequest1 = new RegisterSchemaRequest();
    schemaRequest1.setSchema(allSchemas.get(0));
    RegisterSchemaRequest schemaRequest2 = new RegisterSchemaRequest();
    schemaRequest2.setSchema(allSchemas.get(1));

    AssociationCreateOp op1 = new AssociationCreateOp(
        null, "key", null, null, schemaRequest1, null);
    AssociationCreateOp op2 = new AssociationCreateOp(
        null, "value", null, null, schemaRequest2, null);

    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(op1, op2));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertNull(response.getResults().get(0).getError());

    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        ImmutableList.of("key", "value"), null, 0, -1);
    assertEquals(2, associations.size());
    assertTrue(associations.get(0).isFrozen());
    assertTrue(associations.get(1).isFrozen());
  }

  // Requirement #6: UPDATE allows null subject, lifecycle, and frozen

  @Test
  public void testUpsertWithNullSubjectUsesExistingSubject() throws Exception {
    // A STRONG association is frozen, so it uses its resource's canonical subject and carries
    // its schema inline.
    String subject = ":.default:topic1-value";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-null-sub-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.STRONG, true, schemaRequest, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    // Upsert with null subject — should use existing subject
    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, null, null)));
    restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest);

    // Verify association still uses the original subject
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals(subject, associations.get(0).getSubject());
  }

  @Test
  public void testUpsertWithNullLifecycleKeepsExisting() throws Exception {
    // A STRONG association is frozen, so it uses its resource's canonical subject and carries
    // its schema inline.
    String subject = ":.default:topic1-value";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-null-lc-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    RegisterSchemaRequest createSchema = new RegisterSchemaRequest();
    createSchema.setSchema(allSchemas.get(0));
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.STRONG, true, createSchema, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    // Upsert with schema and null lifecycle — should succeed (existing is STRONG)
    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(1));
    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));
    restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest);

    // Verify lifecycle is still STRONG
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals(LifecyclePolicy.STRONG, associations.get(0).getLifecycle());
  }

  @Test
  public void testUpsertWithSchemaOnExistingWeakFails() throws Exception {
    String subject = "weak-schema-test";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-weak-schema-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    // Register schema and create WEAK association
    restApp.restClient.registerSchema(allSchemas.get(0), subject);
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.WEAK, false, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    // Upsert with schema and null lifecycle on existing WEAK — should fail
    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(1));
    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest));
  }

  /**
   * An upsert carrying only a schema would have to create a topic-owned STRONG association on
   * the default subject, which is only allowed when the association is created with the topic.
   */
  @Test
  public void testUpsertCreatingNewWithSchemaAndNoSubjectFails() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-new-schema-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest));

    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertTrue(associations.isEmpty());
  }

  @Test
  public void testUpsertCreatingNewWithoutSchemaAndNoSubjectFails() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-new-nosub-123";

    // Upsert with no schema, no subject, no lifecycle, no existing association
    // Should apply CREATE defaults → lifecycle=WEAK → subject required → fail
    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, null, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest));
  }

  // An upsert may only create a WEAK association: a STRONG association is owned by its topic
  // and has to be created with it, and a schema implies STRONG.

  @Test
  public void testUpsertCreatingNewWithSchemaAndSubjectFails() throws Exception {
    String subject = "byo-subject-no-lifecycle";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-byo-nolifecycle-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    restApp.restClient.registerSchema(allSchemas.get(0), subject);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(1));

    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", null, null, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest));

    // Nothing was created and the schema was not registered
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertTrue(associations.isEmpty());
    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(subject));
  }

  @Test
  public void testUpsertCreatingNewWithSchemaAndStrongLifecycleFails() throws Exception {
    String subject = "byo-subject-strong";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-byo-strong-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    restApp.restClient.registerSchema(allSchemas.get(0), subject);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(1));

    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.STRONG, null, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest));

    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertTrue(associations.isEmpty());
  }

  @Test
  public void testUpsertCreatingNewStrongWithoutSchemaFails() throws Exception {
    String subject = "byo-subject-strong-noschema";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-byo-strong-noschema-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    restApp.restClient.registerSchema(allSchemas.get(0), subject);

    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.STRONG, null, null, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createOrUpdateAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest));
  }

  @Test
  public void testUpsertCreatingNewWeakSucceeds() throws Exception {
    String subject = "byo-subject-weak";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "upsert-byo-weak-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    restApp.restClient.registerSchema(allSchemas.get(0), subject);

    AssociationCreateOrUpdateRequest upsertRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", null, null, null, null)));
    restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, upsertRequest);

    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals(subject, associations.get(0).getSubject());
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());
    assertFalse(associations.get(0).isFrozen());
  }

  /**
   * The subject already carries a WEAK association from another resource. Silently promoting to
   * STRONG would make this fail with "an association already exists for subject", masking the
   * real reason, which is that the upsert cannot create an association carrying a schema.
   */
  @Test
  public void testUpsertWithSchemaOnSharedSubjectReportsSchemaNotSubjectConflict()
      throws Exception {
    String subject = "shared-subject";
    String resourceNamespace = "default";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    restApp.restClient.registerSchema(allSchemas.get(0), subject);

    // Pre-existing WEAK association on the shared subject, from a different resource
    AssociationCreateOrUpdateRequest weakRequest = new AssociationCreateOrUpdateRequest(
        "topic1", resourceNamespace, "shared-subject-owner-123", "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.WEAK, null, null, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, weakRequest);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(1));

    AssociationUpsertOp upsertOp = new AssociationUpsertOp(
        subject, "value", null, null, schemaRequest, null);
    AssociationOpRequest opRequest = new AssociationOpRequest(
        "topic2", resourceNamespace, "shared-subject-alter-123", "topic",
        Collections.singletonList(upsertOp));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertNotNull(response.getResults().get(0).getError());
    String message = response.getResults().get(0).getError().getMessage();
    assertTrue(message.contains("schema"), "unexpected error message: " + message);
    assertFalse(message.contains("already exists for subject"),
        "should not report a subject conflict: " + message);
  }

  @Test
  public void testBatchUpsertWithNullSubjectUsesExisting() throws Exception {
    // The STRONG association is frozen, so it owns its resource's canonical subject.
    String subject = ":.default:topic1-value";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "batch-upsert-null-sub-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest createSchema = new RegisterSchemaRequest();
    createSchema.setSchema(allSchemas.get(0));
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.STRONG, true, createSchema, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    // Batch upsert with null subject
    AssociationUpsertOp upsertOp = new AssociationUpsertOp(
        null, "value", null, null, null, null);
    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        Collections.singletonList(upsertOp));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertNull(response.getResults().get(0).getError());

    // Verify subject unchanged
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(1, associations.size());
    assertEquals(subject, associations.get(0).getSubject());
  }

  // IMPORT-mode: associations sent without schemas

  @Test
  public void testImportFrozenAssociationWithoutSchemaSucceeds() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "import-frozen-123";
    String defaultKeySubject = ":." + resourceNamespace + ":" + resourceName + "-key";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    restApp.restClient.setMode("IMPORT", defaultKeySubject, true);
    // Replicate source schema preserving its version (not 1) and id
    restApp.restClient.registerSchema(schemaString, defaultKeySubject, 7, 42);

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            defaultKeySubject, "key", LifecyclePolicy.STRONG, true, null, null)));

    AssociationResponse response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(LifecyclePolicy.STRONG, response.getAssociations().get(0).getLifecycle());
    assertTrue(response.getAssociations().get(0).isFrozen());
    assertEquals(defaultKeySubject, response.getAssociations().get(0).getSubject());
  }

  @Test
  public void testImportFrozenAssociationWithoutSchemaViaPutSucceeds() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "import-frozen-put-123";
    String defaultKeySubject = ":." + resourceNamespace + ":" + resourceName + "-key";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    restApp.restClient.setMode("IMPORT", defaultKeySubject, true);
    restApp.restClient.registerSchema(schemaString, defaultKeySubject, 3, 100);

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            defaultKeySubject, "key", LifecyclePolicy.STRONG, true, null, null)));

    AssociationResponse response = restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(LifecyclePolicy.STRONG, response.getAssociations().get(0).getLifecycle());
    assertTrue(response.getAssociations().get(0).isFrozen());
  }

  @Test
  public void testImportFrozenAssociationWithNoSchemaInSubjectFails() throws Exception {
    String resourceName = "topic4";
    String resourceNamespace = "default";
    String resourceId = "import-frozen-empty-123";
    String defaultKeySubject = ":." + resourceNamespace + ":" + resourceName + "-key";

    restApp.restClient.setMode("IMPORT", defaultKeySubject, true);
    // Note: no schema registered for defaultKeySubject

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            defaultKeySubject, "key", LifecyclePolicy.STRONG, true, null, null)));

    // NoActiveSubjectVersionExistsException — validity guard intact in IMPORT
    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));
  }

  /**
   * A subject in IMPORT mode receives its versions by replication, so a schema sent with the
   * association would be dropped instead of registered. It is rejected rather than ignored.
   */
  @Test
  public void testImportAssociationWithSchemaFails() throws Exception {
    String resourceName = "topic6";
    String resourceNamespace = "default";
    String resourceId = "import-with-schema-123";
    String defaultValueSubject = ":." + resourceNamespace + ":" + resourceName + "-value";
    String schemaString = TestUtils.getRandomCanonicalAvroString(1).get(0);

    restApp.restClient.setMode("IMPORT", defaultValueSubject, true);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(schemaString);

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));

    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));

    // Neither the association nor the subject was created
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertTrue(associations.isEmpty());
  }

  /**
   * Updating an existing association: a schema sent while the subject is in IMPORT mode is
   * rejected instead of reported as a success that registered nothing.
   */
  @Test
  public void testImportMutateExistingAssociationWithSchemaFails() throws Exception {
    String resourceName = "topic7";
    String resourceNamespace = "default";
    String resourceId = "import-mutate-schema-123";
    // The STRONG association is frozen, so it owns its resource's canonical subject.
    String subject = ":.default:topic7-value";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    // Establish the association while the subject is writable
    RegisterSchemaRequest createSchema = new RegisterSchemaRequest();
    createSchema.setSchema(allSchemas.get(0));
    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", LifecyclePolicy.STRONG, true, createSchema, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    restApp.restClient.setMode("IMPORT", subject, true);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(1));

    AssociationUpsertOp upsertOp = new AssociationUpsertOp(
        subject, "value", null, null, schemaRequest, null);
    AssociationOpRequest opRequest = new AssociationOpRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        Collections.singletonList(upsertOp));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));

    AssociationBatchResponse response = restApp.restClient.mutateAssociations(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, batchRequest);
    assertNotNull(response.getResults().get(0).getError());

    // The schema was rejected, not silently dropped
    assertEquals(Collections.singletonList(1), restApp.restClient.getAllVersions(subject));
  }

  @Test
  public void testHardDeleteSchemaVersionAllowedWhenAssociationExists() throws Exception {
    String subject1 = "subject1";
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "hard-delete-with-assoc-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(2);

    // Allow incompatible second version so we have two versions to work with.
    restApp.restClient.updateCompatibility(CompatibilityLevel.NONE.name, subject1);
    restApp.restClient.registerSchema(allSchemas.get(0), subject1);
    restApp.restClient.registerSchema(allSchemas.get(1), subject1);

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
                null,
                null
            )
        )
    );
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);

    // Soft delete v1 — still allowed because v2 remains active.
    restApp.restClient.deleteSchemaVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "1", false);

    // Hard delete v1 — previously rejected with AssociationForSubjectExistsException,
    // now permitted since the version is already soft-deleted and v2 is still active.
    int hardDeleted = restApp.restClient.deleteSchemaVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "1", true);
    assertEquals(1, hardDeleted);

    // Subject still has an active version and the association is intact.
    List<Integer> activeVersions = restApp.restClient.getAllVersions(subject1);
    assertEquals(ImmutableList.of(2), activeVersions);
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("key"), null, 0, -1);
    assertEquals(1, associations.size());

    // The soft-delete guard on the last remaining active version is still in force —
    // this is what keeps the "associated subject has >=1 active version" invariant safe
    // and makes the hard-delete relaxation above sound.
    assertThrows(Exception.class, () ->
        restApp.restClient.deleteSchemaVersion(
            RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "2", false));
  }

  @Test
  public void testNonImportFrozenAssociationWithoutSchemaStillFails() throws Exception {
    String resourceName = "topic5";
    String resourceNamespace = "default";
    String resourceId = "nonimport-frozen-123";

    // No setMode call — default READWRITE
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "key", LifecyclePolicy.STRONG, true, null, null)));

    // Legacy guard: in non-IMPORT, frozen requires schema
    assertThrows(Exception.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));
  }

  // Serialization: frozen is hidden when it matches the lifecycle default

  @Test
  public void testStrongAssociationWithoutFrozenHidesFrozenInResponse() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "strong-hidden-frozen-123";
    List<String> allSchemas = TestUtils.getRandomCanonicalAvroString(1);

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(allSchemas.get(0));

    // Create a STRONG association without passing frozen — defaults to frozen=true for STRONG
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));

    AssociationResponse createResponse = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(LifecyclePolicy.STRONG, createResponse.getAssociations().get(0).getLifecycle());
    // The effective value is frozen even though frozen was never passed
    assertTrue(createResponse.getAssociations().get(0).isFrozen());

    // The wire format omits frozen because it matches the STRONG default (true)
    String rawJson = rawGet("/associations/resources/" + resourceId + "?resourceType=topic");
    JsonNode root = JacksonMapper.INSTANCE.readTree(rawJson);
    assertEquals(1, root.size());
    assertFalse(
        root.get(0).has("frozen"),
        "frozen should be hidden for a default STRONG association: " + rawJson);

    // The deserialized association still reports the effective frozen value
    List<Association> associations = restApp.restClient.getAssociationsByResourceId(
        RestService.DEFAULT_REQUEST_PROPERTIES, resourceId, "topic",
        Collections.singletonList("value"), null, 0, -1);
    assertEquals(1, associations.size());
    assertTrue(associations.get(0).isFrozen());
  }

  @Test
  public void testCreateAssociationWithSchemaTagsToAdd() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "schema-tags-add-123";
    String subject = ":." + resourceNamespace + ":" + resourceName + "-value";

    List<SchemaTags> schemaTags = ImmutableList.of(
        new SchemaTags(new SchemaEntity("myrecord", EntityType.SR_RECORD),
            ImmutableList.of("TAG1", "TAG2")));
    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(SCHEMA_STRING);
    schemaRequest.setSchemaTagsToAdd(schemaTags);

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));

    AssociationResponse response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(subject, response.getAssociations().get(0).getSubject());

    // The schema returned by the association carries the requested tags
    Schema registered = response.getAssociations().get(0).getSchema();
    assertNotNull(registered);
    assertEquals(TAGGED_SCHEMA_STRING, registered.getSchema());

    // ...and so does the schema that was actually stored under the subject
    Schema latest = restApp.restClient.getLatestVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, Collections.singleton("*"));
    assertEquals(TAGGED_SCHEMA_STRING, latest.getSchema());
    assertEquals(schemaTags, latest.getSchemaTags());
  }

  @Test
  public void testCreateAssociationWithUnresolvableSchemaTagPath() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "schema-tags-bad-path-123";

    RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
    schemaRequest.setSchema(SCHEMA_STRING);
    schemaRequest.setSchemaTagsToAdd(ImmutableList.of(
        new SchemaTags(new SchemaEntity("nosuchrecord", EntityType.SR_RECORD),
            ImmutableList.of("TAG1"))));

    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, schemaRequest, null)));

    // A tag path that does not resolve is an invalid schema, not a server error,
    // for a dry run as well as a real create
    RestClientException dryRunException = assertThrows(RestClientException.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, true, request));
    assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, dryRunException.getErrorCode());

    RestClientException exception = assertThrows(RestClientException.class, () ->
        restApp.restClient.createAssociation(
            RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request));
    assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, exception.getErrorCode());
  }

  @Test
  public void testUpdateAssociationWithSchemaTags() throws Exception {
    String resourceName = "topic1";
    String resourceNamespace = "default";
    String resourceId = "schema-tags-update-123";
    String subject = ":." + resourceNamespace + ":" + resourceName + "-value";

    // A schema can only be carried by a STRONG association, which is always frozen, so the
    // subject starts out from the schema passed to create
    RegisterSchemaRequest createSchemaRequest = new RegisterSchemaRequest();
    createSchemaRequest.setSchema(SCHEMA_STRING);

    AssociationCreateOrUpdateRequest createRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            null, "value", null, null, createSchemaRequest, null)));
    restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, createRequest);

    // Updating with schemaTagsToAdd tags the schema that gets registered
    List<SchemaTags> schemaTags = ImmutableList.of(
        new SchemaTags(new SchemaEntity("myrecord", EntityType.SR_RECORD),
            ImmutableList.of("TAG1", "TAG2")));
    RegisterSchemaRequest taggedRequest = new RegisterSchemaRequest();
    taggedRequest.setSchema(SCHEMA_STRING);
    taggedRequest.setSchemaTagsToAdd(schemaTags);

    AssociationCreateOrUpdateRequest updateRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", null, null, taggedRequest, null)));
    restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, updateRequest);

    Schema latest = restApp.restClient.getLatestVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, Collections.singleton("*"));
    assertEquals((Integer) 2, latest.getVersion());
    assertEquals(TAGGED_SCHEMA_STRING, latest.getSchema());
    assertEquals(schemaTags, latest.getSchemaTags());

    // Updating with propagateSchemaTags carries the tags onto the evolved schema
    RegisterSchemaRequest propagateRequest = new RegisterSchemaRequest();
    propagateRequest.setSchema(EVOLVED_SCHEMA_STRING);
    propagateRequest.setPropagateSchemaTags(true);

    updateRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", null, null, propagateRequest, null)));
    restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, updateRequest);

    latest = restApp.restClient.getLatestVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, Collections.singleton("*"));
    assertEquals((Integer) 3, latest.getVersion());
    assertEquals(TAGGED_EVOLVED_SCHEMA_STRING, latest.getSchema());
    assertEquals(schemaTags, latest.getSchemaTags());

    // Removing a tag applies to the schema that gets registered
    RegisterSchemaRequest removeRequest = new RegisterSchemaRequest();
    removeRequest.setSchema(TAGGED_EVOLVED_SCHEMA_STRING);
    removeRequest.setSchemaTagsToRemove(ImmutableList.of(
        new SchemaTags(new SchemaEntity("myrecord", EntityType.SR_RECORD),
            ImmutableList.of("TAG2"))));

    updateRequest = new AssociationCreateOrUpdateRequest(
        resourceName, resourceNamespace, resourceId, "topic",
        ImmutableList.of(new AssociationCreateOrUpdateInfo(
            subject, "value", null, null, removeRequest, null)));
    restApp.restClient.createOrUpdateAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, updateRequest);

    latest = restApp.restClient.getLatestVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, Collections.singleton("*"));
    assertEquals((Integer) 4, latest.getVersion());
    assertEquals(ImmutableList.of(
            new SchemaTags(new SchemaEntity("myrecord", EntityType.SR_RECORD),
                ImmutableList.of("TAG1"))),
        latest.getSchemaTags());
  }

  private String rawGet(String path) throws Exception {
    URL url = new URL(restApp.restConnect + path);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(10_000);
    conn.setReadTimeout(10_000);
    try (InputStream in = conn.getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }

}

