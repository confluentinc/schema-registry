/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client;

import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateRequest;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class MockSchemaRegistryClientTest {
    private SchemaRegistryClient client;

    @Before
    public void setUp() {
        this.client = new MockSchemaRegistryClient();
    }

    @Test
    public void testAssociationCreateRequestValidationLogic() {
        // Pre-create subjects used for testing
        try {
            client.register("testKey", new AvroSchema("{\"type\": \"string\"}"), true);
            client.register("testValue", new AvroSchema("{\"type\": \"string\"}"), true);
        } catch (Exception e) {
            assertNull("Schema registration should succeed.", e);
        }

        AssociationCreateInfo createInfo1 = new AssociationCreateInfo(
                "testKey", "key", null, false, null, false);
        AssociationCreateInfo createInfo2 = new AssociationCreateInfo(
                "testValue", "value", null, false, null, false);

        // Invalid requests
        List<AssociationCreateRequest> invalidRequests = new ArrayList<>();

        // No resource name
        invalidRequests.add(new AssociationCreateRequest(
                null, "lkc1", "test-id", "topic",
                Arrays.asList(createInfo1, createInfo2)));

        // No resource namespace
        invalidRequests.add(new AssociationCreateRequest(
                "test", null, "test-id", "topic",
                Arrays.asList(createInfo1, createInfo2)));

        // No resource id
        invalidRequests.add(new AssociationCreateRequest(
                "test", "lkc1", null, "topic",
                Arrays.asList(createInfo1, createInfo2)));

        // No associations
        invalidRequests.add(new AssociationCreateRequest(
                "test", "lkc1", "test-id", "topic", null));

        // No subject name in AssociationCreateInfo
        invalidRequests.add(new AssociationCreateRequest(
                "test", "lkc1", "test-id", "topic",
                Collections.singletonList(
                        new AssociationCreateInfo(null, "value", null, false, null, false)
                )));

        // Unsupported ResourceType
        invalidRequests.add(new AssociationCreateRequest(
                "test", "lkc1", "test-id", "topic2",
                Arrays.asList(createInfo1, createInfo2)));

        // Unsupported AssociationType
        invalidRequests.add(new AssociationCreateRequest(
                "test", "lkc1", "test-id", "topic",
                Collections.singletonList(
                        new AssociationCreateInfo("testValue", "value2", null, false, null, false)
                )));

        // Duplicate AssociationType in the request
        invalidRequests.add(new AssociationCreateRequest(
                "test", "lkc1", "test-id", "topic",
                Arrays.asList(
                        new AssociationCreateInfo("testKey", "value", null, false, null, false),
                        new AssociationCreateInfo("testValue", "value", null, false, null, false)
                )));

        // Weak association with frozen to be true
        invalidRequests.add(new AssociationCreateRequest(
                "test", "lkc1", "test-id", "topic",
                Collections.singletonList(
                        new AssociationCreateInfo("testValue", null, LifecyclePolicy.WEAK, true, null, false)
                )));

        // Test all invalid requests - they should throw exceptions
        for (AssociationCreateRequest invalidRequest : invalidRequests) {
            try {
                client.createAssociation(invalidRequest);
                fail("Expected exception for invalid request");
            } catch (Exception e) {
                // Expected - validation should fail
                assertNotNull("Error should not be null", e);
            }
        }

        // Minimum valid request
        AssociationCreateRequest createRequest = new AssociationCreateRequest(
                "test", "lkc1", "test-id", null,
                Collections.singletonList(
                        new AssociationCreateInfo("testValue", null, null, false, null, false)
                ));
        AssociationResponse createResponse = null;
        try {
            createResponse = client.createAssociation(createRequest);
        } catch (Exception e) {
            assertNull("Error should be null", e);
        }

        // Assertions
        assertNotNull("Response should not be null", createResponse);
        assertEquals("ResourceName should match",
                createRequest.getResourceName(), createResponse.getResourceName());
        assertEquals("ResourceNamespace should match",
                createRequest.getResourceNamespace(), createResponse.getResourceNamespace());
        assertEquals("ResourceId should match",
                createRequest.getResourceId(), createResponse.getResourceId());
        assertEquals("ResourceType should be 'topic'",
                "topic", createResponse.getResourceType());
        assertEquals("Should have 1 association",
                1, createResponse.getAssociations().size());

        AssociationInfo association = createResponse.getAssociations().get(0);
        assertEquals("Subject should match",
                createRequest.getAssociations().get(0).getSubject(), association.getSubject());
        assertEquals("AssociationType should be 'value'",
                "value", association.getAssociationType());
        assertEquals("Lifecycle should be STRONG",
                LifecyclePolicy.STRONG, association.getLifecycle());
        assertFalse("Frozen should be false", association.isFrozen());
        assertNull("Schema should be null", association.getSchema());
        //assertFalse("Normalize should be false", association.isNormalize());
    }

    @Test
    public void testCreateOneAssociationInCreateRequest() {
        // Pre-create subjects
        String testValueSubject = "testValue";
        AvroSchema schemaInfo = new AvroSchema(
                "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\"," +
                        "\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}");
        try {
            client.register(testValueSubject, schemaInfo, true);
        } catch (Exception e) {
            assertNull("Schema registration should succeed.", e);
        }

        // Make an association with an existing subject without new schema
        AssociationCreateRequest createRequest = new AssociationCreateRequest(
                "test", "lkc1", "test-id", null,
                Collections.singletonList(new AssociationCreateInfo(testValueSubject, null,
                        null, false, null, false)
                ));
        try {
            client.createAssociation(createRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }

        // Create association request is idempotent. Re-issue the same create request should succeed.
        try {
            client.createAssociation(createRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }

        // Re-issue the same request with different association property (except schema) will error out.
        createRequest = new AssociationCreateRequest(
                "test", "lkc1", "test-id", null,
                Collections.singletonList(
                        new AssociationCreateInfo(testValueSubject, null, LifecyclePolicy.WEAK, false, null, false)
                ));

        try {
            client.createAssociation(createRequest);
            fail("Expected exception - existing association gets modified");
        } catch (Exception e) {
            assertNotNull("Error should not be null", e);
        }

        // Make an association with an existing subject with new schema
        Schema updatedSchema = new Schema(null, null, null, null, null,
                "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\"," +
                        "\"fields\":[{\"type\":\"string\",\"name\":\"id\"}, {\"type\":\"string\",\"name\":\"id2\"}]}");
        createRequest = new AssociationCreateRequest(
                "test", "lkc1", "test-id", null,
                Collections.singletonList(
                        new AssociationCreateInfo(testValueSubject, null, null, false, updatedSchema, false)
                ));
        try {
            client.createAssociation(createRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }

        // Make an association with a new subject without new schema. Test should fail.
        testValueSubject = "testValue2";
        createRequest = new AssociationCreateRequest(
                "test2", "lkc1", "test-id2", null,
                Collections.singletonList(
                        new AssociationCreateInfo(testValueSubject, null, null, false, null, false)
                ));

        try {
            client.createAssociation(createRequest);
            fail("Expected exception - new subject without schema");
        } catch (Exception e) {
            assertNotNull("Error should not be null", e);
        }

        // Make an association with a new subject with new schema
        testValueSubject = "testValue2";
        createRequest = new AssociationCreateRequest(
                "test2", "lkc1", "test-id2", null,
                Collections.singletonList(
                        new AssociationCreateInfo(testValueSubject, null, null, false, updatedSchema, false)
                ));
        try {
            client.createAssociation(createRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }
    }

    @Test
    public void testCreateMultipleAssociationsInCreateRequest() {
        String schemaString =
                "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\"," +
                        "\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}";

        AvroSchema schemaInfo = new AvroSchema(schemaString);

        // Scenario 1: Both associations using existing subjects
        String keySubject = "test1Key";
        String valueSubject = "test1Value";
        String resourceName = "test1";
        String resourceID = "test1-id";

        // Pre-create subjects
        try {
            client.register(keySubject, schemaInfo, true);
            client.register(valueSubject, schemaInfo, true);
        } catch (Exception e) {
            assertNull("Schema registration should succeed.", e);
        }

        AssociationCreateRequest createRequest = new AssociationCreateRequest(
                resourceName, "lkc1", resourceID, null,
                Arrays.asList(
                        new AssociationCreateInfo(keySubject, "key", null, false, null, false),
                        new AssociationCreateInfo(valueSubject, "value", null, false, null, false)
                ));
        try {
            client.createAssociation(createRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }

        // Scenario 2: One using existing subject, one creating new subject
        keySubject = "test2Key";
        valueSubject = "test2Value";
        resourceName = "test2";
        resourceID = "test2-id";

        try {
            client.register(keySubject, schemaInfo, true);
        } catch (Exception e) {
            assertNull("Schema registration should succeed.", e);
        }

        Schema schema = new Schema(null, null, null, "AVRO", Collections.emptyList(), schemaString);

        createRequest = new AssociationCreateRequest(
                resourceName, "lkc1", resourceID, null,
                Arrays.asList(
                        new AssociationCreateInfo(keySubject, "key", null, false, null, false),
                        new AssociationCreateInfo(valueSubject, "value", null, false, schema, false)
                ));
        try {
            client.createAssociation(createRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }

        // Scenario 3: Both creating new subjects
        keySubject = "test3Key";
        valueSubject = "test3Value";
        resourceName = "test3";
        resourceID = "test3-id";

        createRequest = new AssociationCreateRequest(
                resourceName, "lkc1", resourceID, null,
                Arrays.asList(
                        new AssociationCreateInfo(keySubject, "key", null, false, schema, false),
                        new AssociationCreateInfo(valueSubject, "value", null, false, schema, false)
                ));
        try {
            client.createAssociation(createRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }
    }

    @Test
    public void testCreateStrongAndWeakAssociationsForTheSameSubject() {
        // Resources for testing
        Resource resourceFoo = new Resource("foo", "lkc1", "id-foo", "topic");
        Resource resourceBar = new Resource("bar", "lkc1", "id-bar", "topic");
        String fooValueSubject = "fooValue";
        String value = "value";

        // Pre-create subject
        String schemaString =
                "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\"," +
                        "\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}";
        AvroSchema schemaInfo = new AvroSchema(schemaString);
        try {
            client.register(fooValueSubject, schemaInfo, true);
        } catch (Exception e) {
            assertNull("Schema registration should succeed.", e);
        }

        // Scenario 1: Same subject, Foo=STRONG, Bar=STRONG -> Bar should fail
        AssociationCreateRequest fooRequest = generateAssociationCreateRequest(
                resourceFoo,
                new AssociationCreateInfo(fooValueSubject, value, LifecyclePolicy.STRONG, false, null, false)
        );
        try {
            client.createAssociation(fooRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }

        List<Association> result = null;
        try {
            result = client.getAssociationsByResourceId(
                    resourceFoo.getResourceId(), null, null, null, 0, -1);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }
        assertEquals(1, result.size());
        assertNotNull(result.get(0).getGuid());
        assertFalse(result.get(0).getGuid().isEmpty());

        AssociationCreateRequest barRequest = generateAssociationCreateRequest(
                resourceBar,
                new AssociationCreateInfo(fooValueSubject, value, LifecyclePolicy.STRONG, false, null, false)
        );

        try {
            client.createAssociation(barRequest);
            fail("Expected exception - cannot create strong association when subject already has strong");
        } catch (Exception e) {
            assertNotNull(e);
        }

        // Scenario 2: Foo=STRONG, Bar=WEAK -> Bar should fail
        barRequest = generateAssociationCreateRequest(
                resourceBar,
                new AssociationCreateInfo(fooValueSubject, value, LifecyclePolicy.WEAK, false, null, false)
        );

        try {
            client.createAssociation(barRequest);
            fail("Expected exception - cannot create weak when subject has strong");
        } catch (Exception e) {
            assertNotNull(e);
        }

        // Scenario 3: Foo=WEAK, Bar=STRONG -> Bar should fail
        // Delete Foo association without cascade
        try {
            client.deleteAssociations(fooRequest.getResourceId(), null, null, false);
        } catch (Exception e) {
            assertNull("AssociationDeleteRequest should succeed.", e);
        }
        List<Association> associations = null;
        try {
            associations = client.getAssociationsByResourceId(
                    resourceFoo.getResourceId(), null, null, null, 0, -1);
        } catch (Exception e) {
            assertNull("getAssociationsByResourceId should succeed.", e);
        }
        assertEquals(0, associations.size());

        // Verify subject still exists
        SchemaMetadata metadata = null;
        try {
            metadata = client.getLatestSchemaMetadata(fooValueSubject);
        } catch (Exception e) {
            assertNull("getLatestSchemaMetadata should succeed.", e);
        }
        assertTrue(metadata.getId() > 0);

        // Create Foo weak association
        fooRequest = generateAssociationCreateRequest(
                resourceFoo,
                new AssociationCreateInfo(fooValueSubject, value, LifecyclePolicy.WEAK, false, null, false)
        );
        try {
            client.createAssociation(fooRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }

        try {
            result = client.getAssociationsByResourceId(
                    resourceFoo.getResourceId(), null, null, null, 0, -1);
        } catch (Exception e) {
            assertNull("getAssociationsByResourceId succeed.", e);
        }
        assertEquals(1, result.size());
        assertNotNull(result.get(0).getGuid());
        assertFalse(result.get(0).getGuid().isEmpty());

        // Try to create Bar strong - should fail
        barRequest = generateAssociationCreateRequest(
                resourceBar,
                new AssociationCreateInfo(fooValueSubject, value, LifecyclePolicy.STRONG, false, null, false)
        );

        try {
            client.createAssociation(barRequest);
            fail("Expected exception - cannot create strong when subject has weak");
        } catch (Exception e) {
            assertNotNull(e);
        }

        // Scenario 4: Foo=WEAK, Bar=WEAK -> Bar should succeed
        barRequest = generateAssociationCreateRequest(
                resourceBar,
                new AssociationCreateInfo(fooValueSubject, value, LifecyclePolicy.WEAK, false, null, false)
        );
        try {
            client.createAssociation(barRequest);
        } catch (Exception e) {
            assertNull("AssociationCreateRequest should succeed.", e);
        }

        try {
            result = client.getAssociationsByResourceId(
                    resourceBar.getResourceId(), null, null, null, 0, -1);
        } catch (Exception e) {
            assertNull("getAssociationsByResourceId should succeed.", e);
        }
        assertEquals(1, result.size());
        assertNotNull(result.get(0).getGuid());
        assertFalse(result.get(0).getGuid().isEmpty());

        // Verify subject has 2 associations
        try {
            result = client.getAssociationsBySubject(fooValueSubject, null, null, null, 0, -1);
        } catch (Exception e) {
            assertNull("getAssociationsBySubject should succeed.", e);
        }
        assertEquals(2, result.size());
    }

    // Helper method
    private AssociationCreateRequest generateAssociationCreateRequest(
            Resource resource, AssociationCreateInfo info) {
        return new AssociationCreateRequest(
                resource.getResourceName(),
                resource.getResourceNamespace(),
                resource.getResourceId(),
                resource.getResourceType(),
                Collections.singletonList(info)
        );
    }

    // Helper class
    private static class Resource {
        private final String resourceName;
        private final String resourceNamespace;
        private final String resourceId;
        private final String resourceType;

        public Resource(String name, String namespace, String id, String type) {
            this.resourceName = name;
            this.resourceNamespace = namespace;
            this.resourceId = id;
            this.resourceType = type;
        }

        public String getResourceName() {
            return resourceName;
        }

        public String getResourceNamespace() {
            return resourceNamespace;
        }

        public String getResourceId() {
            return resourceId;
        }

        public String getResourceType() {
            return resourceType;
        }
    }

    @Test
    public void testGetAssociationsWithFilters() {
        // Setup
        String keySubject = "test1Key";
        String valueSubject = "test1Value";
        String resourceName = "test1";
        String resourceID = "test1-id";

        String schemaString =
                "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\"," +
                        "\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}";

        Schema schema = new Schema(
                null, null, null, null,
                Collections.emptyList(),
                schemaString
        );

        // Create associations: key=STRONG, value=WEAK
        AssociationCreateRequest createRequest = new AssociationCreateRequest(
                resourceName, "lkc1", resourceID, null,
                Arrays.asList(
                        new AssociationCreateInfo(
                                keySubject, "key", LifecyclePolicy.STRONG, false, schema, false),
                        new AssociationCreateInfo(
                                valueSubject, "value", LifecyclePolicy.WEAK, false, schema, false)
                ));

        try {
            client.createAssociation(createRequest);
        } catch (Exception e) {
            assertNotNull("createAssociation should succeed.", e);
        }

        // Query by subject with lifecycle filter "weak" - should return error
        List<Association> associations = null;
        try {
            associations = client.getAssociationsBySubject(
                    keySubject, null, Arrays.asList("key", "value"), "weak", 0, -1);
            fail("getAssociationsBySubject with lower case lifecycle should fail.");
        } catch (Exception e) {
            assertNotNull("getAssociationsBySubject should return error.", e);
        }

        // Query by subject with lifecycle filter "WEAK" - should return 0
        try {
            associations = client.getAssociationsBySubject(
                    keySubject, null, Arrays.asList("key", "value"), "WEAK", 0, -1);
        } catch (Exception e) {
            assertNull("getAssociationsBySubject should succeed.", e);
        }
        assertEquals(0, associations.size());

        // Query by subject with lifecycle filter "STRONG" - should return 1
        try {
            associations = client.getAssociationsBySubject(
                    keySubject, null, Arrays.asList("key", "value"), "STRONG", 0, -1);
        } catch (Exception e) {
            assertNull("getAssociationsBySubject should succeed.", e);
        }
        assertEquals(1, associations.size());

        // Query by resourceID without lifecycle filter - should return 2
        try {
            associations = client.getAssociationsByResourceId(
                    resourceID, null, Arrays.asList("key", "value"), null, 0, -1);
        } catch (Exception e) {
            assertNull("getAssociationsByResourceId should succeed.", e);
        }
        assertEquals(2, associations.size());
    }

    @Test
    public void testDeleteAssociation() {
        String schemaString = "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\"," +
                "\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}";
        Schema schema = new Schema(null, null, null, null, Collections.emptyList(), schemaString);

        // Test 1: Delete with cascade=true
        // Strong association subject should be deleted, weak should remain
        testCascadeDelete(schema);

        // Test 2: Delete with cascade=false
        // Both subjects should remain
        testNoCascadeDelete(schema);

        // Test 3: Delete non-existing association
        // Should succeed without error
        testDeleteNonExisting();

        // Test 4: Delete frozen association
        // cascade=false should fail, cascade=true should delete strong only
        testDeleteFrozenAndNonCascade(schema);
    }


    private void testCascadeDelete(Schema schema) {
        String keySubject = "test1Key";
        String valueSubject = "test1Value";
        String resourceID = "test1-id";

        // Create associations
        try {
            client.createAssociation(new AssociationCreateRequest(
                    "test1", "lkc1", resourceID, null,
                    Arrays.asList(
                            new AssociationCreateInfo(keySubject, "key", LifecyclePolicy.STRONG, false, schema, false),
                            new AssociationCreateInfo(valueSubject, "value", LifecyclePolicy.WEAK, false, schema, false)
                    )));
        } catch (Exception e) {
            assertNull("createAssociation should succeed.", e);
        }

        // Delete with cascade=true
        try {
            client.deleteAssociations(resourceID, null, null, true);
        } catch (Exception e) {
            assertNull("deleteAssociation should succeed.", e);
        }

        // Key subject (STRONG) should be deleted
        try {
            SchemaMetadata metadata = client.getLatestSchemaMetadata(keySubject);
            fail("Expected exception - key subject should be deleted");
        } catch (Exception e) {
            assertNotNull(e);
            String errorMsg = e.getMessage().toLowerCase();
            assertTrue("Error should contain 'not found'", errorMsg.contains("not found"));
        }

        // Value subject (WEAK) should still exist
        try {
            SchemaMetadata valueMetadata = client.getLatestSchemaMetadata(valueSubject);
            assertTrue("Value subject should still exist", valueMetadata.getId() > 0);
        } catch (Exception e) {
            assertNull("getLatestSchemaMetadata should succeed.", e);
        }

        // Associations should be deleted
        try {
            List<Association> associations = client.getAssociationsByResourceId(
                    resourceID, null, null, null, 0, -1);
            assertTrue("Associations should be empty",
                    associations == null || associations.isEmpty());
        } catch (Exception e) {
            assertNull("getAssociationsByResourceId should succeed.", e);
        }
    }

    private void testNoCascadeDelete(Schema schema) {
        String keySubject = "test2Key";
        String valueSubject = "test2Value";
        String resourceID = "test2-id";

        // Create associations
        try {
            client.createAssociation(new AssociationCreateRequest(
                    "test2", "lkc1", resourceID, null,
                    Arrays.asList(
                            new AssociationCreateInfo(keySubject, "key", LifecyclePolicy.STRONG, false, schema, false),
                            new AssociationCreateInfo(valueSubject, "value", LifecyclePolicy.WEAK, false, schema, false)
                    )));
        } catch (Exception e) {
            assertNull("createAssociation should succeed.", e);
        }

        // Delete with cascade=false
        try {
            client.deleteAssociations(resourceID, null, null, false);
        } catch (Exception e) {
            assertNull("deleteAssociation should succeed.", e);
        }

        // Both subjects should still exist
        try {
            SchemaMetadata keyMetadata = client.getLatestSchemaMetadata(keySubject);
            assertTrue("Key subject should still exist", keyMetadata.getId() > 0);
        } catch (Exception e) {
            assertNull("getLatestSchemaMetadata should succeed.", e);
        }

        try {
            SchemaMetadata valueMetadata = client.getLatestSchemaMetadata(valueSubject);
            assertTrue("Value subject should still exist", valueMetadata.getId() > 0);
        } catch (Exception e) {
            assertNull("getLatestSchemaMetadata should succeed.", e);
        }

        // Associations should be deleted
        try {
            List<Association> associations = client.getAssociationsByResourceId(
                    resourceID, null, null, null, 0, -1);
            assertTrue("Associations should be empty",
                    associations == null || associations.isEmpty());
        } catch (Exception e) {
            assertNull("getAssociationsByResourceId should succeed.", e);
        }
    }

    private void testDeleteNonExisting() {
        // Delete non-existing association should succeed
        try {
            client.deleteAssociations("non-existing-id", null, null, false);
        } catch (Exception e) {
            assertNull("deleteAssociation should succeed.", e);
        }
        // No exception = success
    }

    private void testDeleteFrozenAndNonCascade(Schema schema) {
        String keySubject = "test3Key";
        String valueSubject = "test3Value";
        String resourceID = "test3-id";

        // Create associations with frozen=true for key
        try {
            client.createAssociation(new AssociationCreateRequest(
                    "test3", "lkc1", resourceID, null,
                    Arrays.asList(
                            new AssociationCreateInfo(keySubject, "key", LifecyclePolicy.STRONG, true, schema, false),
                            new AssociationCreateInfo(valueSubject, "value", LifecyclePolicy.WEAK, false, schema, false)
                    )));
        } catch (Exception e) {
            assertNull("createAssociation should succeed.", e);
        }

        // Delete with cascade=false should fail (frozen association)
        try {
            client.deleteAssociations(resourceID, null, null, false);
            fail("Expected exception - cannot delete frozen association");
        } catch (Exception e) {
            assertNotNull(e);
        }

        // Delete with cascade=true should succeed
        // Only STRONG (key) subject gets deleted, WEAK (value) remains
        try {
            client.deleteAssociations(resourceID, null, null, true);
        } catch (Exception e) {
            assertNull("deleteAssociation should succeed.", e);
        }

        // Key subject should not exist
        try {
            client.getAllVersions(keySubject);
            fail("Expected exception - key subject should be deleted");
        } catch (Exception e) {
            assertNotNull(e);
        }

        // Value subject should exist
        try {
            List<Integer> valueVersions = client.getAllVersions(valueSubject);
            assertNotNull("Value subject should exist", valueVersions);
            assertFalse("Value subject should have versions", valueVersions.isEmpty());
        } catch (Exception e) {
            assertNull("getAllVersions should succeed.", e);
        }
    }

}
