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

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class MockSchemaRegistryClientTest {
    private SchemaRegistryClient client;
    static final String SIMPLE_STRING_SCHEMA = "{\"type\": \"string\"}";
    static final String SIMPLE_AVRO_SCHEMA = "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\"," +
            "\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}";
    static final String EVOLVED_AVRO_SCHEMA = "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\"," +
          "\"fields\":[{\"type\":\"string\",\"name\":\"id\"}, {\"type\":\"string\",\"name\":\"id2\"}]}";
    static final String TOPIC = "topic";
    static final String KEY = "key";
    static final String VALUE = "value";

    private static String defaulKeySubject = "testKey";
    private static String defaultValueSubject = "testValue";
    private static String defaultResourceNamespace = "lkc1";
    private static String defaultResourceName = "test";
    private static String defaultResourceId = "test-id";

    @Before
    public void setUp() {
        this.client = new MockSchemaRegistryClient();
    }

    private void registerTestAvroSchemaInSchemaRegistry(SchemaRegistryClient client, String subject,
                                                    String schemaStr, boolean normalize) {
        try {
          client.register(subject, new AvroSchema(schemaStr), normalize);
        } catch (Exception e) {
          assertNull("Schema registration should succeed.", e);
        }
    }

    private List<AssociationCreateOrUpdateRequest> buildInvalidCreateRequests() {
      AssociationCreateOrUpdateInfo validKeyAssocInfo1 = new AssociationCreateOrUpdateInfo(
              defaulKeySubject, KEY, null, false, null, false);
      AssociationCreateOrUpdateInfo validValueAssocInfo1 = new AssociationCreateOrUpdateInfo(
              defaultValueSubject, VALUE, null, false, null, false);

      // Invalid requests
      List<AssociationCreateOrUpdateRequest> invalidRequests = new ArrayList<>();

      // No resource name
      invalidRequests.add(new AssociationCreateOrUpdateRequest(null, defaultResourceNamespace,
              defaultResourceId, TOPIC, Arrays.asList(validKeyAssocInfo1, validValueAssocInfo1)));

      // No resource namespace
      invalidRequests.add(new AssociationCreateOrUpdateRequest(defaultResourceName, null,
              defaultResourceId, TOPIC, Arrays.asList(validKeyAssocInfo1, validValueAssocInfo1)));

      // No resource id
      invalidRequests.add(new AssociationCreateOrUpdateRequest(defaultResourceName, defaultResourceNamespace,
              null, TOPIC, Arrays.asList(validKeyAssocInfo1, validValueAssocInfo1)));

      // No associations
      invalidRequests.add(new AssociationCreateOrUpdateRequest(defaultResourceName, defaultResourceNamespace,
              defaultResourceId, TOPIC, null));

      // Duplicate association types
      invalidRequests.add(new AssociationCreateOrUpdateRequest(defaultResourceName, defaultResourceNamespace,
              null, TOPIC, Arrays.asList(validKeyAssocInfo1, validKeyAssocInfo1)));

      // No subject name in AssociationCreateOrUpdateInfo
      AssociationCreateOrUpdateInfo invalidValueAssocInfoNoSubject = new AssociationCreateOrUpdateInfo(
              null, VALUE, null, false, null, false);
      invalidRequests.add(new AssociationCreateOrUpdateRequest(defaultResourceName, defaultResourceNamespace,
              defaultResourceId, TOPIC, Arrays.asList(invalidValueAssocInfoNoSubject)));

      // Unsupported ResourceType
      invalidRequests.add(new AssociationCreateOrUpdateRequest(defaultResourceName, defaultResourceNamespace,
              defaultResourceId, "topic2", Arrays.asList(validKeyAssocInfo1, validValueAssocInfo1)));

      // Unsupported AssociationType
      AssociationCreateOrUpdateInfo invalidValueAssocInfoWrongType = new AssociationCreateOrUpdateInfo(
              defaultValueSubject, "value2", null, false, null, false);
      invalidRequests.add(new AssociationCreateOrUpdateRequest(defaultResourceName, defaultResourceNamespace,
              defaultResourceId, TOPIC, Arrays.asList(invalidValueAssocInfoWrongType)));

      // Weak association with frozen to be true
      AssociationCreateOrUpdateInfo invalidValueAssocInfoWeakFrozen = new AssociationCreateOrUpdateInfo(
              defaultValueSubject, VALUE, LifecyclePolicy.WEAK, true, null, false);
      invalidRequests.add(new AssociationCreateOrUpdateRequest(defaultResourceName, defaultResourceNamespace,
              defaultResourceId, TOPIC, Arrays.asList(invalidValueAssocInfoWeakFrozen)));

      return invalidRequests;
    }

    private static class Resource {
    final String resourceName;
    final String resourceNamespace;
    final String resourceId;
    final String resourceType;

    public Resource(String name, String namespace, String id, String type) {
      this.resourceName = name;
      this.resourceNamespace = namespace;
      this.resourceId = id;
      this.resourceType = type;
    }
  }

    private static class AssociationRequestBuilder {
      private String resourceName;
      private String resourceNamespace;
      private String resourceId;
      private String resourceType;
      private List<AssociationCreateOrUpdateInfo> associations = new ArrayList<>();
      private AssociationCreateOrUpdateInfo keyAssociation;
      private AssociationCreateOrUpdateInfo valueAssociation;

      public AssociationRequestBuilder resource(String resourceName, String resourceNamespace, String resourceId, String resourceType) {
        this.resourceName = resourceName;
        this.resourceNamespace = resourceNamespace;
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        return this;
      }

      public AssociationRequestBuilder defaultResource() {
        this.resourceName = defaultResourceName;
        this.resourceNamespace = defaultResourceNamespace;
        this.resourceId = defaultResourceId;
        this.resourceType = TOPIC;
        return this;
      }

      private void initKeyAssociaiton() {
        keyAssociation = new AssociationCreateOrUpdateInfo(null, KEY,
                null, null, null, false);
      }

      private void initValueAssociaiton() {
        valueAssociation = new AssociationCreateOrUpdateInfo(null, VALUE,
                null, null, null, false);
      }

      private RegisterSchemaRequest getRegisterSchemaRequest(String schemaStr) {
        Schema schema = new Schema(null, null, null, null, null, schemaStr);
        return new RegisterSchemaRequest(schema);
      }

      public AssociationRequestBuilder keySubject (String keySubject) {
        if (keyAssociation == null) {
          initKeyAssociaiton();
        }
        keyAssociation.setSubject(keySubject);
        return this;
      }

      public AssociationRequestBuilder keySchema (String keySchema) {
        if (keyAssociation == null) {
          initKeyAssociaiton();
        }
        keyAssociation.setSchema(getRegisterSchemaRequest(keySchema));
        return this;
      }

      public AssociationRequestBuilder keyLifecycle(LifecyclePolicy keyLifecyclePolicy) {
        if (keyAssociation == null) {
          initKeyAssociaiton();
        }
        keyAssociation.setLifecycle(keyLifecyclePolicy);
        return this;
      }

      public AssociationRequestBuilder valueSubject (String valueSubject) {
        if (valueAssociation == null) {
          initValueAssociaiton();
        }
        valueAssociation.setSubject(valueSubject);
        return this;
      }

      public AssociationRequestBuilder valueSchema (String valueSchema) {
        if (valueAssociation == null) {
          initValueAssociaiton();
        }
        valueAssociation.setSchema(getRegisterSchemaRequest(valueSchema));
        return this;
      }

      public AssociationRequestBuilder valueLifecycle(LifecyclePolicy valueLifecyclePolicy) {
        if (valueAssociation == null) {
          initValueAssociaiton();
        }
        valueAssociation.setLifecycle(valueLifecyclePolicy);
        return this;
      }

      public AssociationRequestBuilder valueFrozen(boolean isFrozen) {
        if (valueAssociation == null) {
          initValueAssociaiton();
        }
        valueAssociation.setFrozen(isFrozen);
        return this;
      }

      public AssociationRequestBuilder association(String subject, String associationType, LifecyclePolicy lifecyclePolicy,
                                                   boolean frozen, String schema, boolean normalize) {
        AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(subject, associationType, lifecyclePolicy,
                frozen, schema == null ? null : getRegisterSchemaRequest(schema), normalize);
        associations.add(info);
        return this;
      }

      public AssociationCreateOrUpdateRequest build() {
        if (keyAssociation != null) {
          associations.add(keyAssociation);
        }
        if (valueAssociation != null) {
          associations.add(valueAssociation);
        }
        return new AssociationCreateOrUpdateRequest(resourceName, resourceNamespace, resourceId, resourceType, associations);
      }
    }

    private interface AssociationCreator {
      AssociationResponse create(AssociationCreateOrUpdateRequest request)
              throws IOException, RestClientException;
    }

    private void testInvalidCreateAssociationRequestHelper(AssociationCreator associationCreator) {
      registerTestAvroSchemaInSchemaRegistry(client, defaulKeySubject, SIMPLE_STRING_SCHEMA, true);
      registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, SIMPLE_STRING_SCHEMA, true);

      List<AssociationCreateOrUpdateRequest> invalidRequests = buildInvalidCreateRequests();

      // Test all invalid requests to createAssociation - they should throw exceptions
      for (AssociationCreateOrUpdateRequest invalidRequest : invalidRequests) {
        try {
          associationCreator.create(invalidRequest);
          fail("Expected exception for invalid request");
        } catch (Exception e) {
          // Expected - validation should fail
          assertNotNull("Error should not be null", e);
        }
      }
    }

    @Test
    public void testInvalidCreateAssociationRequest() {
      // Call createAssociation endpoint.
      testInvalidCreateAssociationRequestHelper(client::createAssociation);
      // Reset test
      setUp();
      // Call createAssociation endpoint.
      testInvalidCreateAssociationRequestHelper(client::createOrUpdateAssociation);
    }

    private void testMinimumValidCreateAssociationRequestHelper(AssociationCreator associationCreator) {
      // Pre-create value subject for testing
      registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, SIMPLE_STRING_SCHEMA, true);
      AssociationCreateOrUpdateRequest createRequest = new AssociationRequestBuilder()
              .resource(defaultResourceName, defaultResourceNamespace, defaultResourceId, null)
              .association(defaultValueSubject, null, null, false, null, false)
              .build();
      AssociationResponse createResponse = null;
      try {
        createResponse = associationCreator.create(createRequest);
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
      assertEquals("Lifecycle should be WEAK",
              LifecyclePolicy.WEAK, association.getLifecycle());
      assertFalse("Frozen should be false", association.isFrozen());
      assertNull("Schema should be null", association.getSchema());
    }

    @Test
    public void testMinimumValidCreateAssociationRequest() {
      // Call createAssociation endpoint.
      testMinimumValidCreateAssociationRequestHelper(client::createAssociation);
      // Reset test
      setUp();
      // Call createAssociation endpoint.
      testMinimumValidCreateAssociationRequestHelper(client::createOrUpdateAssociation);
    }

    private void testCreateOneAssociationHelper(AssociationCreator associationCreator) {
    // Pre-create subjects
    registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, SIMPLE_AVRO_SCHEMA, true);

    // Create a new value association using an existing subject.
    AssociationCreateOrUpdateRequest createRequest = new AssociationRequestBuilder().defaultResource()
            .valueSubject(defaultValueSubject).build();

    try {
      associationCreator.create(createRequest);
    } catch (Exception e) {
      assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
    }

    // Create association request is idempotent. Re-issue the same create request should succeed.
    try {
      associationCreator.create(createRequest);
    } catch (Exception e) {
      assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
    }
    // After the second request, the subject and resource should still have just one association.
    List<Association> associations = null;
    try {
      associations = client.getAssociationsBySubject(defaultValueSubject, null,
              null, null, 0, -1);
    }  catch (Exception e) {
      assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
    }
    assertTrue(associations != null);
    assertTrue(associations.size() == 1);

    // Create a key association using a new subject without schema should fail.
    createRequest = new AssociationRequestBuilder().defaultResource().keySubject(defaulKeySubject).build();
    try {
      associationCreator.create(createRequest);
      fail("Expected exception - new subject without schema");
    } catch (Exception e) {
      assertNotNull("Error should not be null", e);
    }

    // Create a key association using a new subject with a schema should succeed.
    createRequest = new AssociationRequestBuilder().defaultResource().keySubject(defaulKeySubject).
            keySchema(EVOLVED_AVRO_SCHEMA).build();
    try {
      associationCreator.create(createRequest);
    } catch (Exception e) {
      assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
    }
  }

    @Test
    public void testCreateOneAssociation() {
      // Call createAssociation endpoint.
      testCreateOneAssociationHelper(client::createAssociation);
      // Reset test
      setUp();
      // Call createAssociation endpoint.
      testCreateOneAssociationHelper(client::createOrUpdateAssociation);
    }

    @Test
    public void testUpdateOneAssociationViaCreateEndpoint() {
      // Pre-create subjects
      registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, SIMPLE_AVRO_SCHEMA, true);

      // Create a new value association using an existing subject.
      AssociationCreateOrUpdateRequest createRequest = new AssociationRequestBuilder().defaultResource()
              .valueSubject(defaultValueSubject).build();

      try {
        client.createAssociation(createRequest);
      } catch (Exception e) {
        assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
      }

      // Re-issue the same request with different association property should error out.
      createRequest = new AssociationRequestBuilder().defaultResource().valueSubject(defaultValueSubject).
              valueLifecycle(LifecyclePolicy.STRONG).build();

      try {
        client.createAssociation(createRequest);
        fail("Expected exception - existing association gets modified");
      } catch (Exception e) {
        assertNotNull("Create association with a different property should fail.", e);
      }

      // Create an existing value association with updated schema through createAssociation should error out.
      createRequest = new AssociationRequestBuilder().defaultResource().valueSubject(defaultValueSubject).
              valueSchema(EVOLVED_AVRO_SCHEMA).build();
      try {
        client.createAssociation(createRequest);
        fail("Expected exception - existing association gets modified");
      } catch (Exception e) {
        assertNotNull("Create association with a different schema should fail.", e);
      }
    }

    @Test
    public void testUpdateOneAssociationViaUpsertEndpoint() {
      // The upsert endpoint is createOrUpdateAssociation endpoint.
      // Pre-create subjects
      registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, SIMPLE_AVRO_SCHEMA, true);

      // Create a new value association using an existing subject.
      // final state: strong, non-frozen
      AssociationCreateOrUpdateRequest createRequest = new AssociationRequestBuilder().defaultResource()
              .valueSubject(defaultValueSubject).valueLifecycle(LifecyclePolicy.STRONG).build();
      try {
        client.createOrUpdateAssociation(createRequest);
      } catch (Exception e) {
        assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
      }

      // Change strong, not frozen lifecycle to weak, non-frozen lifecycle should succeed.
      // final state: weak, non-frozen
      createRequest = new AssociationRequestBuilder().defaultResource().valueSubject(defaultValueSubject)
                      .valueLifecycle(LifecyclePolicy.WEAK).build();
      try {
        client.createOrUpdateAssociation(createRequest);
      } catch (Exception e) {
        assertNull("Create association with a different property should succeed.", e);
      }

      // Update schema should succeed.
      // final state: weak, non-frozen
      createRequest = new AssociationRequestBuilder().defaultResource().valueSubject(defaultValueSubject)
              .valueSchema(EVOLVED_AVRO_SCHEMA).build();
      try {
        client.createOrUpdateAssociation(createRequest);
      } catch (Exception e) {
        assertNull("Update association with a different schema should succeed.", e);
      }

      // Change weak, non-frozen lifecycle to weak, frozen lifecycle should fail.
      // final state: weak, non-frozen
      createRequest = new AssociationRequestBuilder().defaultResource().valueSubject(defaultValueSubject)
              .valueFrozen(true).build();
      try {
        client.createOrUpdateAssociation(createRequest);
        fail("Weak frozen is not supported");
      } catch (Exception e) {
        assertNotNull("Update association to weak frozen should fail.", e);
      }

      // Change to strong, not frozen should succeed.
      // final state: strong, non-frozen
      createRequest = new AssociationRequestBuilder().defaultResource().valueSubject(defaultValueSubject)
              .valueLifecycle(LifecyclePolicy.STRONG).build();
      try {
        client.createOrUpdateAssociation(createRequest);
      } catch (Exception e) {
        assertNull("Update association back to strong should succeed.", e);
      }

      // Changing the frozen attribute on an existing association is not allowed.
      createRequest = new AssociationRequestBuilder().defaultResource().valueSubject(defaultValueSubject)
              .valueLifecycle(LifecyclePolicy.STRONG).valueFrozen(true).build();
      try {
        client.createOrUpdateAssociation(createRequest);
        fail("Expected exception - changing frozen attribute is not allowed");
      } catch (Exception e) {
        assertNotNull("Changing the frozen attribute should fail.", e);
      }

      // Creating a frozen association on a new subject with a schema should succeed.
      String frozenSubject = "frozenValue";
      String frozenResourceId = "frozen-resource-id";
      createRequest = new AssociationCreateOrUpdateRequest(
              "frozen-resource", defaultResourceNamespace, frozenResourceId, TOPIC,
              Collections.singletonList(new AssociationCreateOrUpdateInfo(
                      frozenSubject, VALUE, LifecyclePolicy.STRONG, true,
                      new RegisterSchemaRequest(new Schema(null, null, null, null, null, SIMPLE_AVRO_SCHEMA)),
                      false)));
      try {
        client.createOrUpdateAssociation(createRequest);
      } catch (Exception e) {
        assertNull("Creating a frozen association with schema on new subject should succeed.", e);
      }

      // Any update to a frozen association should fail.
      AssociationCreateOrUpdateRequest updateRequest = new AssociationCreateOrUpdateRequest(
              "frozen-resource", defaultResourceNamespace, frozenResourceId, TOPIC,
              Collections.singletonList(new AssociationCreateOrUpdateInfo(
                      frozenSubject, VALUE, LifecyclePolicy.STRONG, false, null, false)));
      try {
        client.createOrUpdateAssociation(updateRequest);
        fail("Expected exception - updating a frozen association is not allowed");
      } catch (Exception e) {
        assertNotNull("Updating a frozen association should fail.", e);
      }
    }

    private void testCreateMultipleAssociationsHelper(AssociationCreator associationCreator) {
      // Pre-create subjects
      registerTestAvroSchemaInSchemaRegistry(client, defaulKeySubject, SIMPLE_AVRO_SCHEMA, true);
      registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, SIMPLE_AVRO_SCHEMA, true);

      // Scenario 1: Both associations using existing subjects
      AssociationCreateOrUpdateRequest createRequest = new AssociationRequestBuilder().defaultResource()
              .keySubject(defaulKeySubject).valueSubject(defaultValueSubject).build();
      try {
        associationCreator.create(createRequest);
      } catch (Exception e) {
        assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
      }

      // Scenario 2: One using existing subject, one creating new subject
      // Reset
      client.reset();
      registerTestAvroSchemaInSchemaRegistry(client, defaulKeySubject, SIMPLE_AVRO_SCHEMA, true);
      createRequest = new AssociationRequestBuilder().defaultResource()
              .keySubject(defaulKeySubject).valueSubject(defaultValueSubject).valueSchema(SIMPLE_AVRO_SCHEMA).build();
      try {
        associationCreator.create(createRequest);
      } catch (Exception e) {
        assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
      }

      // Scenario 3: Both creating new subjects
      // Reset
      client.reset();
      createRequest = new AssociationRequestBuilder().defaultResource()
              .keySubject(defaulKeySubject).keySchema(SIMPLE_AVRO_SCHEMA)
              .valueSubject(defaultValueSubject).valueSchema(SIMPLE_AVRO_SCHEMA).build();
      try {
        associationCreator.create(createRequest);
      } catch (Exception e) {
        assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
      }
    }

    @Test
    public void testCreateMultipleAssociations() {
      // Call createAssociation endpoint.
      testCreateMultipleAssociationsHelper(client::createAssociation);
      // Reset test
      setUp();
      // Call createAssociation endpoint.
      testCreateMultipleAssociationsHelper(client::createOrUpdateAssociation);
    }

    private void testCreateStrongAndWeakAssociationsForTheSameSubjectHelper(AssociationCreator associationCreator) {
      Resource resourceFoo = new Resource("foo", defaultResourceNamespace, "id-foo", TOPIC);
      Resource resourceBar = new Resource("bar", defaultResourceNamespace, "id-bar", TOPIC);
      String fooValueSubject = "fooValue";

      registerTestAvroSchemaInSchemaRegistry(client, fooValueSubject, SIMPLE_AVRO_SCHEMA, true);

      // Scenario 1: Same subject, Foo=STRONG, Bar=STRONG -> Bar should fail
      AssociationCreateOrUpdateRequest fooRequest = new AssociationRequestBuilder()
              .resource(resourceFoo.resourceName, resourceFoo.resourceNamespace, resourceFoo.resourceId, resourceFoo.resourceType)
              .valueSubject(fooValueSubject).valueLifecycle(LifecyclePolicy.STRONG).build();
      try {
        associationCreator.create(fooRequest);
      } catch (Exception e) {
        assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
      }

      List<Association> result = null;
      try {
        result = client.getAssociationsByResourceId(
                resourceFoo.resourceId, null, null, null, 0, -1);
      } catch (Exception e) {
        assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
      }
      assertEquals(1, result.size());
      assertNotNull(result.get(0).getGuid());
      assertFalse(result.get(0).getGuid().isEmpty());

      AssociationCreateOrUpdateRequest barRequest = new AssociationRequestBuilder()
              .resource(resourceBar.resourceName, resourceBar.resourceNamespace, resourceBar.resourceId, resourceBar.resourceType)
              .valueSubject(fooValueSubject).valueLifecycle(LifecyclePolicy.STRONG).build();

      try {
        client.createOrUpdateAssociation(barRequest);
        fail("Expected exception - cannot create strong association when subject already has strong");
      } catch (Exception e) {
        assertNotNull(e);
      }

      // Scenario 2: Foo=STRONG, Bar=WEAK -> Bar should fail
      barRequest = new AssociationRequestBuilder()
              .resource(resourceBar.resourceName, resourceBar.resourceNamespace, resourceBar.resourceId, resourceBar.resourceType)
              .valueSubject(fooValueSubject).valueLifecycle(LifecyclePolicy.WEAK).build();
      try {
        client.createOrUpdateAssociation(barRequest);
        fail("Expected exception - cannot create weak when subject has strong");
      } catch (Exception e) {
        assertNotNull(e);
      }

      // Scenario 3: Foo=WEAK, Bar=STRONG -> Bar should fail
      // Reset
      client.reset();
      registerTestAvroSchemaInSchemaRegistry(client, fooValueSubject, SIMPLE_AVRO_SCHEMA, true);
      fooRequest = new AssociationRequestBuilder()
              .resource(resourceFoo.resourceName, resourceFoo.resourceNamespace, resourceFoo.resourceId, resourceFoo.resourceType)
              .valueSubject(fooValueSubject).valueLifecycle(LifecyclePolicy.WEAK).build();

      try {
        associationCreator.create(fooRequest);
      } catch (Exception e) {
        assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
      }

      try {
        result = client.getAssociationsByResourceId(
                resourceFoo.resourceId, null, null, null, 0, -1);
      } catch (Exception e) {
        assertNull("getAssociationsByResourceId succeed.", e);
      }
      assertEquals(1, result.size());
      assertNotNull(result.get(0).getGuid());
      assertFalse(result.get(0).getGuid().isEmpty());

      // Try to create Bar strong - should fail
      barRequest = new AssociationRequestBuilder()
              .resource(resourceBar.resourceName, resourceBar.resourceNamespace, resourceBar.resourceId, resourceBar.resourceType)
              .valueSubject(fooValueSubject).valueLifecycle(LifecyclePolicy.STRONG).build();

      try {
        associationCreator.create(barRequest);
        fail("Expected exception - cannot create strong when subject has weak");
      } catch (Exception e) {
        assertNotNull(e);
      }

      // Scenario 4: Foo=WEAK, Bar=WEAK -> Bar should succeed
      barRequest = new AssociationRequestBuilder()
              .resource(resourceBar.resourceName, resourceBar.resourceNamespace, resourceBar.resourceId, resourceBar.resourceType)
              .valueSubject(fooValueSubject).valueLifecycle(LifecyclePolicy.WEAK).build();
      try {
        associationCreator.create(barRequest);
        // Try running the same request twice. The second one should do nothing.
        associationCreator.create(barRequest);
      } catch (Exception e) {
        assertNull("AssociationCreateOrUpdateRequest should succeed.", e);
      }

      try {
        result = client.getAssociationsByResourceId(
                resourceBar.resourceId, null, null, null, 0, -1);
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

    @Test
    public void testCreateStrongAndWeakAssociationsForTheSameSubject() {
      testCreateStrongAndWeakAssociationsForTheSameSubjectHelper(client::createAssociation);
      setUp();
      testCreateStrongAndWeakAssociationsForTheSameSubjectHelper(client::createOrUpdateAssociation);
    }

    @Test
    public void testGetAssociationsWithFilters() {
      // Setup
      // Create associations: key=STRONG, value=WEAK
      AssociationCreateOrUpdateRequest createRequest = new AssociationRequestBuilder().defaultResource()
              .keySubject(defaulKeySubject).keySchema(SIMPLE_AVRO_SCHEMA).keyLifecycle(LifecyclePolicy.STRONG)
              .valueSubject(defaultValueSubject).valueSchema(SIMPLE_AVRO_SCHEMA)
              .valueLifecycle(LifecyclePolicy.WEAK).build();
      try {
          client.createOrUpdateAssociation(createRequest);
      } catch (Exception e) {
          assertNotNull("createOrUpdateAssociation should succeed.", e);
      }

      List<Association> associations = null;
      // Query by subject with a non-existent subject - should return empty result instead of nil
      try {
        associations = client.getAssociationsBySubject(
                "nonExistentSubjectName", null, Arrays.asList(KEY, VALUE), null, 0, -1);
      } catch (Exception e) {
        assertNull("getAssociationsBySubject should succeed.", e);
      }
      assertEquals(0, associations.size());

      // Query by subject with lifecycle filter "weak" - should return error
      try {
          associations = client.getAssociationsBySubject(
                  defaulKeySubject, null, Arrays.asList(KEY, VALUE), "weak", 0, -1);
          fail("getAssociationsBySubject with lower case lifecycle should fail.");
      } catch (Exception e) {
          assertNotNull("getAssociationsBySubject should return error.", e);
      }

      // Query by subject with lifecycle filter "WEAK" - should return 0
      try {
          associations = client.getAssociationsBySubject(
                  defaulKeySubject, null, Arrays.asList(KEY, VALUE), "WEAK", 0, -1);
      } catch (Exception e) {
          assertNull("getAssociationsBySubject should succeed.", e);
      }
      assertEquals(0, associations.size());

      // Query by subject with lifecycle filter "STRONG" - should return 1
      try {
          associations = client.getAssociationsBySubject(
                  defaulKeySubject, null, Arrays.asList(KEY, VALUE), "STRONG", 0, -1);
      } catch (Exception e) {
          assertNull("getAssociationsBySubject should succeed.", e);
      }
      assertEquals(1, associations.size());

      // Query by resourceID without lifecycle filter - should return 2
      try {
          associations = client.getAssociationsByResourceId(
                  defaultResourceId, null, Arrays.asList(KEY, VALUE), null, 0, -1);
      } catch (Exception e) {
          assertNull("getAssociationsByResourceId should succeed.", e);
      }
      assertEquals(2, associations.size());

      // Query by resourceName with a wrong resource name - should return 0
      try {
        associations = client.getAssociationsByResourceName(
                "WrongResourceName", null, null, Arrays.asList(KEY, VALUE), null, 0, -1);
      } catch (Exception e) {
        assertNull("getAssociationsByResourceId should succeed.", e);
      }
      assertEquals(0, associations.size());

      // Query by resourceName without lifecycle filter - should return 2
      try {
        associations = client.getAssociationsByResourceName(
                defaultResourceName, null, null, Arrays.asList(KEY, VALUE), null, 0, -1);
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
        String resourceName = "test1";
        String resourceNamespace = "lkc1";

        // Create associations
        try {
            client.createOrUpdateAssociation(new AssociationCreateOrUpdateRequest(
                    resourceName, resourceNamespace, resourceID, null,
                    Arrays.asList(
                            new AssociationCreateOrUpdateInfo(keySubject, "key", LifecyclePolicy.STRONG, false,
                                new RegisterSchemaRequest(schema), false),
                            new AssociationCreateOrUpdateInfo(valueSubject, "value", LifecyclePolicy.WEAK, false,
                                new RegisterSchemaRequest(schema), false)
                    )));
        } catch (Exception e) {
            assertNull("createOrUpdateAssociation should succeed.", e);
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

        // Associations should be deleted.
        // AssociationsById should be empty
        try {
            List<Association> associations = client.getAssociationsByResourceId(
                    resourceID, null, null, null, 0, -1);
            assertTrue("Associations should be empty",
                    associations == null || associations.isEmpty());
        } catch (Exception e) {
            assertNull("getAssociationsByResourceId should succeed.", e);
        }
        // AssociationsByName should be empty
        try {
          List<Association> associations = client.getAssociationsByResourceName(
                  resourceName, resourceNamespace, null, null, null, 0, -1);
          assertTrue("Associations should be empty",
                  associations == null || associations.isEmpty());
        } catch (Exception e) {
          assertNull("getAssociationsByResourceName should succeed.", e);
        }
    }

    private void testNoCascadeDelete(Schema schema) {
        String keySubject = "test2Key";
        String valueSubject = "test2Value";
        String resourceID = "test2-id";
        String resourceName = "test2";
        String resourceNamespace = "lkc1";

        // Create associations
        try {
            client.createOrUpdateAssociation(new AssociationCreateOrUpdateRequest(
                    resourceName, resourceNamespace, resourceID, null,
                    Arrays.asList(
                            new AssociationCreateOrUpdateInfo(keySubject, "key", LifecyclePolicy.STRONG, false,
                                new RegisterSchemaRequest(schema), false),
                            new AssociationCreateOrUpdateInfo(valueSubject, "value", LifecyclePolicy.WEAK, false,
                                new RegisterSchemaRequest(schema), false)
                    )));
        } catch (Exception e) {
            assertNull("createOrUpdateAssociation should succeed.", e);
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
        // AssociationsById should be empty
        try {
            List<Association> associations = client.getAssociationsByResourceId(
                    resourceID, null, null, null, 0, -1);
            assertTrue("Associations should be empty",
                    associations == null || associations.isEmpty());
        } catch (Exception e) {
            assertNull("getAssociationsByResourceId should succeed.", e);
        }

        // AssociationsByName should be empty
        try {
          List<Association> associations = client.getAssociationsByResourceName(
                  resourceName, resourceNamespace, null, null, null, 0, -1);
          assertTrue("Associations should be empty",
                  associations == null || associations.isEmpty());
        } catch (Exception e) {
          assertNull("getAssociationsByResourceName should succeed.", e);
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
            client.createOrUpdateAssociation(new AssociationCreateOrUpdateRequest(
                    "test3", "lkc1", resourceID, null,
                    Arrays.asList(
                            new AssociationCreateOrUpdateInfo(keySubject, "key", LifecyclePolicy.STRONG, true,
                                new RegisterSchemaRequest(schema), false),
                            new AssociationCreateOrUpdateInfo(valueSubject, "value", LifecyclePolicy.WEAK, false,
                                new RegisterSchemaRequest(schema), false)
                    )));
        } catch (Exception e) {
            assertNull("createOrUpdateAssociation should succeed.", e);
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

    @Test
    public void testSchemaWithRuleSetGetsDifferentId() throws IOException, RestClientException {
        String subject = "test-ruleset-subject";
        AvroSchema schemaWithoutRuleSet = new AvroSchema(SIMPLE_AVRO_SCHEMA);

        int idWithoutRuleSet = client.register(subject, schemaWithoutRuleSet);

        Rule rule = new Rule("my-rule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
                "JSONATA", null, null, "true", null, null, false);
        RuleSet ruleSet = new RuleSet(null, Collections.singletonList(rule), null, null);
        AvroSchema schemaWithRuleSet = schemaWithoutRuleSet.copy(null, ruleSet);

        int idWithRuleSet = client.register(subject, schemaWithRuleSet);

        assertNotEquals("Schema with ruleSet should have a different ID than schema without ruleSet",
                idWithoutRuleSet, idWithRuleSet);
    }

    @Test
    public void testClientForScopeWithMultipleProviders() {
        // Clean up any existing scopes first
        MockSchemaRegistry.dropScope("scope");

        // Test 1: Get client with AvroSchemaProvider
        AvroSchemaProvider avroProvider = new AvroSchemaProvider();
        SchemaRegistryClient client1 = MockSchemaRegistry
                .getClientForScope("scope", Collections.singletonList(avroProvider));
        assertNotNull("Client1 should not be null", client1);
        assertTrue("Client1 should be a MockSchemaRegistryClient",
                client1 instanceof MockSchemaRegistryClient);

        // Verify that client1 has AvroSchemaProvider
        MockSchemaRegistryClient mockClient1 = (MockSchemaRegistryClient) client1;
        assertTrue("Client1 should have Avro provider",
                mockClient1.getProviders().containsKey("AVRO"));

        // Test 2: Get client with DummySchemaProvider
        DummySchemaProvider dummyProvider = new DummySchemaProvider();
        SchemaRegistryClient client2 = MockSchemaRegistry
                .getClientForScope("scope", Collections.singletonList(dummyProvider));
        assertNotNull("Client2 should not be null", client2);
        assertTrue("Client2 should be a MockSchemaRegistryClient",
                client2 instanceof MockSchemaRegistryClient);

        // Verify that client2 has DummySchemaProvider
        MockSchemaRegistryClient mockClient2 = (MockSchemaRegistryClient) client2;
        assertTrue("Client1 should have Avro provider",
                mockClient2.getProviders().containsKey("AVRO"));
        assertTrue("Client2 should have Dummy provider",
                mockClient2.getProviders().containsKey("DUMMY"));

        // Verify that the two clients are the same instance
        assertEquals("Clients should be the same instance", client1, client2);

        // Clean up
        MockSchemaRegistry.dropScope("scope");
    }

    /**
     * A dummy schema provider for testing purposes only.
     */
    private static class DummySchemaProvider extends AbstractSchemaProvider {
        @Override
        public String schemaType() {
            return "DUMMY";
        }

        @Override
        public ParsedSchema parseSchemaOrElseThrow(
            Schema schema, boolean isNew, boolean normalize) {
            return new DummySchema(schema);
        }
    }

    /**
     * A simple dummy schema implementation for testing.
     */
    private static class DummySchema implements ParsedSchema {
        private final Schema schema;
        private final Metadata metadata;
        private final RuleSet ruleSet;
        private final Integer version;

        public DummySchema(Schema schema) {
            this(schema, null, null, null);
        }

        public DummySchema(Schema schema, Metadata metadata, RuleSet ruleSet, Integer version) {
            this.schema = schema;
            this.metadata = metadata;
            this.ruleSet = ruleSet;
            this.version = version;
        }

        @Override
        public String schemaType() {
            return "DUMMY";
        }

        @Override
        public String name() {
            return "DummySchema";
        }

        @Override
        public String canonicalString() {
            return schema.getSchema() != null ? schema.getSchema() : "{}";
        }

        @Override
        public Integer version() {
            return version;
        }

        @Override
        public List<SchemaReference> references() {
            return Collections.emptyList();
        }

        @Override
        public Metadata metadata() {
            return metadata;
        }

        @Override
        public RuleSet ruleSet() {
            return ruleSet;
        }

        @Override
        public ParsedSchema copy() {
            return new DummySchema(schema, metadata, ruleSet, version);
        }

        @Override
        public ParsedSchema copy(Integer version) {
            return new DummySchema(schema, metadata, ruleSet, version);
        }

        @Override
        public ParsedSchema copy(Metadata metadata, RuleSet ruleSet) {
            return new DummySchema(schema, metadata, ruleSet, version);
        }

        @Override
        public ParsedSchema copy(
                Map<SchemaEntity, Set<String>> tagsToAdd,
                Map<SchemaEntity, Set<String>> tagsToRemove) {
            return this;
        }

        @Override
        public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
            return Collections.emptyList();
        }

        @Override
        public Object rawSchema() {
            return schema.getSchema();
        }
    }

}
