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
        Collections.singletonList("key"), null);
    assertEquals(1, associations.size());
    assertEquals(resourceId, associations.get(0).getResourceId());
    assertEquals(resourceName, associations.get(0).getResourceName());
    assertEquals(resourceNamespace, associations.get(0).getResourceNamespace());
    assertEquals(resourceNamespace, associations.get(0).getResourceNamespace());
    assertEquals("key", associations.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.WEAK, associations.get(0).getLifecycle());

    associations = restApp.restClient.getAssociationsBySubject(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject2, "topic",
        Collections.singletonList("value"), null);
    assertEquals(1, associations.size());
    assertEquals(resourceId, associations.get(0).getResourceId());
    assertEquals(resourceName, associations.get(0).getResourceName());
    assertEquals(resourceNamespace, associations.get(0).getResourceNamespace());
    assertEquals("value", associations.get(0).getAssociationType());
    assertEquals(LifecyclePolicy.STRONG, associations.get(0).getLifecycle());

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
        Collections.singletonList("key"), null);
    assertEquals(0, associations.size());

    List<Schema> schemas = restApp.restClient.getSchemas(null, false, false);
    assertEquals(2, schemas.size());

    cascadeDelete = true;
    restApp.restClient.deleteAssociations(RestService.DEFAULT_REQUEST_PROPERTIES,
        resourceId, "topic", Collections.singletonList("value"), cascadeDelete);

    associations = restApp.restClient.getAssociationsBySubject(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject1, "topic",
        Collections.singletonList("value"), null);
    assertEquals(0, associations.size());

    schemas = restApp.restClient.getSchemas(null, false, false);
    assertEquals(1, schemas.size());

  }
}

