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

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.ContextId;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import java.io.IOException;
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
                new Schema(subject1, keyRequest),
                null
            ),
            new AssociationCreateOrUpdateInfo(
                subject2,
                "value",
                LifecyclePolicy.STRONG,
                false,
                new Schema(subject2, valueRequest),
                null
            )
        )
    );

    AssociationResponse response = restApp.restClient.createAssociation(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, request);
    assertEquals(resourceName, response.getResourceName());
  }
}

