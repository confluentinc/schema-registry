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

package io.confluent.dekregistry.web.rest.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.storage.AbstractDekRegistry;
import io.confluent.dekregistry.storage.exceptions.InvalidKeyException;
import io.confluent.dekregistry.web.rest.exceptions.DekRegistryErrors;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.rest.exceptions.RestConstraintViolationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Storage-layer subclasses (e.g. multi-tenant KMS validation) can throw
 * {@link InvalidKeyException} out of {@code createKekOrForward}; verifies that
 * {@link DekRegistryResource#createKek} maps it to a 422 instead of falling through to the
 * generic 500 handler.
 */
public class DekRegistryResourceTest {

  @Test
  public void testCreateKekInvalidKeyExceptionMapsTo422() throws Exception {
    SchemaRegistryConfig config = mock(SchemaRegistryConfig.class);
    when(config.whitelistHeaders()).thenReturn(Collections.emptyList());

    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.config()).thenReturn(config);

    AbstractDekRegistry dekRegistry = mock(AbstractDekRegistry.class);
    when(dekRegistry.createKekOrForward(any(CreateKekRequest.class), any(Map.class)))
        .thenThrow(new InvalidKeyException("cckId is only supported for azure-kms KEKs"));

    DekRegistryResource resource = new DekRegistryResource(schemaRegistry, dekRegistry);

    CreateKekRequest request = new CreateKekRequest();
    request.setName("kek1");
    request.setKmsType("aws-kms");
    request.setKmsKeyId("arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab");
    request.setKmsProps(Collections.singletonMap("cckId", "some-cck-id"));
    request.setShared(true);

    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    HttpHeaders headers = mock(HttpHeaders.class);

    RestConstraintViolationException ex = assertThrows(RestConstraintViolationException.class,
        () -> resource.createKek(asyncResponse, headers, false, request));

    assertEquals(422, ex.getStatus());
    assertEquals(DekRegistryErrors.INVALID_KEY_ERROR_CODE, ex.getErrorCode());
  }
}
