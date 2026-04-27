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

package io.confluent.kafka.schemaregistry.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Shared integration-test shape for schema.reject.empty.subject=true. Assumes the subclass
 * has configured the registry with that flag set; the subject used for the "register empty"
 * case is the context-prefix workaround {@code :.:} because Jetty rejects literal empty
 * path segments.
 */
@Tag("IntegrationTest")
public abstract class RestApiRejectEmptySubjectTest {

  protected static final String EMPTY_SUBJECT_VIA_CONTEXT = ":.:";
  protected static final String SCHEMA_STRING = AvroUtils.parseSchema(
      "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}"
  ).canonicalString();

  protected RestApp restApp;

  public void setRestApp(RestApp restApp) {
    this.restApp = restApp;
  }

  @Test
  public void testEmptySubjectRejectedWhenFlagEnabled() {
    RestClientException ex = assertThrows(RestClientException.class,
        () -> restApp.restClient.registerSchema(SCHEMA_STRING, EMPTY_SUBJECT_VIA_CONTEXT));
    assertEquals(Errors.INVALID_SUBJECT_ERROR_CODE, ex.getErrorCode());
  }

  @Test
  public void testNonEmptySubjectStillAllowedWhenFlagEnabled() throws Exception {
    int id = restApp.restClient.registerSchema(SCHEMA_STRING, "normal-subject");
    assertTrue(id > 0, "non-empty subject registration should still succeed");
  }
}
