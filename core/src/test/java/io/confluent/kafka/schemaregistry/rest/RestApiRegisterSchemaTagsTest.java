/*
 * Copyright 2023 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTags;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class RestApiRegisterSchemaTagsTest extends ClusterTestHarness {

  public final String schemaString = "{" +
      "\"type\":\"record\"," +
      "\"name\":\"myrecord\"," +
      "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]" +
      "}";

  public RestApiRegisterSchemaTagsTest() {
    super(1, true);
  }

  @Test
  public void testRegisterSchemaTagsBasic() throws Exception {
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemaString, 1, subject);

    TagSchemaRequest tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setNewVersion(2);
    tagSchemaRequest.setTagsToAdd(Collections.singletonList(
        new SchemaTags(new SchemaEntity("myrecord", SchemaEntity.EntityType.SR_RECORD),
            Collections.singletonList("TAG1"))));

    String expectedSchema = "{" +
        "\"type\":\"record\"," +
        "\"name\":\"myrecord\"," +
        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]," +
        "\"confluent:tags\":[\"TAG1\"]}";
    RegisterSchemaResponse responses = restApp.restClient
        .registerSchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(expectedSchema, responses.getSchema());
    assertEquals((Integer) 2, responses.getVersion());
    assertEquals(2, responses.getId());
    assertEquals("2", responses.getMetadata().getProperties().get("confluent:version"));
  }

  @Test
  public void testRegisterSchemaTagsWithInvalidSchema() throws Exception {
    // subject doesn't exist
    TagSchemaRequest tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setNewVersion(2);
    tagSchemaRequest.setTagsToAdd(Collections.singletonList(
        new SchemaTags(new SchemaEntity("myrecord", SchemaEntity.EntityType.SR_RECORD),
            Collections.singletonList("TAG1"))));
    try {
      RegisterSchemaResponse responses = restApp.restClient
          .registerSchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, "non-exist", "1");
    } catch (RestClientException e) {
      assertEquals(Errors.SUBJECT_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemaString, 1, subject);

    // version doesn't exist
    try {
      RegisterSchemaResponse responses = restApp.restClient
          .registerSchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "2");
    } catch (RestClientException e) {
      assertEquals(Errors.VERSION_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    // invalid version
    try {
      RegisterSchemaResponse responses = restApp.restClient
          .registerSchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "-1");
    } catch (RestClientException e) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, e.getErrorCode());
    }

    // create tag on existing subject version
    tagSchemaRequest.setNewVersion(1);
    try {
      RegisterSchemaResponse responses = restApp.restClient
          .registerSchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "1");
    } catch (RestClientException e) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, e.getErrorCode());
    }
  }

  @Test
  public void testRegisterSchemaTagsWithInvalidTags() throws Exception {
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemaString, 1, subject);

    // invalid path
    TagSchemaRequest tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setNewVersion(2);
    tagSchemaRequest.setTagsToAdd(Collections.singletonList(
        new SchemaTags(new SchemaEntity("does.not.exist", SchemaEntity.EntityType.SR_FIELD),
            Collections.singletonList("TAG1"))));
    try {
      RegisterSchemaResponse responses = restApp.restClient
          .registerSchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "1");
    } catch (RestClientException e) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, e.getErrorCode());
    }
  }
}
