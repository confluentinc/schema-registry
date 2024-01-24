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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTags;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.RuleSetHandler;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
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

  @Before
  public void setUp() throws Exception {
    super.setUp();
    ((KafkaSchemaRegistry) restApp.schemaRegistry()).setRuleSetHandler(new RuleSetHandler() {
      public void handle(String subject, ConfigUpdateRequest request) {
      }

      public void handle(String subject, boolean normalize, RegisterSchemaRequest request) {
      }

      public void handle(String subject, TagSchemaRequest request) {
      }

      public io.confluent.kafka.schemaregistry.storage.RuleSet transform(RuleSet ruleSet) {
        return ruleSet != null
            ? new io.confluent.kafka.schemaregistry.storage.RuleSet(ruleSet)
            : null;
      }
    });
  }

  @Test
  public void testRegisterSchemaTagsBasic() throws Exception {
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemaString, 1, subject);

    TagSchemaRequest tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setNewVersion(2);
    tagSchemaRequest.setTagsToAdd(Collections.singletonList(
        new SchemaTags(new SchemaEntity("myrecord", SchemaEntity.EntityType.SR_RECORD),
            Arrays.asList("TAG1", "TAG2"))));

    String expectedSchema = "{" +
        "\"type\":\"record\"," +
        "\"name\":\"myrecord\"," +
        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]," +
        "\"confluent:tags\":[\"TAG1\",\"TAG2\"]}";
    RegisterSchemaResponse responses = restApp.restClient
        .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(2, responses.getId());

    Schema result = restApp.restClient.getLatestVersion(subject);
    assertEquals(expectedSchema, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setNewVersion(3);
    tagSchemaRequest.setTagsToRemove(Collections.singletonList(
        new SchemaTags(new SchemaEntity("myrecord", SchemaEntity.EntityType.SR_RECORD),
            Arrays.asList("TAG2"))));

    expectedSchema = "{" +
        "\"type\":\"record\"," +
        "\"name\":\"myrecord\"," +
        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]," +
        "\"confluent:tags\":[\"TAG1\"]}";
    responses = restApp.restClient
        .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(3, responses.getId());

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(expectedSchema, result.getSchema());
    assertEquals((Integer) 3, result.getVersion());
    assertEquals("3", result.getMetadata().getProperties().get("confluent:version"));
  }

  @Test
  public void testRegisterSchemaWithoutNewVersionInput() throws Exception {
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemaString, 1, subject);

    TagSchemaRequest tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setTagsToAdd(Collections.singletonList(
        new SchemaTags(new SchemaEntity("myrecord", SchemaEntity.EntityType.SR_RECORD),
            Arrays.asList("TAG1", "TAG2"))));

    String expectedSchema = "{" +
        "\"type\":\"record\"," +
        "\"name\":\"myrecord\"," +
        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]," +
        "\"confluent:tags\":[\"TAG1\",\"TAG2\"]}";
    RegisterSchemaResponse responses = restApp.restClient
        .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(2, responses.getId());

    Schema result = restApp.restClient.getLatestVersion(subject);
    assertEquals(expectedSchema, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));

    tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setTagsToRemove(Collections.singletonList(
        new SchemaTags(new SchemaEntity("myrecord", SchemaEntity.EntityType.SR_RECORD),
            Arrays.asList("TAG2"))));

    expectedSchema = "{" +
        "\"type\":\"record\"," +
        "\"name\":\"myrecord\"," +
        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]," +
        "\"confluent:tags\":[\"TAG1\"]}";
    responses = restApp.restClient
        .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(3, responses.getId());

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals(expectedSchema, result.getSchema());
    assertEquals((Integer) 3, result.getVersion());
    assertEquals("3", result.getMetadata().getProperties().get("confluent:version"));
  }

  @Test
  public void testRegisterSchemaTagsInDiffContext() throws Exception {
    String subject = ":.ctx:testSubject";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemaString, 1, subject);

    TagSchemaRequest tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setNewVersion(2);
    tagSchemaRequest.setTagsToAdd(Collections.singletonList(
        new SchemaTags(new SchemaEntity("myrecord", SchemaEntity.EntityType.SR_RECORD),
            Arrays.asList("TAG1", "TAG2"))));

    String expectedSchema = "{" +
        "\"type\":\"record\"," +
        "\"name\":\"myrecord\"," +
        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]," +
        "\"confluent:tags\":[\"TAG1\",\"TAG2\"]}";
    RegisterSchemaResponse responses = restApp.restClient
        .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(2, responses.getId());

    Schema result = restApp.restClient.getLatestVersion(subject);
    assertEquals(expectedSchema, result.getSchema());
    assertEquals((Integer) 2, result.getVersion());
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));
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
          .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, "non-exist", "1");
    } catch (RestClientException e) {
      assertEquals(Errors.SUBJECT_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemaString, 1, subject);

    // version doesn't exist
    try {
      RegisterSchemaResponse responses = restApp.restClient
          .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "2");
    } catch (RestClientException e) {
      assertEquals(Errors.VERSION_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    // invalid version
    try {
      RegisterSchemaResponse responses = restApp.restClient
          .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "-1");
    } catch (RestClientException e) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, e.getErrorCode());
    }

    // create tag on existing subject version
    tagSchemaRequest.setNewVersion(1);
    try {
      RegisterSchemaResponse responses = restApp.restClient
          .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "1");
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
          .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "1");
    } catch (RestClientException e) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, e.getErrorCode());
    }
  }

  @Test
  public void testRegisterSchemaTagsIncrementalRuleSet() throws Exception {
    String subject = "test";
    TestUtils.registerAndVerifySchema(restApp.restClient, schemaString, 1, subject);

    TagSchemaRequest tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setNewVersion(2);
    Rule migrationRule = new Rule("myMigrationRule", null, null, RuleMode.UPGRADE,
        "fooType", ImmutableSortedSet.of("PII"), null, null, null, "NONE", false);
    Rule migrationRule2 = new Rule("myMigrationRule2", null, null, RuleMode.UPGRADE,
        "fooType", ImmutableSortedSet.of("PII"), null, null, null, "NONE", false);
    Rule domainRule = new Rule("myRule", null, null, null,
        "fooType", ImmutableSortedSet.of("PII"), null, null, null, "NONE", false);
    Rule domainRule2 = new Rule("myRule2", null, null, null,
        "fooType", ImmutableSortedSet.of("PII"), null, null, null, "NONE", false);
    Rule domainRule3 = new Rule("myRule3", null, null, null,
        "fooType", ImmutableSortedSet.of("PII"), null, null, null, "NONE", false);
    RuleSet ruleSet = new RuleSet(ImmutableList.of(migrationRule, migrationRule2),
        ImmutableList.of(domainRule, domainRule2, domainRule3));
    tagSchemaRequest.setRulesToMerge(ruleSet);
    tagSchemaRequest.setRulesToRemove(ImmutableList.of("myRule4"));

    RegisterSchemaResponse responses = restApp.restClient
        .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(2, responses.getId());
    assertEquals(ruleSet, responses.getRuleSet());

    Schema result = restApp.restClient.getLatestVersion(subject);
    assertEquals("2", result.getMetadata().getProperties().get("confluent:version"));
    assertEquals(ruleSet, responses.getRuleSet());

    tagSchemaRequest = new TagSchemaRequest();
    tagSchemaRequest.setNewVersion(3);
    Rule migrationRule3 = new Rule("myMigrationRule3", null, null, RuleMode.UPGRADE,
        "fooType", ImmutableSortedSet.of("PII2"), null, null, null, "NONE", false);
    Rule domainRule5 = new Rule("myRule5", null, null, null,
        "fooType", ImmutableSortedSet.of("PII2"), null, null, null, "NONE", false);
    Rule domainRule4 = new Rule("myRule4", null, null, null,
        "fooType", ImmutableSortedSet.of("PII2"), null, null, null, "NONE", false);
    domainRule2 = new Rule("myRule2", null, null, null,
        "fooType", ImmutableSortedSet.of("PII2"), null, null, null, "NONE", false);
    ruleSet = new RuleSet(ImmutableList.of(migrationRule3),
        ImmutableList.of(domainRule5, domainRule4, domainRule2));
    tagSchemaRequest.setRulesToMerge(ruleSet);
    tagSchemaRequest.setRulesToRemove(ImmutableList.of("myRule", "myMigrationRule2"));

    RuleSet expectedRuleSet = new RuleSet(ImmutableList.of(migrationRule, migrationRule3),
        ImmutableList.of(domainRule3, domainRule5, domainRule4, domainRule2));
    responses = restApp.restClient
        .modifySchemaTags(RestService.DEFAULT_REQUEST_PROPERTIES, tagSchemaRequest, subject, "latest");
    assertEquals(3, responses.getId());
    assertEquals(expectedRuleSet, responses.getRuleSet());

    result = restApp.restClient.getLatestVersion(subject);
    assertEquals((Integer) 3, result.getVersion());
    assertEquals("3", result.getMetadata().getProperties().get("confluent:version"));
    assertEquals(expectedRuleSet, responses.getRuleSet());
  }
}
