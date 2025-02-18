/*
 * Copyright 2018 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestIncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidRuleSetException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidSchemaException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.RuleSetHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RestApiCompatibilityTest extends ClusterTestHarness {

  public RestApiCompatibilityTest() {
    super(1, true, CompatibilityLevel.BACKWARD.name);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ((KafkaSchemaRegistry) restApp.schemaRegistry()).setRuleSetHandler(new RuleSetHandler() {
      public void handle(String subject, ConfigUpdateRequest request) {
      }

      public void handle(String subject, boolean normalize, RegisterSchemaRequest request) {
      }

      public io.confluent.kafka.schemaregistry.storage.RuleSet transform(RuleSet ruleSet) {
        return ruleSet != null
            ? new io.confluent.kafka.schemaregistry.storage.RuleSet(ruleSet)
            : null;
      }
    });
  }

  @Test
  public void testCompatibility() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register an incompatible avro
    String incompatibleSchemaString = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();
    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
      assertTrue(
          e.getMessage().contains(READER_FIELD_MISSING_DEFAULT_VALUE.toString()),
          "Verifying error message verbosity"
      );
    }

    // register a non-avro
    String nonAvroSchemaString = "non-avro schema string";
    try {
      restApp.restClient.registerSchema(nonAvroSchemaString, subject);
      fail("Registering a non-avro schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidSchemaException.ERROR_CODE,
          e.getErrorCode(),
          "Should get a bad request status"
      );
    }

    // register a backward compatible avro
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(schemaString2, subject),
        "Registering a compatible schema should succeed"
    );
  }

  @Test
  public void testCompatibilityLevelChangeToNone() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register an incompatible avro
    String incompatibleSchemaString = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();
    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
      fail("Registering an incompatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }

    // change compatibility level to none and try again
    assertEquals(
        CompatibilityLevel.NONE.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.NONE.name, null)
            .getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    try {
      restApp.restClient.registerSchema(incompatibleSchemaString, subject);
    } catch (RestClientException e) {
      fail("Registering an incompatible schema should succeed after bumping down the compatibility "
           + "level to none");
    }
  }

  @Test
  public void testCompatibilityLevelChangeToBackward() throws Exception {
    String subject = "testSubject";

    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );
    // verify that default compatibility level is backward
    assertEquals(
        new Config(CompatibilityLevel.BACKWARD.name),
        restApp.restClient.getConfig(null),
        "Default compatibility level should be backward"
    );
    // change it to forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.FORWARD.name, null)
            .getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    // verify that new compatibility level is forward
    assertEquals(
        new Config(CompatibilityLevel.FORWARD.name),
        restApp.restClient.getConfig(null),
        "New compatibility level should be forward"
    );

    // register schema that is forward compatible with schemaString1
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(schemaString2, subject),
        "Registering should succeed"
    );

    // change compatibility to backward
    assertEquals(
         CompatibilityLevel.BACKWARD.name,
         restApp.restClient.updateCompatibility(CompatibilityLevel.BACKWARD.name,
             null).getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    // verify that new compatibility level is backward
    assertEquals(
        new Config(CompatibilityLevel.BACKWARD.name),
        restApp.restClient.getConfig(null),
        "Updated compatibility level should be backward"
    );

    // register forward compatible schema, which should fail
    String schemaString3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\"}]}").canonicalString();
    try {
      restApp.restClient.registerSchema(schemaString3, subject);
      fail("Registering a forward compatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }

    // now try registering a backward compatible schema (add a field with a default)
    String schemaString4 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema4 = 3;
    assertEquals(
        expectedIdSchema4,
        restApp.restClient.registerSchema(schemaString4, subject),
        "Registering should succeed with backwards compatible schema"
    );
  }

  @Test
  public void testCompatibilityGroup() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setCompatibilityGroup("application.version");
    config.setValidateFields(false);
    // add compatibility group
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Adding compatibility group should succeed"
    );

    Map<String, String> properties = new HashMap<>();
    properties.put("application.version", "1");
    Metadata metadata1 = new Metadata(null, properties, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setMetadata(metadata1);
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId(),
        "Registering should succeed"
    );
    // verify that default compatibility level is backward
    assertEquals(
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be backward"
    );

    // register forward compatible schema, which should fail
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    try {
      restApp.restClient.registerSchema(request2, subject, false);
      fail("Registering a forward compatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }

    // now try registering a forward compatible schema in a different compatibility group
    properties = new HashMap<>();
    properties.put("application.version", "2");
    Metadata metadata2 = new Metadata(null, properties, null);
    request2.setMetadata(metadata2);
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );
  }


  @Test
  public void testAddCompatibilityGroup() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Map<String, String> properties = new HashMap<>();
    Metadata metadata1 = new Metadata(null, properties, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setMetadata(metadata1);
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId(),
        "Registering should succeed"
    );
    // verify that default compatibility level is backward
    assertEquals(
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be backward"
    );

    // register forward compatible schema, which should fail
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    try {
      restApp.restClient.registerSchema(request2, subject, false);
      fail("Registering a forward compatible schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a conflict status"
      );
    }

    // Add compatibility group after first schema already registered
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setCompatibilityGroup("application.version");
    config.setValidateFields(false);
    // add compatibility group
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Adding compatibility group should succeed"
    );

    // now try registering a forward compatible schema in a different compatibility group
    properties = new HashMap<>();
    properties.put("application.version", "2");
    Metadata metadata2 = new Metadata(null, properties, null);
    request2.setMetadata(metadata2);
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );

    ParsedSchema schema3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":[\"null\", \"int\"],\"name\":\"f2\",\"default\":null}]}");

    properties = new HashMap<>();
    Metadata metadata3 = new Metadata(null, properties, null);
    RegisterSchemaRequest request3 = new RegisterSchemaRequest(schema3);
    request3.setMetadata(metadata3);
    int expectedIdSchema3 = 3;
    assertEquals(
        expectedIdSchema3,
        restApp.restClient.registerSchema(request3, subject, false).getId(),
        "Registering should succeed"
    );
  }

  @Test
  public void testConfigMetadata() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Map<String, String> properties = new HashMap<>();
    properties.put("configKey", "configValue");
    Metadata metadata = new Metadata(null, properties, null);
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setDefaultMetadata(metadata);
    config.setValidateFields(false);
    // add config metadata
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Adding config with initial metadata should succeed"
    );

    properties = new HashMap<>();
    properties.put("subjectKey", "subjectValue");
    Metadata metadata1 = new Metadata(null, properties, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setMetadata(metadata1);
    int expectedIdSchema1 = 1;
    RegisterSchemaResponse response = restApp.restClient.registerSchema(request1, subject, false);
    assertEquals(
        expectedIdSchema1,
        response.getId(),
        "Registering should succeed"
    );
    Metadata metadata2 = response.getMetadata();
    assertEquals("configValue", metadata2.getProperties().get("configKey"));
    assertEquals("subjectValue", metadata2.getProperties().get("subjectKey"));

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    // verify that default compatibility level is backward
    assertEquals(
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be backward"
    );

    // change it to forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.FORWARD.name, null)
            .getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    // verify that new compatibility level is forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "New compatibility level should be forward"
    );

    // register forward compatible schema
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    int expectedIdSchema2 = 2;
    response = restApp.restClient.registerSchema(request2, subject, false);
    assertEquals(
        expectedIdSchema2,
        response.getId(),
        "Registering should succeed"
    );
    metadata2 = response.getMetadata();
    assertEquals("configValue", metadata2.getProperties().get("configKey"));
    assertEquals("subjectValue", metadata2.getProperties().get("subjectKey"));

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    metadata2 = schemaString.getMetadata();
    assertEquals("configValue", metadata2.getProperties().get("configKey"));
    assertEquals("subjectValue", metadata2.getProperties().get("subjectKey"));

    // re-register
    response = restApp.restClient.registerSchema(request2, subject, false);
    assertEquals(
        expectedIdSchema2,
        response.getId(),
        "Registering should succeed"
    );
    metadata2 = response.getMetadata();
    assertEquals("configValue", metadata2.getProperties().get("configKey"));
    assertEquals("subjectValue", metadata2.getProperties().get("subjectKey"));

    // register forward compatible schema with specified metadata
    ParsedSchema schema3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\"}]}");
    properties = new HashMap<>();
    properties.put("newSubjectKey", "newSubjectValue");
    Metadata metadata3 = new Metadata(null, properties, null);
    RegisterSchemaRequest request3 = new RegisterSchemaRequest(schema3);
    request3.setMetadata(metadata3);
    int expectedIdSchema3 = 3;
    response = restApp.restClient.registerSchema(request3, subject, false);
    assertEquals(
        expectedIdSchema3,
        response.getId(),
        "Registering should succeed"
    );
    Metadata metadata4 = response.getMetadata();
    assertEquals("configValue", metadata4.getProperties().get("configKey"));
    assertNull(metadata4.getProperties().get("subjectKey"));
    assertEquals("newSubjectValue", metadata4.getProperties().get("newSubjectKey"));

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    schemaString = restApp.restClient.getId(expectedIdSchema3, subject);
    metadata4 = schemaString.getMetadata();
    assertEquals("configValue", metadata4.getProperties().get("configKey"));
    assertNull(metadata4.getProperties().get("subjectKey"));
    assertEquals("newSubjectValue", metadata4.getProperties().get("newSubjectKey"));
  }

  @Test
  public void testConfigRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Rule r1 = new Rule("foo", null, null, RuleMode.UPGRADE, "IGNORE", null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(rules, null);
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setDefaultRuleSet(ruleSet);
    config.setValidateFields(false);
    // add config ruleSet
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Adding config with initial ruleSet should succeed"
    );

    Rule r2 = new Rule("bar", null, null, RuleMode.UPGRADE, "type1", null, null, null, null, null, false);
    rules = Collections.singletonList(r2);
    ruleSet = new RuleSet(rules, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setRuleSet(ruleSet);
    int expectedIdSchema1 = 1;
    RegisterSchemaResponse response = restApp.restClient.registerSchema(request1, subject, false);
    assertEquals(
        expectedIdSchema1,
        response.getId(),
        "Registering should succeed"
    );
    RuleSet ruleSet2 = response.getRuleSet();
    assertEquals("foo", ruleSet2.getMigrationRules().get(0).getName());
    assertEquals("bar", ruleSet2.getMigrationRules().get(1).getName());

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    List<Schema> schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, false, "type1", null, null);
    assertEquals(1, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, true, "type1", null, null);
    assertEquals(1, schemas.size());

    // verify that default compatibility level is backward
    assertEquals(
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "Default compatibility level should be backward"
    );

    // change it to forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.FORWARD.name, null)
            .getCompatibilityLevel(),
        "Changing compatibility level should succeed"
    );

    // verify that new compatibility level is forward
    assertEquals(
        CompatibilityLevel.FORWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel(),
        "New compatibility level should be forward"
    );

    // register forward compatible schema
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    int expectedIdSchema2 = 2;
    response = restApp.restClient.registerSchema(request2, subject, false);
    assertEquals(
        expectedIdSchema2,
        response.getId(),
        "Registering should succeed"
    );
    ruleSet2 = response.getRuleSet();
    assertEquals("foo", ruleSet2.getMigrationRules().get(0).getName());
    assertEquals("bar", ruleSet2.getMigrationRules().get(1).getName());

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    ruleSet2 = schemaString.getRuleSet();
    assertEquals("foo", ruleSet2.getMigrationRules().get(0).getName());
    assertEquals("bar", ruleSet2.getMigrationRules().get(1).getName());

    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, false, "type1", null, null);
    assertEquals(2, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, true, "type1", null, null);
    assertEquals(1, schemas.size());

    // re-register
    response = restApp.restClient.registerSchema(request2, subject, false);
    assertEquals(
        expectedIdSchema2,
        response.getId(),
        "Registering should succeed"
    );
    ruleSet2 = schemaString.getRuleSet();
    assertEquals("foo", ruleSet2.getMigrationRules().get(0).getName());
    assertEquals("bar", ruleSet2.getMigrationRules().get(1).getName());

    // register forward compatible schema with specified metadata
    ParsedSchema schema3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\"}]}");
    Rule r3 = new Rule("zap", null, null, RuleMode.UPGRADE, "type2", null, null, null, null, null, false);
    rules = Collections.singletonList(r3);
    ruleSet = new RuleSet(rules, null);
    RegisterSchemaRequest request3 = new RegisterSchemaRequest(schema3);
    request3.setRuleSet(ruleSet);
    int expectedIdSchema3 = 3;
    response = restApp.restClient.registerSchema(request3, subject, false);
    assertEquals(
        expectedIdSchema3,
        response.getId(),
        "Registering should succeed"
    );
    RuleSet ruleSet3 = response.getRuleSet();
    assertEquals("foo", ruleSet3.getMigrationRules().get(0).getName());
    assertEquals("zap", ruleSet3.getMigrationRules().get(1).getName());

    assertEquals(
        response.getVersion(),
        restApp.restClient.lookUpSubjectVersion(
            new RegisterSchemaRequest(
                new Schema(subject, response)), subject, false, false).getVersion(),
        "Version should match"
    );

    schemaString = restApp.restClient.getId(expectedIdSchema3, subject);
    ruleSet3 = schemaString.getRuleSet();
    assertEquals("foo", ruleSet3.getMigrationRules().get(0).getName());
    assertEquals("zap", ruleSet3.getMigrationRules().get(1).getName());

    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, false, "type1", null, null);
    assertEquals(2, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, false, "type2", null, null);
    assertEquals(1, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, true, "type1", null, null);
    assertEquals(0, schemas.size());
    schemas = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, null, false, true, "type2", null, null);
    assertEquals(1, schemas.size());
  }

  @Test
  public void testSchemaMetadata() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId(),
        "Registering should succeed"
    );

    // register just metadata, schema should be inherited from version 1
    RegisterSchemaRequest request2 = new RegisterSchemaRequest();
    Map<String, String> properties = new HashMap<>();
    properties.put("subjectKey", "subjectValue");
    Metadata metadata = new Metadata(null, properties, null);
    request2.setMetadata(metadata);
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    assertEquals(schema1.canonicalString(), schemaString.getSchemaString());
    assertEquals(metadata, schemaString.getMetadata());
  }

  @Test
  public void testSchemaRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId(),
        "Registering should succeed"
    );

    // register just ruleSet, schema should be inherited from version 1
    RegisterSchemaRequest request2 = new RegisterSchemaRequest();
    Rule r1 = new Rule("foo", null, null, RuleMode.UPGRADE, "IGNORE", null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(rules, null);
    request2.setRuleSet(ruleSet);
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    assertEquals(schema1.canonicalString(), schemaString.getSchemaString());
    assertEquals(ruleSet, schemaString.getRuleSet());
  }

  @Test
  public void testCompareAndSetVersion() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro with wrong version number
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    request2.setVersion(3);
    try {
      restApp.restClient.registerSchema(request2, subject, false);
      fail("Registering a wrong version should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidSchemaException.ERROR_CODE,
          e.getErrorCode(),
          "Should get a bad request status"
      );
    }

    // register a backward compatible avro with right version number
    request2.setVersion(2);
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false).getId(),
        "Registering should succeed"
    );
  }

  @Test
  public void testConfigInvalidRuleSet() throws Exception {
    Rule r1 = new Rule("foo", null, null, RuleMode.READ, "IGNORE", null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    // Add READ rule to migrationRules
    RuleSet ruleSet = new RuleSet(rules, null);
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setDefaultRuleSet(ruleSet);
    // add config ruleSet
    try {
      restApp.restClient.updateConfig(config, null);
      fail("Registering an invalid ruleSet should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidRuleSetException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a bad request status"
      );
    }

    // Add rule with duplicate name
    Rule r2 = new Rule("foo", null, null, RuleMode.READ, "IGNORE", null, null, null, null, null, false);
    rules = ImmutableList.of(r1, r2);
    ruleSet = new RuleSet(null, rules);
    config = new ConfigUpdateRequest();
    config.setDefaultRuleSet(ruleSet);
    // add config ruleSet
    try {
      restApp.restClient.updateConfig(config, null);
      fail("Registering an invalid ruleSet should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidRuleSetException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a bad request status"
      );
    }
  }

  @Test
  public void testRegisterInvalidRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Rule r1 = new Rule("foo", null, null, RuleMode.READ, null, null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(rules, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setRuleSet(ruleSet);
    try {
      restApp.restClient.registerSchema(request1, subject, false);
      fail("Registering an invalid ruleSet should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals(
          RestInvalidRuleSetException.DEFAULT_ERROR_CODE,
          e.getStatus(),
          "Should get a bad request status"
      );
    }
  }

  @Test
  public void testRegisterBadDefaultWithNormalizeConfig() throws Exception {
    String subject = "testSubject";

    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"int\",\"default\":\"foo\",\"name\":"
        + "\"f" + "\"}]}";
    String schema = AvroUtils.parseSchema(schemaString).canonicalString();

    List<String> errors = restApp.restClient.testCompatibility(schema, subject, "latest");
    assertTrue(errors.isEmpty());

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setNormalize(true);
    config.setValidateFields(false);
    // set normalize config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Setting normalize config should succeed"
    );

    try {
      restApp.restClient.testCompatibility(schema, subject, "latest");
      fail("Testing compatibility for schema with invalid default should fail with "
          + Errors.INVALID_SCHEMA_ERROR_CODE
          + " (invalid schema)");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }

    try {
      restApp.restClient.registerSchema(schema, subject);
      fail("Registering schema with invalid default should fail with "
          + Errors.INVALID_SCHEMA_ERROR_CODE
          + " (invalid schema)");
    } catch (RestClientException rce) {
      assertEquals(Errors.INVALID_SCHEMA_ERROR_CODE, rce.getErrorCode());
    }
  }

  @Test
  public void testSubjectAlias() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias"),
        "Setting alias config should succeed"
    );

    Schema schema = restApp.restClient.getVersion("testAlias", 1);
    assertEquals(schemaString1, schema.getSchema());
  }

  @Test
  public void testSubjectAliasWithSlash() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "test/Alias"),
        "Setting alias config should succeed"
    );

    Schema schema = restApp.restClient.getVersion("test/Alias", 1);
    assertEquals(schemaString1, schema.getSchema());
  }

  @Test
  public void testSubjectAliasWithContext() throws Exception {
    RestService restClient1 = new RestService(restApp.restConnect + "/contexts/.mycontext");
    RestService restClient2 = new RestService(restApp.restConnect + "/contexts/.mycontext2");

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restClient1.registerSchema(schemaString1, "testSubject"),
        "Registering should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias(":.mycontext:testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, ":.mycontext2:testAlias"),
        "Setting alias config should succeed"
    );

    Schema schema = restClient2.getVersion("testAlias", 1);
    assertEquals(schemaString1, schema.getSchema());
  }

  @Test
  public void testGlobalAliasNotUsed() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("badSubject");
    config.setValidateFields(false);
    // set global alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, null),
        "Setting alias config should succeed"
    );

    Schema schema = restApp.restClient.getVersion("testSubject", 1);
    assertEquals(schemaString1, schema.getSchema());
  }

  @Test
  public void testGetSchemasWithAliases() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(schemaString2, subject),
        "Registering a compatible schema should succeed"
    );

    subject = "noTestSubject";

    // register unrelated schemas
    String unrelated1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"x1\"}]}").canonicalString();
    int expectedIdUnrelated1 = 3;
    assertEquals(
        expectedIdUnrelated1,
        restApp.restClient.registerSchema(unrelated1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    String unrelated2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"x1\"},"
        + " {\"type\":\"string\",\"name\":\"x2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdUnrelated2 = 4;
    assertEquals(
        expectedIdUnrelated2,
        restApp.restClient.registerSchema(unrelated2, subject),
        "Registering a compatible schema should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias"),
        "Setting alias config should succeed"
    );

    List<Schema> schemas = restApp.restClient.getSchemas("testAlias", true, false);
    assertEquals(0, schemas.size());

    List<ExtendedSchema> schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "testAlias", true, false, false, null, null, null);
    assertEquals(2, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }

    subject = "testAlligator";
    String schemaString3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"a1\"},"
        + " {\"type\":\"string\",\"name\":\"a2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema3 = 5;
    assertEquals(
        expectedIdSchema3,
        restApp.restClient.registerSchema(schemaString3, subject),
        "Registering a schema should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "testAl", true, false, false, null, null, null);
    assertEquals(3, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().endsWith("testAlligator")) {
        assertNull(schema.getAliases());
      } else if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }

    // make sure we don't get repeats with a common subjectPrefix
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "test", true, false, false, null, null, null);
    assertEquals(3, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().endsWith("testAlligator")) {
        assertNull(schema.getAliases());
      } else if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }

    // another alias to same subject
    config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias2"),
        "Setting alias config should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "testAl", true, false, false, null, null, null);
    assertEquals(3, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().endsWith("testAlligator")) {
        assertNull(schema.getAliases());
      } else if (schema.getSubject().equals("testSubject")) {
        assertEquals(2, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
        assertEquals("testAlias2", schema.getAliases().get(1));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }

    // make sure we don't get repeats with a common subjectPrefix
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, "test", true, false, false, null, null, null);
    assertEquals(3, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().endsWith("testAlligator")) {
        assertNull(schema.getAliases());
      } else if (schema.getSubject().equals("testSubject")) {
        assertEquals(2, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
        assertEquals("testAlias2", schema.getAliases().get(1));
      } else {
        fail("Unexpected subject: " + schema.getSubject());
      }
    }
  }

  @Test
  public void testGetSchemasWithAliasesAndContextWildcard() throws Exception {
    String subject = "testSubject";

    // register a valid avro
    String schemaString1 = AvroUtils.parseSchema("{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":\"f1\"}]}").canonicalString();
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(schemaString1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema2 = 2;
    assertEquals(
        expectedIdSchema2,
        restApp.restClient.registerSchema(schemaString2, subject),
        "Registering a compatible schema should succeed"
    );

    subject = "noTestSubject";

    // register unrelated schemas
    String unrelated1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"x1\"}]}").canonicalString();
    int expectedIdUnrelated1 = 3;
    assertEquals(
        expectedIdUnrelated1,
        restApp.restClient.registerSchema(unrelated1, subject),
        "Registering should succeed"
    );

    // register a backward compatible avro
    String unrelated2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"x1\"},"
        + " {\"type\":\"string\",\"name\":\"x2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdUnrelated2 = 4;
    assertEquals(
        expectedIdUnrelated2,
        restApp.restClient.registerSchema(unrelated2, subject),
        "Registering a compatible schema should succeed"
    );

    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias"),
        "Setting alias config should succeed"
    );

    List<Schema> schemas = restApp.restClient.getSchemas("testAlias", true, false);
    assertEquals(0, schemas.size());

    List<ExtendedSchema> schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":*:", true, false, false, null, null, null);
    assertEquals(4, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        assertNull(schema.getAliases());
      }
    }

    subject = "testAlligator";
    String schemaString3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"a1\"},"
        + " {\"type\":\"string\",\"name\":\"a2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema3 = 5;
    assertEquals(
        expectedIdSchema3,
        restApp.restClient.registerSchema(schemaString3, subject),
        "Registering a schema should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":*:", true, false, false, null, null, null);
    assertEquals(5, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
      } else {
        assertNull(schema.getAliases());
      }
    }

    // another alias to same subject
    config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, "testAlias2"),
        "Setting alias config should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":*:", true, false, false, null, null, null);
    assertEquals(5, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(2, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
        assertEquals("testAlias2", schema.getAliases().get(1));
      } else {
        assertNull(schema.getAliases());
      }
    }

    subject = ":.myctx:testSubject";
    String schemaString4 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"b1\"},"
        + " {\"type\":\"string\",\"name\":\"b2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema4 = 1;
    assertEquals(
        expectedIdSchema4,
        restApp.restClient.registerSchema(schemaString4, subject),
        "Registering a schema should succeed"
    );

    // another alias to same subject
    config = new ConfigUpdateRequest();
    config.setAlias("testSubject");
    // set alias config
    assertEquals(
        config,
        restApp.restClient.updateConfig(config, ":.myctx:testAlias3"),
        "Setting alias config should succeed"
    );

    // see if the query picks up the new schema
    schemasWithAliases = restApp.restClient.getSchemas(
        RestService.DEFAULT_REQUEST_PROPERTIES, ":*:", true, false, false, null, null, null);
    assertEquals(6, schemasWithAliases.size());
    for (ExtendedSchema schema : schemasWithAliases) {
      if (schema.getSubject().equals("testSubject")) {
        assertEquals(2, schema.getAliases().size());
        assertEquals("testAlias", schema.getAliases().get(0));
        assertEquals("testAlias2", schema.getAliases().get(1));
      } else if (schema.getSubject().equals(":.myctx:testSubject")) {
        assertEquals(1, schema.getAliases().size());
        assertEquals(":.myctx:testAlias3", schema.getAliases().get(0));
      } else {
        assertNull(schema.getAliases());
      }
    }
  }

  @Test
  public void testRegisterEmptyRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    List<Rule> rules = Collections.emptyList();
    RuleSet ruleSet = new RuleSet(null, rules);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setRuleSet(ruleSet);
    int expectedIdSchema1 = 1;
    assertEquals(
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId());

    request1.setRuleSet(null);
    Schema s = restApp.restClient.lookUpSubjectVersion(request1, subject, false, false);
    assertEquals(expectedIdSchema1, s.getId().intValue());
  }
}
