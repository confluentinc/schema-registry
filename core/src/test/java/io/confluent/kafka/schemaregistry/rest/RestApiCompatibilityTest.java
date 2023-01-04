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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestIncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidSchemaException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RestApiCompatibilityTest extends ClusterTestHarness {

  public RestApiCompatibilityTest() {
    super(1, true, CompatibilityLevel.BACKWARD.name);
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
    assertEquals("Registering should succeed",
            expectedIdSchema1,
            restApp.restClient.registerSchema(schemaString1, subject));

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
      assertEquals("Should get a conflict status",
                   RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
                   e.getStatus());
      assertTrue("Verifying error message verbosity",
              e.getMessage().contains(READER_FIELD_MISSING_DEFAULT_VALUE.toString()));
    }

    // register a non-avro
    String nonAvroSchemaString = "non-avro schema string";
    try {
      restApp.restClient.registerSchema(nonAvroSchemaString, subject);
      fail("Registering a non-avro schema should fail");
    } catch (RestClientException e) {
      // this is expected.
      assertEquals("Should get a bad request status",
                   RestInvalidSchemaException.ERROR_CODE,
                   e.getErrorCode());
    }

    // register a backward compatible avro
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema2 = 2;
    assertEquals("Registering a compatible schema should succeed",
                 expectedIdSchema2,
                 restApp.restClient.registerSchema(schemaString2, subject));
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
    assertEquals("Registering should succeed",
            expectedIdSchema1,
            restApp.restClient.registerSchema(schemaString1, subject));

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
      assertEquals("Should get a conflict status",
                   RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
                   e.getStatus());
    }

    // change compatibility level to none and try again
    assertEquals("Changing compatibility level should succeed",
            CompatibilityLevel.NONE.name,
            restApp.restClient
                    .updateCompatibility(CompatibilityLevel.NONE.name, null)
                    .getCompatibilityLevel());

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
    assertEquals("Registering should succeed",
            expectedIdSchema1,
            restApp.restClient.registerSchema(schemaString1, subject));
    // verify that default compatibility level is backward
    assertEquals("Default compatibility level should be backward",
            CompatibilityLevel.BACKWARD.name,
            restApp.restClient.getConfig(null).getCompatibilityLevel());
    // change it to forward
    assertEquals("Changing compatibility level should succeed",
            CompatibilityLevel.FORWARD.name,
            restApp.restClient
                    .updateCompatibility(CompatibilityLevel.FORWARD.name, null)
                    .getCompatibilityLevel());

    // verify that new compatibility level is forward
    assertEquals("New compatibility level should be forward",
            CompatibilityLevel.FORWARD.name,
            restApp.restClient.getConfig(null).getCompatibilityLevel());

    // register schema that is forward compatible with schemaString1
    String schemaString2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}").canonicalString();
    int expectedIdSchema2 = 2;
    assertEquals("Registering should succeed",
                 expectedIdSchema2,
                 restApp.restClient.registerSchema(schemaString2, subject));

    // change compatibility to backward
    assertEquals("Changing compatibility level should succeed",
            CompatibilityLevel.BACKWARD.name,
            restApp.restClient.updateCompatibility(CompatibilityLevel.BACKWARD.name,
                    null).getCompatibilityLevel());

    // verify that new compatibility level is backward
    assertEquals("Updated compatibility level should be backward",
            CompatibilityLevel.BACKWARD.name,
            restApp.restClient.getConfig(null).getCompatibilityLevel());

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
      assertEquals("Should get a conflict status",
                   RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
                   e.getStatus());
    }

    // now try registering a backward compatible schema (add a field with a default)
    String schemaString4 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"foo\"}]}").canonicalString();
    int expectedIdSchema4 = 3;
    assertEquals("Registering should succeed with backwards compatible schema",
            expectedIdSchema4,
            restApp.restClient.registerSchema(schemaString4, subject));
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
    // add compatibility group
    assertEquals("Adding compatibility group should succeed",
        config,
        restApp.restClient.updateConfig(config, null));

    Map<String, String> properties = new HashMap<>();
    properties.put("application.version", "1");
    Metadata metadata1 = new Metadata(null, properties, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setMetadata(metadata1);
    int expectedIdSchema1 = 1;
    assertEquals("Registering should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false));
    // verify that default compatibility level is backward
    assertEquals("Default compatibility level should be backward",
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel());

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
      assertEquals("Should get a conflict status",
          RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
          e.getStatus());
    }

    // now try registering a forward compatible schema in a different compatibility group
    properties = new HashMap<>();
    properties.put("application.version", "2");
    Metadata metadata2 = new Metadata(null, properties, null);
    request2.setMetadata(metadata2);
    int expectedIdSchema2 = 2;
    assertEquals("Registering should succeed",
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false));
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
    config.setInitialMetadata(metadata);
    // add config metadata
    assertEquals("Adding config with initial metadata should succeed",
        config,
        restApp.restClient.updateConfig(config, null));

    properties = new HashMap<>();
    properties.put("subjectKey", "subjectValue");
    Metadata metadata1 = new Metadata(null, properties, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setMetadata(metadata1);
    int expectedIdSchema1 = 1;
    assertEquals("Registering should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false));
    // verify that default compatibility level is backward
    assertEquals("Default compatibility level should be backward",
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel());

    // change it to forward
    assertEquals("Changing compatibility level should succeed",
        CompatibilityLevel.FORWARD.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.FORWARD.name, null)
            .getCompatibilityLevel());

    // verify that new compatibility level is forward
    assertEquals("New compatibility level should be forward",
        CompatibilityLevel.FORWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel());

    // register forward compatible schema
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    int expectedIdSchema2 = 2;
    assertEquals("Registering should succeed",
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false));

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    Metadata metadata2 = schemaString.getMetadata();
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
    assertEquals("Registering should succeed",
        expectedIdSchema3,
        restApp.restClient.registerSchema(request3, subject, false));

    schemaString = restApp.restClient.getId(expectedIdSchema3, subject);
    metadata3 = schemaString.getMetadata();
    assertEquals("configValue", metadata3.getProperties().get("configKey"));
    assertEquals(null, metadata3.getProperties().get("subjectKey"));
    assertEquals("newSubjectValue", metadata3.getProperties().get("newSubjectKey"));
  }

  @Test
  public void testConfigRuleSet() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Rule r1 = new Rule("foo", null, null, null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(rules, null);
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setInitialRuleSet(ruleSet);
    // add config ruleSet
    assertEquals("Adding config with initial ruleSet should succeed",
        config,
        restApp.restClient.updateConfig(config, null));

    Rule r2 = new Rule("bar", null, null, null, null, null, null, null, false);
    rules = Collections.singletonList(r2);
    ruleSet = new RuleSet(rules, null);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setRuleSet(ruleSet);
    int expectedIdSchema1 = 1;
    assertEquals("Registering should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false));
    // verify that default compatibility level is backward
    assertEquals("Default compatibility level should be backward",
        CompatibilityLevel.BACKWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel());

    // change it to forward
    assertEquals("Changing compatibility level should succeed",
        CompatibilityLevel.FORWARD.name,
        restApp.restClient
            .updateCompatibility(CompatibilityLevel.FORWARD.name, null)
            .getCompatibilityLevel());

    // verify that new compatibility level is forward
    assertEquals("New compatibility level should be forward",
        CompatibilityLevel.FORWARD.name,
        restApp.restClient.getConfig(null).getCompatibilityLevel());

    // register forward compatible schema
    ParsedSchema schema2 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"}]}");
    RegisterSchemaRequest request2 = new RegisterSchemaRequest(schema2);
    int expectedIdSchema2 = 2;
    assertEquals("Registering should succeed",
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false));

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    RuleSet ruleSet2 = schemaString.getRuleSet();
    assertEquals("foo", ruleSet2.getMigrationRules().get(0).getName());
    assertEquals("bar", ruleSet2.getMigrationRules().get(1).getName());

    // register forward compatible schema with specified metadata
    ParsedSchema schema3 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"},"
        + " {\"type\":\"string\",\"name\":\"f2\"},"
        + " {\"type\":\"string\",\"name\":\"f3\"}]}");
    Rule r3 = new Rule("zap", null, null, null, null, null, null, null, false);
    rules = Collections.singletonList(r3);
    ruleSet = new RuleSet(rules, null);
    RegisterSchemaRequest request3 = new RegisterSchemaRequest(schema3);
    request3.setRuleSet(ruleSet);
    int expectedIdSchema3 = 3;
    assertEquals("Registering should succeed",
        expectedIdSchema3,
        restApp.restClient.registerSchema(request3, subject, false));

    schemaString = restApp.restClient.getId(expectedIdSchema3, subject);
    RuleSet ruleSet3 = schemaString.getRuleSet();
    assertEquals("foo", ruleSet3.getMigrationRules().get(0).getName());
    assertEquals("zap", ruleSet3.getMigrationRules().get(1).getName());
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
    assertEquals("Registering should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false));

    // register just metadata, schema should be inherited from version 1
    RegisterSchemaRequest request2 = new RegisterSchemaRequest();
    Map<String, String> properties = new HashMap<>();
    properties.put("subjectKey", "subjectValue");
    Metadata metadata = new Metadata(null, properties, null);
    request2.setMetadata(metadata);
    int expectedIdSchema2 = 2;
    assertEquals("Registering should succeed",
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false));

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
    assertEquals("Registering should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false));

    // register just ruleSet, schema should be inherited from version 1
    RegisterSchemaRequest request2 = new RegisterSchemaRequest();
    Rule r1 = new Rule("foo", null, null, null, null, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(rules, null);
    request2.setRuleSet(ruleSet);
    int expectedIdSchema2 = 2;
    assertEquals("Registering should succeed",
        expectedIdSchema2,
        restApp.restClient.registerSchema(request2, subject, false));

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema2, subject);
    assertEquals(schema1.canonicalString(), schemaString.getSchemaString());
    assertEquals(ruleSet, schemaString.getRuleSet());
  }
}
