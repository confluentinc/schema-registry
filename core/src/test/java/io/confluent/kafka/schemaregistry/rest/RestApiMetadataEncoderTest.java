/*
 * Copyright 2022 Confluent Inc.
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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class RestApiMetadataEncoderTest extends ClusterTestHarness {

  private static String SCHEMA_STRING = AvroUtils.parseSchema(
      "{\"type\":\"record\","
          + "\"name\":\"myrecord\","
          + "\"fields\":"
          + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
      .canonicalString();

  public RestApiMetadataEncoderTest() {
    super(1, true, CompatibilityLevel.BACKWARD.name);
  }

  @Override
  protected Properties getSchemaRegistryProperties() throws Exception {
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG, "mysecret");
    return props;
  }

  @Test
  public void testRegisterSchemaWithSensitiveMetadata() throws Exception {
    String subject = "testSubject";

    Map<String, String> properties = new HashMap<>();
    properties.put("nonsensitive", "foo");
    properties.put("sensitive", "foo");
    Metadata metadata = new Metadata(null, properties, Collections.singleton("sensitive"));
    Schema schema = new Schema(subject, null, null, null, null, metadata, null, SCHEMA_STRING);
    RegisterSchemaRequest request = new RegisterSchemaRequest(schema);

    int expectedIdSchema1 = 1;
    assertEquals("Registering without id should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request, subject, false).getId());

    List<SubjectVersion> subjectVersions = restApp.restClient.getAllVersionsById(1);
    assertEquals(ImmutableList.of(new SubjectVersion(subject, 1)), subjectVersions);

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema1);
    assertEquals(properties, schemaString.getMetadata().getProperties());
  }
}
