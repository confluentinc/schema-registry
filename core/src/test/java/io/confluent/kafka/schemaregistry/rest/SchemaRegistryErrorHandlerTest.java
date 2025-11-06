/*
 * Copyright 2019 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth
    .BasicAuthCredentialProviderFactory;
import org.apache.kafka.common.security.JaasUtils;

import javax.security.auth.login.Configuration;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SchemaRegistryErrorHandlerTest extends ClusterTestHarness {

  Properties props = new Properties();
  private final String schemaString1 = AvroUtils.parseSchema(
      "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"}]}")
      .canonicalString();

  private final String subject = "testSubject";

  public SchemaRegistryErrorHandlerTest() {
    super(1, true, CompatibilityLevel.BACKWARD.name);
  }


  @Test
  public void testIncorrectPassword() throws Exception {
    setupBasicAuthClient("pkey", "incorrect");
    try {
      restApp.restClient.registerSchema(schemaString1, subject);
      fail("Should fail for incorrect password");
    } catch (RestClientException ex) {
      assertEquals(401, ex.getStatus());
      assertEquals("Unauthorized; error code: 401", ex.getMessage());
    }

  }

  private void setupBasicAuthClient(String user, String password) {
    String restUrl = new StringBuilder(restApp.restConnect).
        insert(getSchemaRegistryProtocol().length() + 3, user + ":" + password + "@")
        .toString();
    restApp.restClient = new RestService(restUrl);


    BasicAuthCredentialProvider basicAuthCredentialProvider =
        BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider(
            "URL",
            new HashMap<String, String>());
    restApp.restClient.setBasicAuthCredentialProvider(basicAuthCredentialProvider);
  }


  @Override
  protected Properties getSchemaRegistryProperties() {
    Configuration.setConfiguration(null);

    props.put(SchemaRegistryConfig.AUTHENTICATION_METHOD_CONFIG, SchemaRegistryConfig
        .AUTHENTICATION_METHOD_BASIC);
    props.put(SchemaRegistryConfig.AUTHENTICATION_REALM_CONFIG, "SchemaRegistry");
    props.put(SchemaRegistryConfig.AUTHENTICATION_ROLES_CONFIG, "ccloud");
    try {
      File jaasConfigFile = File.createTempFile("ks-jaas-", ".conf");
      jaasConfigFile.deleteOnExit();

      File userPropsFile =
          new File(SchemaRegistryErrorHandlerTest.class.getResource("/testauth.properties").getFile());
      System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasConfigFile.getPath());
      List<String> lines = new ArrayList<>();
      lines.add("SchemaRegistry { org.eclipse.jetty.security.jaas.spi.PropertyFileLoginModule required "
                + "file=\"" + userPropsFile.getAbsolutePath()
                + "\";};");
      Files.write(jaasConfigFile.toPath(), lines, StandardCharsets.UTF_8);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return props;
  }

  @Override
  protected String getSchemaRegistryProtocol() {
    return "http";
  }
}
