/*
 * Copyright 2017-2019 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.security.basicauth;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.Configuration;

public class SaslBasicAuthCredentialProviderTest {

  @Test
  public void testJaasConfigFile() throws IOException {
    File jaasConfigFile = File.createTempFile("ks-jaas-",".conf");
    jaasConfigFile.deleteOnExit();

    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM,jaasConfigFile.getPath());

    List<String> lines = new ArrayList<>();
    lines.add("KafkaClient { org.apache.kafka.common.security.plain.PlainLoginModule required "
              + "username=\"user\""
              +" password=\"password\";};");
    Files.write(jaasConfigFile.toPath(), lines, StandardCharsets.UTF_8);

    Configuration.setConfiguration(null);

    SaslBasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
    provider.configure(new HashMap<String, Object>());
    Assert.assertEquals("user:password", provider.getUserInfo(null));
  }

  @Test
  public void testClientJaasConfigWithPlain() throws IOException {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
        new Password("org.apache.kafka.common.security.scram.ScramLoginModule required "
        + "username=\"user\""
        + " password=\"password\";"));

    Configuration.setConfiguration(null);

    SaslBasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertEquals("user:password", provider.getUserInfo(null));
  }

  @Test
  public void testClientJaasConfigWithScram() throws IOException {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
        new Password("org.apache.kafka.common.security.plain.PlainLoginModule required "
                     + "username=\"user\""
                     + " password=\"password\";"));

    Configuration.setConfiguration(null);

    SaslBasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertEquals("user:password", provider.getUserInfo(null));
  }

  @Test(expected = ConfigException.class)
  public void testClientJaasConfigWithKerberos() throws IOException {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
        new Password("com.sun.security.auth.module.Krb5LoginModule required "
                     + "        useKeyTab=true "
                     + "        storeKey=true  "
                     + "        keyTab=\"/etc/security/keytabs/kafka_client.keytab\" "
                     + "        principal=\"kafka-client-1@EXAMPLE.COM\";"));

    Configuration.setConfiguration(null);

    SaslBasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
    provider.configure(clientConfig);
  }

  @Test
  public void testGetUpdatedConfigsForJaasUtilWithString(){

    String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"user\""
                        + " password=\"password\";";
    Map<String, String> originalMap = ImmutableMap.of("test-key", "test-value",
        SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

    SaslBasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
    Map<String, Object> updatedMap = provider.getConfigsForJaasUtil(originalMap);
    Assert.assertNotNull(updatedMap);
    Assert.assertEquals(originalMap.keySet(), updatedMap.keySet());
    Assert.assertTrue(updatedMap.get(SaslConfigs.SASL_JAAS_CONFIG) instanceof Password);
    Assert.assertEquals(new Password(jaasConfig), updatedMap.get(SaslConfigs.SASL_JAAS_CONFIG) );
  }

  @Test
  public void testGetUpdatedConfigsForJaasUtilWithPassword(){

    String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"user\""
                        + " password=\"password\";";
    Map<String, Object> originalMap = ImmutableMap.of("test-key", "test-value",
        SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfig));

    SaslBasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
    Map<String, Object> updatedMap = provider.getConfigsForJaasUtil(originalMap);
    Assert.assertNotNull(updatedMap);
    Assert.assertEquals(originalMap.keySet(), updatedMap.keySet());
    Assert.assertTrue(updatedMap.get(SaslConfigs.SASL_JAAS_CONFIG) instanceof Password);
    Assert.assertEquals(new Password(jaasConfig), updatedMap.get(SaslConfigs.SASL_JAAS_CONFIG) );
  }

}
