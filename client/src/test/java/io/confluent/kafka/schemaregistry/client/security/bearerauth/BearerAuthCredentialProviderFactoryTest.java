/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.security.bearerauth;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.login.Configuration;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BearerAuthCredentialProviderFactoryTest {
  private Map<String, String> CONFIG_MAP = new HashMap<>();

  @Before
  public void setup() throws IOException {
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG, "user:password");
    File jaasConfigFile = File.createTempFile("ks-jaas-",".conf");
    jaasConfigFile.deleteOnExit();

    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM,jaasConfigFile.getPath());

    List<String> lines = new ArrayList<>();
    lines.add("KafkaClient { org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"user\""
            +" password=\"password\";};");
    Files.write(jaasConfigFile.toPath(), lines, StandardCharsets.UTF_8);

    Configuration.setConfiguration(null);
  }

  @Test
  public void testSuccess() {
    assertInstance(BearerAuthCredentialProviderFactory.getBearerAuthCredentialProvider(
            "STATIC_TOKEN", CONFIG_MAP), StaticTokenCredentialProvider.class);
  }

  @Test
  public void testUnknownProvider() {
    Assert.assertNull(BearerAuthCredentialProviderFactory.getBearerAuthCredentialProvider(
            "UNKNOWN", CONFIG_MAP));
  }

  public void assertInstance(BearerAuthCredentialProvider instance,
                             Class<? extends BearerAuthCredentialProvider> klass) {
    Assert.assertNotNull(instance);
    Assert.assertEquals(klass, instance.getClass());
  }
}
