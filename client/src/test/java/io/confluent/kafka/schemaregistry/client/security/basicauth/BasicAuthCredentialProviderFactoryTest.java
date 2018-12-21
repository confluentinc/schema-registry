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

package io.confluent.kafka.schemaregistry.client.security.basicauth;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.security.auth.login.Configuration;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class BasicAuthCredentialProviderFactoryTest {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { SchemaRegistryClientConfig.USER_INFO_CONFIG },
        { SchemaRegistryClientConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG }
    });
  }

  @Parameterized.Parameter
  public String userInfoConfigName;

  private Map<String, String> CONFIG_MAP = new HashMap<>();

  @Before
  public void setup() throws IOException {
    CONFIG_MAP.put(userInfoConfigName, "user:password");
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
    assertInstance(BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider("URL", CONFIG_MAP), UrlBasicAuthCredentialProvider.class);
    assertInstance(BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider("USER_INFO", CONFIG_MAP), UserInfoCredentialProvider.class);
    assertInstance(BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider("SASL_INHERIT", CONFIG_MAP), SaslBasicAuthCredentialProvider.class);
  }

  @Test
  public void testUnknownProvider() {
    Assert.assertNull(BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider("UNKNOWN", CONFIG_MAP));
  }

  public void assertInstance(BasicAuthCredentialProvider instance,
                             Class<? extends BasicAuthCredentialProvider> klass) {
    Assert.assertNotNull(instance);
    Assert.assertEquals(klass, instance.getClass());
  }
}
