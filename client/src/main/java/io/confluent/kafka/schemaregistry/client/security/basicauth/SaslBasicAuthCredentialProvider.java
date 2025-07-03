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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;


public class SaslBasicAuthCredentialProvider implements BasicAuthCredentialProvider {

  private String userInfo;

  @Override
  public String alias() {
    return "SASL_INHERIT";
  }

  @Override
  public void configure(Map<String, ?> configs) {

    Map<String, Object> updatedConfigs = getConfigsForJaasUtil(configs);
    JaasContext jaasContext = JaasContext.loadClientContext(updatedConfigs);
    List<AppConfigurationEntry> appConfigurationEntries = jaasContext.configurationEntries();
    if (appConfigurationEntries != null && !appConfigurationEntries.isEmpty()) {
      Map<String, ?> options = appConfigurationEntries.get(0).getOptions();
      StringBuilder userInfoBuilder = new StringBuilder();
      if (options.containsKey("username")) {
        userInfoBuilder.append(options.get("username")).append(":").append(options.get("password"));
        userInfo = userInfoBuilder.toString();
      }
    }

    if (userInfo == null || userInfo.isEmpty()) {
      throw new ConfigException("Provided SASL Login module doesn't provide username and password"
                                + " options and can't be inherited");
    }
  }

  //visible for unit test
  Map<String, Object> getConfigsForJaasUtil(Map<String, ?> configs) {
    Map<String, Object> updatedConfigs = new HashMap<>(configs);
    if (updatedConfigs.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
      Object saslJaasConfig = updatedConfigs.get(SaslConfigs.SASL_JAAS_CONFIG);
      if (saslJaasConfig instanceof String) {
        updatedConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, new Password((String) saslJaasConfig));
      } 
    }
    return updatedConfigs;
  }

  @Override
  public String getUserInfo(URL url) {
    return userInfo;
  }
}
