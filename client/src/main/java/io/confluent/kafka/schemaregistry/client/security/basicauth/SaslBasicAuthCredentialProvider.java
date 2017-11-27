/*
 * Copyright 2017 Confluent Inc.
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

import org.apache.kafka.common.security.JaasContext;

import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

public class SaslBasicAuthCredentialProvider implements BasicAuthCredentialProvider {

  private String userInfo;

  @Override
  public void configure(Map<String, ?> configs) {
    JaasContext jaasContext = JaasContext.load(JaasContext.Type.CLIENT, null, configs);
    List<AppConfigurationEntry> appConfigurationEntries = jaasContext.configurationEntries();
    if (appConfigurationEntries != null && !appConfigurationEntries.isEmpty()) {
      Map<String, ?> options = appConfigurationEntries.get(0).getOptions();
      StringBuilder userInfoBuilder = new StringBuilder();
      if (options.containsKey("username")) {
        userInfoBuilder.append(options.get("username")).append(":").append(options.get("password"));
        userInfo = userInfoBuilder.toString();
      }
    }
  }

  @Override
  public String getUserInfo(URL url) {
    return userInfo;
  }
}
