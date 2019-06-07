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

package io.confluent.kafka.schemaregistry.client.security.auth.providers;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

public class BuiltInAuthProviders {

  public enum BasicAuthCredentialProviders {
    URL,
    SASL_INHERIT,
    USER_INFO
  }

  public static Set<String> builtInBasicAuthCredentialProviders() {
    return Utils.mkSet(BasicAuthCredentialProviders.values()).stream()
            .map(BasicAuthCredentialProviders::name).collect(Collectors.toSet());
  }

  public static BasicAuthCredentialProvider loadBasicAuthCredentialProvider(String name) {
    ServiceLoader<BasicAuthCredentialProvider> providers = ServiceLoader.load(
            BasicAuthCredentialProvider.class,
            BuiltInAuthProviders.class.getClassLoader()
    );
    for (BasicAuthCredentialProvider provider : providers) {
      if (provider.alias().equals(name)) {
        return provider;
      }
    }
    throw new ConfigException("BasicAuthCredentialProvider not found for " + name);
  }

  public enum HttpCredentialProviders {
    BASIC, // HTTP Basic credential provider
    BEARER, // HTTP Bearer token credential provider
  }

  public static Set<String> builtInHttpCredentialProviders() {
    return Utils.mkSet(HttpCredentialProviders.values()).stream()
            .map(HttpCredentialProviders::name).collect(Collectors.toSet());
  }

  public static HttpCredentialProvider loadHttpCredentialProviders(String name) {
    ServiceLoader<HttpCredentialProvider> providers = ServiceLoader.load(
            HttpCredentialProvider.class,
            BasicAuthCredentialProvider.class.getClassLoader()
    );
    for (HttpCredentialProvider provider : providers) {
      if (provider.getScheme().equalsIgnoreCase(name)) {
        return provider;
      }
    }
    throw new ConfigException("HttpCredentialProvider not found for " + name);
  }
}
