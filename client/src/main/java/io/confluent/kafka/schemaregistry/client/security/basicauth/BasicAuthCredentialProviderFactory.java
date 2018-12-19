/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.security.basicauth;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class BasicAuthCredentialProviderFactory {

  public static final Map<String, BasicAuthCredentialProvider>
      basicAuthCredentialProviderMap = new HashMap<>();

  static {
    for (BasicAuthCredentialProvider basicAuthCredentialProvider
        : ServiceLoader.load(BasicAuthCredentialProvider.class)) {
      basicAuthCredentialProviderMap.put(
          basicAuthCredentialProvider.alias(),
          basicAuthCredentialProvider);
    }
  }

  public static BasicAuthCredentialProvider getBasicAuthCredentialProvider(
      String basicAuthCredentialSource,
      Map<String, ?> configs) {
    BasicAuthCredentialProvider basicAuthCredentialProvider =
        basicAuthCredentialProviderMap.get(basicAuthCredentialSource);
    if (basicAuthCredentialProvider != null) {
      basicAuthCredentialProvider.configure(configs);
    }
    return basicAuthCredentialProvider;
  }
}
