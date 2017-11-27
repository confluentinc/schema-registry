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

import java.util.Map;

public class BasicAuthCredentialProviderFactory {

  private static final String URL_BASIC_AUTH = "URL";

  private static final String SASL_INHERIT_BASIC_AUTH = "SASL_INHERIT";

  public static BasicAuthCredentialProvider getBasicAuthCredentialProvider(
      String basicAuthProvider,
      Map<String, ?> configs) {

    BasicAuthCredentialProvider basicAuthCredentialProvider = null;
    if (URL_BASIC_AUTH.equals(basicAuthProvider)) {
      basicAuthCredentialProvider = new UrlBasicAuthCredentialProvider();
    } else if (SASL_INHERIT_BASIC_AUTH.equals(basicAuthProvider)) {
      basicAuthCredentialProvider = new SaslBasicAuthCredentialProvider();
    }
    if (basicAuthCredentialProvider != null) {
      basicAuthCredentialProvider.configure(configs);
    }
    return basicAuthCredentialProvider;
  }
}
