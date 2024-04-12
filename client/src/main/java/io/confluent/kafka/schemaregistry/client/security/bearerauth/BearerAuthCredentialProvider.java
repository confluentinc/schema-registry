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

import org.apache.kafka.common.Configurable;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;

public interface BearerAuthCredentialProvider extends Closeable, Configurable {

  /*
  Making alias() default method as custom implementation loaded using
  CustomBearerAuthCredentialProvider via config bearer.auth.custom.provider.class don't need to
  Implement it.
  */
  default String alias() {
    return null;
  }

  /*
   This getBearerToken method should Ideally return a token from a cache. The cache should be
   responsible for refreshing the token.
  */
  String getBearerToken(URL url);

  default String getTargetSchemaRegistry() {
    return null;
  }

  default String getTargetIdentityPoolId() {
    return null;
  }

  @Override
  default void close() throws IOException {}
}
