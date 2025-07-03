/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.security.bearerauth.Resources;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;

/**
 * <code>MyFileTokenProvider</code> is a <code>BearerAuthCredentialProvider</code>
 * Indented only for testing purpose. The <code>MyFileTokenProvider</code> can loaded using
 * <code>CustomTokenCredentialProvider<code/>
 */
public class MyFileTokenProvider implements BearerAuthCredentialProvider {

  private String token;

  @Override
  public String getBearerToken(URL url) {
    // Ideally this might be fetching a cache. And cache should hold the mechanism to refresh.
    return this.token;
  }

  @Override
  public void configure(Map<String, ?> map) {
    Path path = Paths.get(
        (String) map.get(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL));
    try {
      this.token = Files
          .lines(path, StandardCharsets.UTF_8)
          .collect(Collectors.joining(System.lineSeparator()));
    } catch (IOException e) {
      throw new ConfigException(String.format(
          "Not to read file at given location %s", path.toString()
      ));
    }
  }

}
