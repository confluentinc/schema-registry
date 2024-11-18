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
