package io.confluent.kafka.schemaregistry.client.security.bearerauth;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;


public class CustomBearerAuthCredentialProviderTest {

  private final String LSRC_ID = "lsrc-dummy";
  private final String POOL_ID = "my-pool-id";

  @Test
  public void testWithStaticTokenProvider() throws MalformedURLException {
    Map<String, String> CONFIG_MAP = new HashMap<>();
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, LSRC_ID);
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, POOL_ID);
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_CUSTOM_PROVIDER_CLASS,
        StaticTokenCredentialProvider.class.getName());
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG, "custom-token");

    BearerAuthCredentialProvider provider = new CustomBearerAuthCredentialProvider();
    provider.configure(CONFIG_MAP);
    Assert.assertEquals("custom-token", provider.getBearerToken(new URL("http://dummy")));
    Assert.assertEquals(LSRC_ID, provider.getTargetSchemaRegistry());
    Assert.assertEquals(POOL_ID, provider.getTargetIdentityPoolId());
  }

  @Test
  public void testWithMyFileTokenProvider() throws IOException {
    String token = "my_custom_file_token";
    Path tempFile = Files.createTempFile("MyTokenFile", "txt");
    Files.write(tempFile, token.getBytes(StandardCharsets.UTF_8));

    Map<String, String> CONFIG_MAP = new HashMap<>();
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, LSRC_ID);
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, POOL_ID);
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL, tempFile.toString());
    CONFIG_MAP.put(SchemaRegistryClientConfig.BEARER_AUTH_CUSTOM_PROVIDER_CLASS,
        "io.confluent.kafka.schemaregistry.client.security.bearerauth.Resources.MyFileTokenProvider");

    BearerAuthCredentialProvider provider = new CustomBearerAuthCredentialProvider();
    provider.configure(CONFIG_MAP);

    Assert.assertEquals(token, provider.getBearerToken(new URL("http://dummy")));
    Assert.assertEquals(LSRC_ID, provider.getTargetSchemaRegistry());
    Assert.assertEquals(POOL_ID, provider.getTargetIdentityPoolId());

  }


}
