package io.confluent.kafka.schemaregistry.client.security.basicauth;


import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class AbstractBasicAuthCredentialProviderTest {

  @Test
  public void testNullUserInfo() {
    Assert.assertNull(AbstractBasicAuthCredentialProvider.decodeUserInfo(null));
  }

  @Test
  public void testSpecialCharUserInfo() {
    Assert.assertEquals("~!@#$%^&*()💙", AbstractBasicAuthCredentialProvider
        .decodeUserInfo("~%21%40%23%24%25%5E%26%2A%28%29%F0%9F%92%99"));
  }

  @Test
  public void testAsciiCredentials() {
    Assert.assertEquals("user:password", AbstractBasicAuthCredentialProvider
        .decodeUserInfo("user:password"));
  }

  @Test
  public void testGetAuthHeader() throws MalformedURLException {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(SchemaRegistryClientConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG, "%C3%BC%24%C3%ABr:%CF%B1%CE%B1%24swo%7C2d");
    UserInfoCredentialProvider provider = new UserInfoCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertEquals("w7wkw6tyOs+xzrEkc3dvfDJk", provider.getAuthHeader(new URL("http://localhost")));
  }

}
