package io.confluent.kafka.schemaregistry.client.security.basicauth;


import org.junit.Assert;
import org.junit.Test;

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

}
