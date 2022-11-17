package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class SaslOauthCredentialProviderTest {

  @Mock
  CachedOauthTokenRetriever tokenRetriever;

  @InjectMocks
  BearerAuthCredentialProvider oAuthCredentialProvider = new SaslOauthCredentialProvider();

  private String tokenString = "dummy-token";

  private Map<String, Object> clientConfig = new HashMap<>();

  @Before
  public void InitialiaseSaslConfig() {
    clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
        new Password(
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                + "clientId=\"0oa3tq39ol3OLrnQj4x4\" "
                + "scope='test' "
                + "clientSecret='mysecret123' "
                + "extension_logicalCluster='LKC_OF_KAFKA_CLUSTER' "
                + "extension_identityPoolId='SASL_IDENTITY_POOL_ID';")
    );
    clientConfig.put(SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, "lsrc-dummy");
    clientConfig.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
        "https://dev-531534.okta.com/oauth2/default/v1/token");
  }


  @Test
  public void TestGetBearerToken() throws MalformedURLException {
    when(tokenRetriever.getToken()).thenReturn(tokenString);
    Assert.assertEquals(tokenString,
        oAuthCredentialProvider.getBearerToken(new URL("https://dummy.com")));
  }

  @Test
  public void TestkafkaConfigInheritance() {
    Map<String, Object> clientConfig = new HashMap<>(this.clientConfig);
    // expecting no error during configure
    oAuthCredentialProvider.configure(clientConfig);
    Assert.assertEquals("SASL_IDENTITY_POOL_ID", oAuthCredentialProvider.getTargetIdentityPoolId());
  }

  @Test
  public void TestBearerConfigHigherPrecedence() {
    Map<String, Object> clientConfig = new HashMap<>(this.clientConfig);
    // expecting no error during configure
    clientConfig.put(SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, "BEARER_IDENTITY_POOL_ID");
    oAuthCredentialProvider.configure(clientConfig);
    Assert.assertEquals("BEARER_IDENTITY_POOL_ID",
        oAuthCredentialProvider.getTargetIdentityPoolId());
  }
}
