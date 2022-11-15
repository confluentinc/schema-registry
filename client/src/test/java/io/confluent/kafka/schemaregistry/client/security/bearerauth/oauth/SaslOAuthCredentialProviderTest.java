package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import io.confluent.kafka.schemaregistry.client.security.basicauth.SaslBasicAuthCredentialProvider;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.Configuration;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class SaslOAuthCredentialProviderTest {
  @Mock
  CachedOauthTokenRetriever tokenRetriever;

  @InjectMocks
  OauthCredentialProvider oAuthCredentialProvider = new OauthCredentialProvider();

  private String tokenString = "dummy-token";


  @Test
  public void testkafkaConfigInheritance() throws IOException {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
        new Password("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId=\"0oa3tq39ol3OLrnQj4x7\" scope='test' clientSecret='6cOHtlVhOIg3AtBFXlysH-_XJloGmu1i07llyH6a' extension_logicalCluster='LKC_OF_KAFKA_CLUSTER' "
            + "extension_identityPoolId='IDENTITY_POOL_ID';"));

    Configuration.setConfiguration(null);

    SaslBasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
    provider.configure(clientConfig);

  }

}
