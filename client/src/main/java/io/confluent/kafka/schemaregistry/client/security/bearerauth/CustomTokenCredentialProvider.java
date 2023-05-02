package io.confluent.kafka.schemaregistry.client.security.bearerauth;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.net.URL;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.secured.ConfigurationUtils;

public class CustomTokenCredentialProvider implements BearerAuthCredentialProvider {

  BearerAuthCredentialProvider customTokenCredentialProvider;
  private String targetSchemaRegistry;
  private String targetIdentityPoolId;

  @Override
  public String alias() {
    return "CUSTOM_TOKEN_PROVIDER";
  }

  @Override
  public String getBearerToken(URL url) {
    return this.customTokenCredentialProvider.getBearerToken(url);
  }

  @Override
  public String getTargetSchemaRegistry() {
    return this.customTokenCredentialProvider.getTargetSchemaRegistry() != null ?
        this.customTokenCredentialProvider.getTargetSchemaRegistry() : this.targetSchemaRegistry;
  }

  @Override
  public String getTargetIdentityPoolId() {
    return this.customTokenCredentialProvider.getTargetIdentityPoolId() != null ?
        this.customTokenCredentialProvider.getTargetIdentityPoolId() : this.targetIdentityPoolId;
  }

  @Override
  public void configure(Map<String, ?> map) {
    ConfigurationUtils cu = new ConfigurationUtils(map);
    String className = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_CUSTOM_TOKEN_PROVIDER_CLASS);

    try {
      this.customTokenCredentialProvider = (BearerAuthCredentialProvider) Class.forName(className)
          .getDeclaredConstructor()
          .newInstance();
    } catch (Exception e) {
      throw new ConfigException(String.format(
          "Unable to instantiate an object of class %s",
          SchemaRegistryClientConfig.BEARER_AUTH_CUSTOM_TOKEN_PROVIDER_CLASS
      ));
    }
    targetSchemaRegistry = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, false);
    targetIdentityPoolId = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID, false);
    this.customTokenCredentialProvider.configure(map);
  }
}
