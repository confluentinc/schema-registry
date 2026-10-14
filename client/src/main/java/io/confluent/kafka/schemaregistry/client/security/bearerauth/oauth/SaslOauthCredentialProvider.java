/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;

import io.confluent.kafka.schemaregistry.client.ssl.HostSslSocketFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.oauthbearer.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClientAssertionRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionSupplierFactory;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.CloseableSupplier;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>SaslOauthCredentialProvider</code> is a <code>BearerAuthCredentialProvider</code> that
 * inherits the OAuth configuration of the Kafka client (<code>sasl.jaas.config</code> and the
 * <code>sasl.oauthbearer.*</code> configs). The <code>bearer.auth.*</code> configs, if set, take
 * precedence over the inherited ones.
 *
 * <p>The client authenticates to the token endpoint with a client assertion (KIP-1258) if
 * <code>sasl.oauthbearer.assertion.file</code> or <code>sasl.oauthbearer.assertion.claim.iss</code>
 * is configured, unless <code>bearer.auth.client.secret</code> is set. Otherwise it uses the
 * client secret.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class SaslOauthCredentialProvider implements BearerAuthCredentialProvider {

  public static final String SASL_IDENTITY_POOL_CONFIG = "extension_identityPoolId";
  private static final String SASL_OAUTHBEARER_ASSERTION_PREFIX = "sasl.oauthbearer.assertion.";
  private static final Logger log = LoggerFactory.getLogger(SaslOauthCredentialProvider.class);
  private CachedOauthTokenRetriever tokenRetriever;
  private JwtRetriever jwtRetriever;
  private String targetSchemaRegistry;
  private String targetIdentityPoolId;

  @Override
  public String alias() {
    return "SASL_OAUTHBEARER_INHERIT";
  }

  @Override
  public String getBearerToken(URL url) {
    return tokenRetriever.getToken();
  }

  @Override
  public String getTargetSchemaRegistry() {
    return this.targetSchemaRegistry;
  }

  @Override
  public String getTargetIdentityPoolId() {
    return this.targetIdentityPoolId;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Map<String, Object> updatedConfigs = getConfigsForJaasUtil(configs);
    JaasContext jaasContext = JaasContext.loadClientContext(updatedConfigs);
    List<AppConfigurationEntry> appConfigurationEntries = jaasContext.configurationEntries();
    Map<String, ?> jaasconfig;
    if (Objects.requireNonNull(appConfigurationEntries).size() == 1
        && appConfigurationEntries.get(0) != null) {
      jaasconfig = Collections.unmodifiableMap(appConfigurationEntries.get(0).getOptions());
    } else {
      throw new ConfigException(
          String.format("Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
              appConfigurationEntries.size()));
    }

    ConfigurationUtils cu = new ConfigurationUtils(configs);
    JaasOptionsUtils jou = new JaasOptionsUtils((Map<String, Object>) jaasconfig);

    targetSchemaRegistry = cu.validateString(
        SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, false);

    // if the schema registry oauth configs are set it is given higher preference
    targetIdentityPoolId = cu.get(SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID) != null
        ? cu.validateString(SchemaRegistryClientConfig.BEARER_AUTH_IDENTITY_POOL_ID)
        : jou.validateString(SASL_IDENTITY_POOL_CONFIG, false);

    tokenRetriever = new CachedOauthTokenRetriever();
    jwtRetriever = getTokenRetriever(configs, cu, jou);
    tokenRetriever.configure(jwtRetriever, getTokenValidator(cu, configs),
        getOauthTokenCache(configs));
  }

  @Override
  public void close() throws IOException {
    if (jwtRetriever != null) {
      jwtRetriever.close();
    }
  }


  private OauthTokenCache getOauthTokenCache(Map<String, ?> map) {
    short cacheExpiryBufferSeconds = SchemaRegistryClientConfig
        .getBearerAuthCacheExpiryBufferSeconds(map);
    return new OauthTokenCache(cacheExpiryBufferSeconds);
  }

  private JwtRetriever getTokenRetriever(Map<String, ?> configs, ConfigurationUtils cu,
      JaasOptionsUtils jou) {
    // if the schema registry oauth configs are set they are given higher preference, followed by
    // the Kafka client configs and then the JAAS options
    String clientId = getConfigOrJaas(cu, jou, SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID,
        SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID,
        OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG, false);

    String scope = getConfigOrJaas(cu, jou, SchemaRegistryClientConfig.BEARER_AUTH_SCOPE,
        SaslConfigs.SASL_OAUTHBEARER_SCOPE, OAuthBearerLoginCallbackHandler.SCOPE_CONFIG, false);

    //Keeping following configs needed by HttpAccessTokenRetriever as constants and not exposed to
    //users for modifications
    long retryBackoffMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS;
    long retryBackoffMaxMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
    Integer loginConnectTimeoutMs = null;
    Integer loginReadTimeoutMs = null;

    SSLSocketFactory sslSocketFactory = null;

    URL url = cu.get(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL) != null
        ? cu.validateUrl(SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL)
        : cu.validateUrl(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

    if (jou.shouldCreateSSLSocketFactory(url)) {
      sslSocketFactory = new HostSslSocketFactory(jou.createSSLSocketFactory(), url.getHost());
    }

    // Mirrors the selection in Kafka's ClientCredentialsRequestFormatterFactory: a file-based
    // assertion is used if configured, otherwise one is created locally if an issuer is configured.
    boolean hasAssertionFile =
        cu.validateString(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, false) != null;
    boolean hasAssertionIssuer = cu.containsKey(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS);
    boolean hasSchemaRegistryClientSecret =
        cu.get(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET) != null;

    // An explicitly configured schema registry client secret takes precedence. Otherwise, as in
    // the Kafka client, a configured client assertion is preferred over an inherited client secret.
    if (!hasSchemaRegistryClientSecret && (hasAssertionFile || hasAssertionIssuer)) {
      if (hasAssertionFile && hasAssertionIssuer) {
        log.warn("Both {} and {} are configured. Using file-based assertion; locally-generated "
                + "assertion configs will be ignored.", SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE,
            SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS);
      }
      log.info("Schema Registry client using client assertion authentication with {} assertion",
          hasAssertionFile ? "file-based" : "locally-generated");
      CloseableSupplier<String> assertionSupplier = AssertionSupplierFactory.create(
          new ConfigurationUtils(withAssertionConfigDefaults(configs)), Time.SYSTEM);
      try {
        return new HttpJwtRetriever(
            new ClientAssertionRequestFormatter(clientId, scope, assertionSupplier),
            sslSocketFactory, url.toString(), retryBackoffMs, retryBackoffMaxMs,
            loginConnectTimeoutMs, loginReadTimeoutMs);
      } catch (RuntimeException e) {
        Utils.closeQuietly(assertionSupplier, "assertion supplier");
        throw e;
      }
    }

    String clientSecret = getConfigOrJaas(cu, jou,
        SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET,
        SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET,
        OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG, false);
    if (clientSecret == null) {
      throw new ConfigException(String.format("No OAuth client credentials are configured for the "
              + "Schema Registry client. Configure either a client secret (%s, %s or the %s JAAS "
              + "option) or a client assertion (%s, or %s with %s)",
          SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET,
          SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET,
          OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG,
          SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE,
          SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS,
          SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE));
    }
    if (clientId == null) {
      clientId = jou.validateString(OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG);
    }

    if (hasAssertionFile || hasAssertionIssuer) {
      log.info("{} is configured, so the client assertion configs are ignored and client secret "
          + "authentication is used", SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET);
    } else {
      log.info("Schema Registry client using client secret authentication");
    }

    return new HttpJwtRetriever(clientId, clientSecret, scope, sslSocketFactory,
        url.toString(), retryBackoffMs, retryBackoffMaxMs, loginConnectTimeoutMs,
        loginReadTimeoutMs, false);
  }

  private static String getConfigOrJaas(ConfigurationUtils cu, JaasOptionsUtils jou,
      String schemaRegistryConfig, String kafkaConfig, String jaasOption, boolean required) {
    if (cu.get(schemaRegistryConfig) != null) {
      return validateStringOrPassword(cu, schemaRegistryConfig);
    }
    if (cu.get(kafkaConfig) != null) {
      return validateStringOrPassword(cu, kafkaConfig);
    }
    return jou.validateString(jaasOption, required);
  }

  private static String validateStringOrPassword(ConfigurationUtils cu, String name) {
    return cu.get(name) instanceof Password ? cu.validatePassword(name) : cu.validateString(name);
  }

  // The configs passed to the schema registry client are unparsed, so parse the
  // sasl.oauthbearer.assertion.* configs and apply their Kafka client defaults (e.g.
  // sasl.oauthbearer.assertion.algorithm) that the assertion supplier relies on. Only these configs
  // are parsed so that unrelated configs are neither revalidated nor overwritten by defaults.
  private static Map<String, Object> withAssertionConfigDefaults(Map<String, ?> configs) {
    ConfigDef saslConfigDef = new ConfigDef();
    SaslConfigs.addClientSaslSupport(saslConfigDef);
    Map<String, Object> assertionConfigs = new HashMap<>();
    configs.forEach((name, value) -> {
      if (name.startsWith(SASL_OAUTHBEARER_ASSERTION_PREFIX)) {
        assertionConfigs.put(name, value);
      }
    });
    Map<String, Object> parsedConfigs = new HashMap<>(configs);
    saslConfigDef.parse(assertionConfigs).forEach((name, value) -> {
      if (name.startsWith(SASL_OAUTHBEARER_ASSERTION_PREFIX) && value != null) {
        parsedConfigs.put(name, value);
      }
    });
    return parsedConfigs;
  }

  private JwtValidator getTokenValidator(ConfigurationUtils cu, Map<String, ?> configs) {
    // if the schema registry oauth configs are set it is given higher preference
    String scopeClaimName = cu.get(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME) != null
        ? cu.validateString(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME)
        : SchemaRegistryClientConfig.getBearerAuthScopeClaimName(configs);

    String subClaimName = cu.get(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME) != null
        ? cu.validateString(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME)
        : SchemaRegistryClientConfig.getBearerAuthSubClaimName(configs);

    return new ClientJwtValidator(scopeClaimName, subClaimName);
  }

  Map<String, Object> getConfigsForJaasUtil(Map<String, ?> configs) {
    Map<String, Object> updatedConfigs = new HashMap<>(configs);
    if (updatedConfigs.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
      Object saslJaasConfig = updatedConfigs.get(SaslConfigs.SASL_JAAS_CONFIG);
      if (saslJaasConfig instanceof String) {
        updatedConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, new Password((String) saslJaasConfig));
      }
    }
    return updatedConfigs;
  }
}


