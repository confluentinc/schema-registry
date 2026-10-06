/*
 * Copyright 2026 Confluent Inc.
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

import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClientAssertionRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionSupplierFactory;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that <code>SASL_OAUTHBEARER_INHERIT</code> inherits the Kafka client's OAuth client
 * assertion configuration (KIP-1258), e.g. for AKS Workload Identity, against a local token
 * endpoint.
 */
public class SaslOauthCredentialProviderClientAssertionTest {

  private static final String CLIENT_ASSERTION_TYPE =
      "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
  // AKS projects the federated service account token into a file as a JWT
  private static final String FEDERATED_TOKEN = createJwt("system:serviceaccount:ns:sa", 1);
  private static final String FEDERATED_TOKEN_2 = createJwt("system:serviceaccount:ns:sa", 2);
  private static final String JAAS_CONFIG_PREFIX =
      "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ";

  private final List<TokenRequest> requests = new CopyOnWriteArrayList<>();
  private final List<File> tempFiles = new ArrayList<>();
  private HttpServer tokenEndpoint;
  private String tokenEndpointUrl;
  private String accessToken;
  private String previousAllowedUrls;
  private String previousAllowedFiles;
  private SaslOauthCredentialProvider provider;

  @Before
  public void setUp() throws IOException {
    accessToken = createAccessToken();
    tokenEndpoint = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    tokenEndpoint.createContext("/token", exchange -> {
      String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      requests.add(new TokenRequest(
          exchange.getRequestHeaders().getFirst("Authorization"), parseForm(body)));
      byte[] response = ("{\"access_token\":\"" + accessToken
          + "\",\"token_type\":\"Bearer\",\"expires_in\":3600}").getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, response.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response);
      }
    });
    tokenEndpoint.start();
    tokenEndpointUrl = "http://localhost:" + tokenEndpoint.getAddress().getPort() + "/token";

    previousAllowedUrls = System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG,
        tokenEndpointUrl);
    previousAllowedFiles = System.getProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);
  }

  @After
  public void tearDown() throws IOException {
    if (provider != null) {
      provider.close();
    }
    tokenEndpoint.stop(0);
    restoreProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, previousAllowedUrls);
    restoreProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, previousAllowedFiles);
    tempFiles.forEach(File::delete);
  }

  @Test
  public void testInheritsFileBasedClientAssertion() throws IOException {
    File assertionFile = createAllowedFile(FEDERATED_TOKEN);
    Map<String, Object> configs = baseConfigs(
        "clientId='my-client' scope='api://sr/.default' "
            + "extension_logicalCluster='lkc-123' extension_identityPoolId='pool-123';");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

    provider = configure(configs);

    assertEquals(accessToken, provider.getBearerToken(new URL("https://sr.example.com")));
    assertEquals("pool-123", provider.getTargetIdentityPoolId());
    TokenRequest request = singleRequest();
    assertNull("client assertion must not send client secret basic auth",
        request.authorization);
    assertEquals("client_credentials", request.form.get("grant_type"));
    assertEquals(CLIENT_ASSERTION_TYPE, request.form.get("client_assertion_type"));
    assertEquals(FEDERATED_TOKEN, request.form.get("client_assertion"));
    assertEquals("my-client", request.form.get("client_id"));
    assertEquals("api://sr/.default", request.form.get("scope"));
  }

  @Test
  public void testInheritsKafkaClientCredentialsConfigs() throws IOException {
    File assertionFile = createAllowedFile(FEDERATED_TOKEN);
    Map<String, Object> configs = baseConfigs(";");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "my-client");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_SCOPE, "api://sr/.default");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

    provider = configure(configs);

    assertEquals(accessToken, provider.getBearerToken(new URL("https://sr.example.com")));
    TokenRequest request = singleRequest();
    assertEquals(FEDERATED_TOKEN, request.form.get("client_assertion"));
    assertEquals("my-client", request.form.get("client_id"));
    assertEquals("api://sr/.default", request.form.get("scope"));
  }

  @Test
  public void testSchemaRegistryConfigsOverrideInheritedConfigs() throws IOException {
    File assertionFile = createAllowedFile(FEDERATED_TOKEN);
    Map<String, Object> configs = baseConfigs("clientId='kafka-client' scope='kafka-scope';");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());
    configs.put(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID, "sr-client");
    configs.put(SchemaRegistryClientConfig.BEARER_AUTH_SCOPE, "sr-scope");

    provider = configure(configs);
    provider.getBearerToken(new URL("https://sr.example.com"));

    TokenRequest request = singleRequest();
    assertEquals(FEDERATED_TOKEN, request.form.get("client_assertion"));
    assertEquals("sr-client", request.form.get("client_id"));
    assertEquals("sr-scope", request.form.get("scope"));
  }

  @Test
  public void testClientAssertionPreferredOverInheritedClientSecret() throws IOException {
    File assertionFile = createAllowedFile(FEDERATED_TOKEN);
    Map<String, Object> configs = baseConfigs("clientId='my-client' clientSecret='secret';");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

    provider = configure(configs);
    provider.getBearerToken(new URL("https://sr.example.com"));

    TokenRequest request = singleRequest();
    assertNull(request.authorization);
    assertEquals(FEDERATED_TOKEN, request.form.get("client_assertion"));
  }

  @Test
  public void testSchemaRegistryClientSecretPreferredOverClientAssertion() throws IOException {
    File assertionFile = createAllowedFile(FEDERATED_TOKEN);
    Map<String, Object> configs = baseConfigs("clientId='my-client';");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());
    configs.put(SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET, "sr-secret");

    provider = configure(configs);
    provider.getBearerToken(new URL("https://sr.example.com"));

    TokenRequest request = singleRequest();
    assertEquals("Basic " + Base64.getEncoder().encodeToString(
            "my-client:sr-secret".getBytes(StandardCharsets.UTF_8)), request.authorization);
    assertFalse(request.form.containsKey("client_assertion"));
  }

  @Test
  public void testInheritsClientSecretFromKafkaClientCredentialsConfigs() throws IOException {
    Map<String, Object> configs = baseConfigs(";");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "my-client");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, "kafka-secret");

    provider = configure(configs);
    provider.getBearerToken(new URL("https://sr.example.com"));

    assertEquals("Basic " + Base64.getEncoder().encodeToString(
            "my-client:kafka-secret".getBytes(StandardCharsets.UTF_8)),
        singleRequest().authorization);
  }

  @Test
  public void testMissingClientSecretAndAssertionFails() {
    Map<String, Object> configs = baseConfigs("clientId='my-client';");

    ConfigException e = assertThrows(ConfigException.class, () -> configure(configs));
    assertTrue(e.getMessage(), e.getMessage().contains("clientSecret"));
  }

  @Test
  public void testInheritsLocallySignedClientAssertion() throws Exception {
    KeyPair keyPair = generateKeyPair();
    File privateKeyFile = createPrivateKeyFile(keyPair);
    Map<String, Object> configs = baseConfigs("clientId='my-client';");
    // sasl.oauthbearer.assertion.algorithm and the claim lifetimes are left at their defaults
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE,
        privateKeyFile.getAbsolutePath());
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, "my-client");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, "my-client");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, tokenEndpointUrl);

    provider = configure(configs);
    provider.getBearerToken(new URL("https://sr.example.com"));

    TokenRequest request = singleRequest();
    assertEquals(CLIENT_ASSERTION_TYPE, request.form.get("client_assertion_type"));
    String[] assertion = request.form.get("client_assertion").split("\\.");
    assertEquals(3, assertion.length);
    ObjectMapper mapper = new ObjectMapper();
    assertEquals("RS256", mapper.readTree(decode(assertion[0])).get("alg").asText());
    JsonNode claims = mapper.readTree(decode(assertion[1]));
    assertEquals("my-client", claims.get("iss").asText());
    assertEquals("my-client", claims.get("sub").asText());
    assertEquals(tokenEndpointUrl, claims.get("aud").asText());

    Signature signature = Signature.getInstance("SHA256withRSA");
    signature.initVerify((RSAPublicKey) keyPair.getPublic());
    signature.update((assertion[0] + "." + assertion[1]).getBytes(StandardCharsets.US_ASCII));
    assertTrue(signature.verify(Base64.getUrlDecoder().decode(assertion[2])));
  }

  @Test
  public void testAssertionFilePreferredOverLocallySignedAssertion() throws IOException {
    File assertionFile = createAllowedFile(FEDERATED_TOKEN);
    Map<String, Object> configs = baseConfigs("clientId='my-client';");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());
    // no private key is configured, so this would fail if a local assertion were created
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, "my-client");

    provider = configure(configs);
    provider.getBearerToken(new URL("https://sr.example.com"));

    assertEquals(FEDERATED_TOKEN, singleRequest().form.get("client_assertion"));
  }

  @Test
  public void testLocallySignedAssertionWithoutPrivateKeyFails() {
    Map<String, Object> configs = baseConfigs("clientId='my-client';");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, "my-client");

    ConfigException e = assertThrows(ConfigException.class, () -> configure(configs));
    assertTrue(e.getMessage(),
        e.getMessage().contains(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE));
  }

  @Test
  public void testInvalidAssertionAlgorithmFails() throws Exception {
    File privateKeyFile = createPrivateKeyFile(generateKeyPair());
    Map<String, Object> configs = baseConfigs("clientId='my-client';");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE,
        privateKeyFile.getAbsolutePath());
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, "my-client");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM, "HS256");

    ConfigException e = assertThrows(ConfigException.class, () -> configure(configs));
    assertTrue(e.getMessage(),
        e.getMessage().contains(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM));
  }

  @Test
  public void testUnrelatedSaslConfigsAreNotRevalidated() throws IOException {
    File assertionFile = createAllowedFile(FEDERATED_TOKEN);
    Map<String, Object> configs = baseConfigs("clientId='my-client';");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());
    // a class only visible to the Kafka client's class loader must not be loaded here
    configs.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "com.example.NotOnClasspath");

    provider = configure(configs);
    provider.getBearerToken(new URL("https://sr.example.com"));

    assertEquals(FEDERATED_TOKEN, singleRequest().form.get("client_assertion"));
  }

  @Test
  public void testKafkaClientConfigsPreferredOverJaasOptions() throws IOException {
    Map<String, Object> configs = baseConfigs(
        "clientId='jaas-client' clientSecret='jaas-secret' scope='jaas-scope';");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "kafka-client");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, "kafka-secret");
    configs.put(SaslConfigs.SASL_OAUTHBEARER_SCOPE, "kafka-scope");

    provider = configure(configs);
    provider.getBearerToken(new URL("https://sr.example.com"));

    TokenRequest request = singleRequest();
    assertEquals("Basic " + Base64.getEncoder().encodeToString(
            "kafka-client:kafka-secret".getBytes(StandardCharsets.UTF_8)), request.authorization);
    assertEquals("kafka-scope", request.form.get("scope"));
  }

  @Test
  public void testRotatedAssertionFileIsReread() throws IOException {
    File assertionFile = createAllowedFile(FEDERATED_TOKEN);
    Map<String, Object> configs = new HashMap<>();
    configs.put(SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

    try (HttpJwtRetriever retriever = new HttpJwtRetriever(
        new ClientAssertionRequestFormatter("my-client", null,
            AssertionSupplierFactory.create(new ConfigurationUtils(configs), Time.SYSTEM)),
        null, tokenEndpointUrl, 100L, 1000L, null, null)) {
      retriever.retrieve();
      Files.write(assertionFile.toPath(), FEDERATED_TOKEN_2.getBytes(StandardCharsets.UTF_8));
      assertTrue(assertionFile.setLastModified(assertionFile.lastModified() + 10_000L));
      retriever.retrieve();
    }

    assertEquals(2, requests.size());
    assertEquals(FEDERATED_TOKEN, requests.get(0).form.get("client_assertion"));
    assertEquals(FEDERATED_TOKEN_2, requests.get(1).form.get("client_assertion"));
  }

  private Map<String, Object> baseConfigs(String jaasOptions) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SaslConfigs.SASL_JAAS_CONFIG, JAAS_CONFIG_PREFIX + jaasOptions);
    configs.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
    configs.put(SchemaRegistryClientConfig.BEARER_AUTH_LOGICAL_CLUSTER, "lsrc-123");
    return configs;
  }

  private static SaslOauthCredentialProvider configure(Map<String, Object> configs) {
    SaslOauthCredentialProvider provider = new SaslOauthCredentialProvider();
    provider.configure(configs);
    return provider;
  }

  private TokenRequest singleRequest() {
    assertEquals(1, requests.size());
    return requests.get(0);
  }

  private File createAllowedFile(String contents) throws IOException {
    File file = File.createTempFile("sr-client-assertion", ".tmp");
    tempFiles.add(file);
    Files.write(file.toPath(), contents.getBytes(StandardCharsets.UTF_8));
    String allowed = System.getProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);
    System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG,
        allowed == null || allowed.isEmpty()
            ? file.getAbsolutePath() : allowed + "," + file.getAbsolutePath());
    return file;
  }

  private static KeyPair generateKeyPair() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return generator.generateKeyPair();
  }

  private File createPrivateKeyFile(KeyPair keyPair) throws IOException {
    return createAllowedFile("-----BEGIN PRIVATE KEY-----\n"
        + Base64.getMimeEncoder().encodeToString(keyPair.getPrivate().getEncoded())
        + "\n-----END PRIVATE KEY-----\n");
  }

  private static String createAccessToken() {
    return createJwt("my-client", 0);
  }

  private static String createJwt(String subject, int version) {
    long now = System.currentTimeMillis() / 1000;
    String header = encode("{\"alg\":\"none\"}");
    String payload = encode("{\"sub\":\"" + subject + "\",\"scope\":\"sr\",\"iat\":" + now
        + ",\"exp\":" + (now + 3600) + ",\"version\":" + version + "}");
    return header + "." + payload + ".signature";
  }

  private static String encode(String json) {
    return Base64.getUrlEncoder().withoutPadding()
        .encodeToString(json.getBytes(StandardCharsets.UTF_8));
  }

  private static String decode(String base64Url) {
    return new String(Base64.getUrlDecoder().decode(base64Url), StandardCharsets.UTF_8);
  }

  private static Map<String, String> parseForm(String body) {
    Map<String, String> form = new HashMap<>();
    for (String pair : body.split("&")) {
      String[] parts = pair.split("=", 2);
      form.put(URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
          parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "");
    }
    return form;
  }

  private static void restoreProperty(String name, String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }

  private static class TokenRequest {
    private final String authorization;
    private final Map<String, String> form;

    TokenRequest(String authorization, Map<String, String> form) {
      this.authorization = authorization;
      this.form = form;
    }
  }
}
