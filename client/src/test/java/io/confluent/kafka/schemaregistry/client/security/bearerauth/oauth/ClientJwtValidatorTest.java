/*
 * Copyright 2025 Confluent Inc.
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

import org.apache.kafka.common.security.oauthbearer.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.JwtValidatorException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.junit.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class ClientJwtValidatorTest {

  protected JwtValidator createJwtValidator() {
    return new ClientJwtValidator("scope", "subj");
  }

  @Test
  public void testNull() throws Exception {
    JwtValidator validator = createJwtValidator();
    assertThrows(JwtValidatorException.class, () -> validator.validate(null));
  }

  @Test
  public void testEmptyString() throws Exception {
    JwtValidator validator = createJwtValidator();
    assertThrows(JwtValidatorException.class, () -> validator.validate(""));
  }

  @Test
  public void testWhitespace() throws Exception {
    JwtValidator validator = createJwtValidator();
    assertThrows(JwtValidatorException.class, () -> validator.validate(""));
  }

  @Test
  public void testEmptySections() throws Exception {
    JwtValidator validator = createJwtValidator();
    assertThrows(JwtValidatorException.class, () -> validator.validate(".."));
  }

  @Test
  public void testMissingHeader() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = "";
    String payload = "payload";
    String signature = "signature";
    String accessToken = String.format("%s.%s.%s", header, payload, signature);
    assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
  }

  @Test
  public void testMissingPayload() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = "header";
    String payload = "";
    String signature = "signature";
    String accessToken = String.format("%s.%s.%s", header, payload, signature);
    assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
  }

  @Test
  public void testMissingSignature() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = "header";
    String payload = "payload";
    String signature = "";
    String accessToken = String.format("%s.%s.%s", header, payload, signature);
    assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
  }

  @Test
  public void testValidJwtWithStringScope() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = Base64.getEncoder().encodeToString("{\"alg\":\"none\"}".getBytes());
    String payload = Base64.getEncoder().encodeToString(
        ("{\"scope\":\"test-scope\",\"exp\":1735689600,\"subj\":\"test-user\",\"iat\":1735686000}")
            .getBytes());
    String signature = Base64.getEncoder().encodeToString("signature".getBytes());;
    String accessToken = String.format("%s.%s.%s", header, payload, signature);

    OAuthBearerToken token = validator.validate(accessToken);
    assertNotNull(token);
    assertEquals(Collections.singleton("test-scope"), token.scope());
    assertEquals("test-user", token.principalName());
    assertTrue(token.lifetimeMs() > 0);
    assertTrue(token.startTimeMs() > 0);
  }

  @Test
  public void testValidJwtWithArrayScope() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = Base64.getEncoder().encodeToString("{\"alg\":\"none\"}".getBytes());
    String payload = Base64.getEncoder().encodeToString(
        ("{\"scope\":[\"scope1\",\"scope2\"],\"exp\":1735689600,\"subj\":\"test-user\",\"iat\":1735686000}")
            .getBytes());
    String signature = Base64.getEncoder().encodeToString("signature".getBytes());;
    String accessToken = String.format("%s.%s.%s", header, payload, signature);

    OAuthBearerToken token = validator.validate(accessToken);
    assertNotNull(token);
    Set<String> expectedScopes = new HashSet<>(Arrays.asList("scope1", "scope2"));
    assertEquals(expectedScopes, token.scope());
  }

  @Test
  public void testMissingScopeClaim() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = Base64.getEncoder().encodeToString("{\"alg\":\"none\"}".getBytes());
    String payload = Base64.getEncoder().encodeToString(
        ("{\"exp\":1735689600,\"subj\":\"test-user\",\"iat\":1735686000}")
            .getBytes());
    String signature = Base64.getEncoder().encodeToString("signature".getBytes());;
    String accessToken = String.format("%s.%s.%s", header, payload, signature);

    OAuthBearerToken token = validator.validate(accessToken);
    assertNotNull(token);
    assertTrue(token.scope().isEmpty());
  }

  @Test
  public void testMissingExpirationClaim() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = Base64.getEncoder().encodeToString("{\"alg\":\"none\"}".getBytes());
    String payload = Base64.getEncoder().encodeToString(
        ("{\"scope\":\"test-scope\",\"subj\":\"test-user\",\"iat\":1735686000}")
            .getBytes());
    String signature = Base64.getEncoder().encodeToString("signature".getBytes());;
    String accessToken = String.format("%s.%s.%s", header, payload, signature);

    assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
  }

  @Test
  public void testMissingSubjectClaim() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = Base64.getEncoder().encodeToString("{\"alg\":\"none\"}".getBytes());
    String payload = Base64.getEncoder().encodeToString(
        ("{\"scope\":\"test-scope\",\"exp\":1735689600,\"iat\":1735686000}")
            .getBytes());
    String signature = Base64.getEncoder().encodeToString("signature".getBytes());;
    String accessToken = String.format("%s.%s.%s", header, payload, signature);

    assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
  }

  @Test
  public void testInvalidExpirationType() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = Base64.getEncoder().encodeToString("{\"alg\":\"none\"}".getBytes());
    String payload = Base64.getEncoder().encodeToString(
        ("{\"scope\":\"test-scope\",\"exp\":-1,\"subj\":\"test-user\",\"iat\":1735686000}")
            .getBytes());
    String signature = Base64.getEncoder().encodeToString("signature".getBytes());;
    String accessToken = String.format("%s.%s.%s", header, payload, signature);

    assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
  }

  @Test
  public void testInvalidIssuedAtType() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = Base64.getEncoder().encodeToString("{\"alg\":\"none\"}".getBytes());
    String payload = Base64.getEncoder().encodeToString(
        ("{\"scope\":\"test-scope\",\"exp\":1735689600,\"subj\":\"test-user\",\"iat\":-1}")
            .getBytes());
    String signature = Base64.getEncoder().encodeToString("signature".getBytes());;
    String accessToken = String.format("%s.%s.%s", header, payload, signature);

    assertThrows(JwtValidatorException.class, () -> validator.validate(accessToken));
  }

  @Test
  public void testInvalidScopeType() throws Exception {
    JwtValidator validator = createJwtValidator();
    String header = Base64.getEncoder().encodeToString("{\"alg\":\"none\"}".getBytes());
    String payload = Base64.getEncoder().encodeToString(
        ("{\"scope\":123,\"exp\":1735689600,\"subj\":\"test-user\",\"iat\":1735686000}")
            .getBytes());
    String signature = Base64.getEncoder().encodeToString("signature".getBytes());;
    String accessToken = String.format("%s.%s.%s", header, payload, signature);

    OAuthBearerToken token = validator.validate(accessToken);
    assertNotNull(token);
    assertTrue(token.scope().isEmpty());
  }
}
