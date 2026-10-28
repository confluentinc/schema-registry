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

package io.confluent.kafka.schemaregistry.encryption.hcvault;

import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.TOKEN_ID;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.VAULT_ADDR;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.VAULT_APPROLE_ROLE_ID;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.VAULT_APPROLE_SECRET_ID;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.VAULT_TOKEN;
import static io.confluent.kafka.schemaregistry.encryption.hcvault.HcVaultKmsDriver.VAULT_TRUSTED_ADDRESSES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.github.jopenlibs.vault.EnvironmentLoader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class HcVaultKmsDriverTest {

  private static final String ATTACKER_KEY = "hcvault://https://attacker.example.com/transit/keys/x";
  private static final String TRUSTED_KEY = "hcvault://https://vault.internal:8200/transit/keys/x";

  private static HcVaultKmsDriver driver(Map<String, String> env) {
    return new HcVaultKmsDriver(new EnvironmentLoader() {
      @Override
      public String loadVariable(String name) {
        return env.get(name);
      }
    });
  }

  private static Map<String, String> envWithToken() {
    Map<String, String> env = new HashMap<>();
    env.put(VAULT_TOKEN, "server-token");
    env.put(VAULT_ADDR, "https://vault.internal:8200");
    return env;
  }

  @Test
  public void testAmbientTokenRejectedForUntrustedAddress() {
    GeneralSecurityException e = assertThrows(GeneralSecurityException.class,
        () -> driver(envWithToken()).newKmsClient(Collections.emptyMap(),
            Optional.of(ATTACKER_KEY)));
    assertTrue(e.getMessage().contains("https://attacker.example.com:443"));
  }

  @Test
  public void testAmbientAppRoleRejectedForUntrustedAddress() {
    Map<String, String> env = new HashMap<>();
    env.put(VAULT_APPROLE_ROLE_ID, "role");
    env.put(VAULT_APPROLE_SECRET_ID, "secret");
    assertThrows(GeneralSecurityException.class,
        () -> driver(env).newKmsClient(Collections.emptyMap(), Optional.of(ATTACKER_KEY)));
  }

  @Test
  public void testAmbientTokenAllowedForVaultAddr() throws Exception {
    assertNotNull(driver(envWithToken()).newKmsClient(Collections.emptyMap(),
        Optional.of(TRUSTED_KEY)));
  }

  @Test
  public void testAmbientTokenAllowedForTrustedAddresses() throws Exception {
    Map<String, String> env = envWithToken();
    env.put(VAULT_TRUSTED_ADDRESSES, "https://other.internal, https://ATTACKER.example.com:443");
    assertNotNull(driver(env).newKmsClient(Collections.emptyMap(), Optional.of(ATTACKER_KEY)));
  }

  @Test
  public void testExplicitTokenAllowedForAnyAddress() throws Exception {
    assertNotNull(driver(envWithToken()).newKmsClient(
        Collections.singletonMap(TOKEN_ID, "tenant-token"), Optional.of(ATTACKER_KEY)));
  }

  @Test
  public void testNoAmbientCredentialsAllowedForAnyAddress() throws Exception {
    assertNotNull(driver(new HashMap<>()).newKmsClient(Collections.emptyMap(),
        Optional.of(ATTACKER_KEY)));
  }

  @Test
  public void testToAddress() {
    assertEquals("https://vault:443", HcVaultKmsDriver.toAddress("https://Vault/transit/keys/x"));
    assertEquals("http://vault:80", HcVaultKmsDriver.toAddress("http://vault"));
    assertEquals("https://vault:8200", HcVaultKmsDriver.toAddress("https://vault:8200"));
    assertNull(HcVaultKmsDriver.toAddress("vault:8200"));
    assertNull(HcVaultKmsDriver.toAddress(null));
  }
}
