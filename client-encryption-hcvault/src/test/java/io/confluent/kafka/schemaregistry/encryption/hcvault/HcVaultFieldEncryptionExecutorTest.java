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

package io.confluent.kafka.schemaregistry.encryption.hcvault;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.schemaregistry.encryption.EncryptionProperties;
import io.github.jopenlibs.vault.VaultException;
import java.security.GeneralSecurityException;
import java.util.List;
import org.junit.Test;

public class HcVaultFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  public HcVaultFieldEncryptionExecutorTest() throws Exception {
    super();
  }

  @Override
  protected EncryptionProperties getFieldEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor) {
    return new HcVaultEncryptionProperties(ruleNames, ruleExecutor);
  }

  @Test
  public void testIsAccessDeniedForForbiddenAndUnauthorized() {
    HcVaultKmsDriver driver = new HcVaultKmsDriver();
    assertTrue(driver.isAccessDeniedException(new VaultException("denied", 403)));
    assertTrue(driver.isAccessDeniedException(new VaultException("denied", 401)));
  }

  @Test
  public void testIsNotAccessDeniedForOtherErrors() {
    HcVaultKmsDriver driver = new HcVaultKmsDriver();
    assertFalse(driver.isAccessDeniedException(new VaultException("bad request", 400)));
    assertFalse(driver.isAccessDeniedException(new VaultException("server error", 500)));
    assertFalse(driver.isAccessDeniedException(new RuntimeException("not vault")));
  }

  @Test
  public void testIsAccessDeniedWalksCauseChain() {
    HcVaultKmsDriver driver = new HcVaultKmsDriver();
    assertTrue(driver.isAccessDenied(
        new GeneralSecurityException("encryption failed", new VaultException("denied", 403))));
    assertFalse(driver.isAccessDenied(
        new GeneralSecurityException("encryption failed", new VaultException("oops", 500))));
    assertFalse(driver.isAccessDenied(null));
  }
}

