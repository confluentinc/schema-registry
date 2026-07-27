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

package io.confluent.kafka.schemaregistry.encryption.alicloud;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.aliyun.tea.TeaException;
import io.confluent.kafka.schemaregistry.encryption.EncryptionProperties;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import java.security.GeneralSecurityException;
import java.util.List;
import org.junit.Test;

public class AliCloudFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  public AliCloudFieldEncryptionExecutorTest() throws Exception {
    super();
  }

  @Override
  protected EncryptionProperties getFieldEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor) {
    return new AliCloudEncryptionProperties(ruleNames, ruleExecutor);
  }

  @Test
  public void testIsAccessDeniedForForbiddenAndUnauthorized() {
    AliCloudKmsDriver driver = new AliCloudKmsDriver();
    assertTrue(driver.isAccessDeniedException(teaException(403, "SomethingElse")));
    assertTrue(driver.isAccessDeniedException(teaException(401, "SomethingElse")));
  }

  @Test
  public void testIsAccessDeniedByErrorCode() {
    AliCloudKmsDriver driver = new AliCloudKmsDriver();
    assertTrue(driver.isAccessDeniedException(teaException(400, "AccessDenied")));
    assertTrue(driver.isAccessDeniedException(teaException(400, "Unauthorized")));
  }

  @Test
  public void testIsNotAccessDeniedForOtherErrors() {
    AliCloudKmsDriver driver = new AliCloudKmsDriver();
    assertFalse(driver.isAccessDeniedException(teaException(400, "InvalidParameter")));
    assertFalse(driver.isAccessDeniedException(teaException(500, "InternalError")));
    assertFalse(driver.isAccessDeniedException(new RuntimeException("not alicloud")));
  }

  @Test
  public void testIsAccessDeniedWalksCauseChain() {
    AliCloudKmsDriver driver = new AliCloudKmsDriver();
    assertTrue(driver.isAccessDenied(
        new GeneralSecurityException("encryption failed", teaException(403, "AccessDenied"))));
    assertFalse(driver.isAccessDenied(
        new GeneralSecurityException(
            "encryption failed",
            teaException(503, "ServiceUnavailable"))));
    assertFalse(driver.isAccessDenied(null));
  }

  private static TeaException teaException(int statusCode, String code) {
    TeaException exception = new TeaException();
    exception.setStatusCode(statusCode);
    exception.setCode(code);
    return exception;
  }
}
