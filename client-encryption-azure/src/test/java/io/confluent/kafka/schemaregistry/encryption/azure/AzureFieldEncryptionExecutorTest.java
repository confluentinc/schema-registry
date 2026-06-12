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

package io.confluent.kafka.schemaregistry.encryption.azure;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpResponse;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.schemaregistry.encryption.EncryptionProperties;
import java.security.GeneralSecurityException;
import java.util.List;
import org.junit.Test;

public class AzureFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  public AzureFieldEncryptionExecutorTest() throws Exception {
    super();
  }

  @Override
  protected EncryptionProperties getFieldEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor) {
    return new AzureEncryptionProperties(ruleNames, ruleExecutor);
  }

  private static HttpResponseException httpException(int statusCode) {
    HttpResponse response = mock(HttpResponse.class);
    when(response.getStatusCode()).thenReturn(statusCode);
    return new HttpResponseException("denied", response);
  }

  @Test
  public void testIsAccessDeniedForForbiddenAndUnauthorized() {
    AzureKmsDriver driver = new AzureKmsDriver();
    assertTrue(driver.isAccessDeniedException(httpException(403)));
    assertTrue(driver.isAccessDeniedException(httpException(401)));
  }

  @Test
  public void testIsNotAccessDeniedForOtherErrors() {
    AzureKmsDriver driver = new AzureKmsDriver();
    assertFalse(driver.isAccessDeniedException(httpException(404)));
    assertFalse(driver.isAccessDeniedException(httpException(500)));
    assertFalse(driver.isAccessDeniedException(new RuntimeException("not azure")));
  }

  @Test
  public void testIsAccessDeniedWalksCauseChain() {
    AzureKmsDriver driver = new AzureKmsDriver();
    assertTrue(driver.isAccessDenied(
        new GeneralSecurityException("encryption failed", httpException(403))));
    assertFalse(driver.isAccessDenied(
        new GeneralSecurityException("encryption failed", httpException(503))));
    assertFalse(driver.isAccessDenied(null));
  }
}

