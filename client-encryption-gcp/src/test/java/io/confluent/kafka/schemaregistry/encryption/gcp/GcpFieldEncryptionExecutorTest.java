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

package io.confluent.kafka.schemaregistry.encryption.gcp;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.schemaregistry.encryption.EncryptionProperties;
import java.security.GeneralSecurityException;
import java.util.List;
import org.junit.Test;

public class GcpFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  public GcpFieldEncryptionExecutorTest() throws Exception {
    super();
  }

  @Override
  protected EncryptionProperties getFieldEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor) {
    return new GcpEncryptionProperties(ruleNames, ruleExecutor);
  }

  private static HttpResponseException httpException(int statusCode) {
    return new HttpResponseException.Builder(statusCode, "status", new HttpHeaders()).build();
  }

  @Test
  public void testIsAccessDeniedForForbiddenAndUnauthorized() {
    GcpKmsDriver driver = new GcpKmsDriver();
    assertTrue(driver.isAccessDeniedException(httpException(403)));
    assertTrue(driver.isAccessDeniedException(httpException(401)));
  }

  @Test
  public void testIsNotAccessDeniedForOtherErrors() {
    GcpKmsDriver driver = new GcpKmsDriver();
    assertFalse(driver.isAccessDeniedException(httpException(404)));
    assertFalse(driver.isAccessDeniedException(httpException(500)));
    assertFalse(driver.isAccessDeniedException(new RuntimeException("not http")));
  }

  @Test
  public void testIsAccessDeniedWalksCauseChain() {
    GcpKmsDriver driver = new GcpKmsDriver();
    assertTrue(driver.isAccessDenied(
        new GeneralSecurityException("encryption failed", httpException(403))));
    assertFalse(driver.isAccessDenied(
        new GeneralSecurityException("encryption failed", httpException(503))));
    assertFalse(driver.isAccessDenied(null));
  }
}

