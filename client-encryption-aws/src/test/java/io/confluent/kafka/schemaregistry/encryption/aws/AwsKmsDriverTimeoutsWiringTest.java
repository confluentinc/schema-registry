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

package io.confluent.kafka.schemaregistry.encryption.aws;

import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_CONNECT_TIMEOUT_MS;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_OPERATION_TIMEOUT_MS;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_REQUEST_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.crypto.tink.Aead;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.kms.KmsClient;

public class AwsKmsDriverTimeoutsWiringTest {

  private static final String KEK_URL =
      "aws-kms://arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab";

  private static Map<String, Object> configs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AwsKmsDriver.ACCESS_KEY_ID, "id");
    configs.put(AwsKmsDriver.SECRET_ACCESS_KEY, "secret");
    return configs;
  }

  private static ClientOverrideConfiguration overridesOf(Map<String, Object> configs)
      throws Exception {
    Aead aead = new AwsKmsDriver().newKmsClient(configs, Optional.of(KEK_URL)).getAead(KEK_URL);
    Field field = AwsKmsAead.class.getDeclaredField("kmsClient");
    field.setAccessible(true);
    try (KmsClient sdkClient = (KmsClient) field.get(aead)) {
      return sdkClient.serviceClientConfiguration().overrideConfiguration();
    }
  }

  @Test
  public void configuredTimeoutsReachTheSdkClient() throws Exception {
    Map<String, Object> configs = configs();
    configs.put(KMS_CONNECT_TIMEOUT_MS, "2000");
    configs.put(KMS_REQUEST_TIMEOUT_MS, "4000");
    configs.put(KMS_OPERATION_TIMEOUT_MS, "12000");

    ClientOverrideConfiguration overrides = overridesOf(configs);
    assertEquals(Optional.of(Duration.ofMillis(6000)), overrides.apiCallAttemptTimeout());
    assertEquals(Optional.of(Duration.ofMillis(12000)), overrides.apiCallTimeout());
  }

  @Test
  public void noKeysLeavesTheSdkClientUntouched() throws Exception {
    ClientOverrideConfiguration overrides = overridesOf(configs());
    assertEquals(Optional.empty(), overrides.apiCallAttemptTimeout());
    assertEquals(Optional.empty(), overrides.apiCallTimeout());
  }

  @Test
  public void invalidTimeoutsAreRejectedByTheDriver() {
    Map<String, Object> configs = configs();
    configs.put(KMS_OPERATION_TIMEOUT_MS, "12000");
    assertThrows(IllegalArgumentException.class,
        () -> new AwsKmsDriver().newKmsClient(configs, Optional.of(KEK_URL)));
  }
}
