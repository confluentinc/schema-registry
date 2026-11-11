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

import io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;

public class AwsKmsClientTimeoutsTest {

  private static KmsClient build(KmsTimeouts timeouts) {
    KmsClientBuilder builder = KmsClient.builder()
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .region(Region.US_WEST_2);
    AwsKmsClient.applyTimeouts(builder, timeouts);
    return builder.build();
  }

  @Test
  public void noTimeoutsLeavesSdkDefaults() {
    try (KmsClient client = build(KmsTimeouts.NONE)) {
      assertEquals(Optional.empty(),
          client.serviceClientConfiguration().overrideConfiguration().apiCallAttemptTimeout());
      assertEquals(Optional.empty(),
          client.serviceClientConfiguration().overrideConfiguration().apiCallTimeout());
    }
  }

  @Test
  public void attemptTimeoutIsConnectPlusRequest() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KMS_CONNECT_TIMEOUT_MS, "1000");
    configs.put(KMS_REQUEST_TIMEOUT_MS, "4000");
    try (KmsClient client = build(KmsTimeouts.fromConfigs(configs))) {
      assertEquals(Optional.of(Duration.ofMillis(5000)),
          client.serviceClientConfiguration().overrideConfiguration().apiCallAttemptTimeout());
      assertEquals(Optional.empty(),
          client.serviceClientConfiguration().overrideConfiguration().apiCallTimeout());
    }
  }

  @Test
  public void absentConnectTimeoutIsCostedAtTheSdkDefault() {
    assertEquals(Duration.ofSeconds(2), AwsKmsClient.DEFAULT_CONNECT_TIMEOUT);
    try (KmsClient client = build(KmsTimeouts.fromConfigs(
        Collections.singletonMap(KMS_REQUEST_TIMEOUT_MS, "4000")))) {
      assertEquals(Optional.of(Duration.ofMillis(4000).plus(AwsKmsClient.DEFAULT_CONNECT_TIMEOUT)),
          client.serviceClientConfiguration().overrideConfiguration().apiCallAttemptTimeout());
    }
  }

  @Test
  public void operationTimeoutBoundsTheWholeCall() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KMS_CONNECT_TIMEOUT_MS, "2000");
    configs.put(KMS_REQUEST_TIMEOUT_MS, "4000");
    configs.put(KMS_OPERATION_TIMEOUT_MS, "12000");
    try (KmsClient client = build(KmsTimeouts.fromConfigs(configs))) {
      assertEquals(Optional.of(Duration.ofMillis(6000)),
          client.serviceClientConfiguration().overrideConfiguration().apiCallAttemptTimeout());
      assertEquals(Optional.of(Duration.ofMillis(12000)),
          client.serviceClientConfiguration().overrideConfiguration().apiCallTimeout());
    }
  }
}
