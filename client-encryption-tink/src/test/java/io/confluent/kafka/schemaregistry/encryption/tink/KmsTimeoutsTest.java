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

package io.confluent.kafka.schemaregistry.encryption.tink;

import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_CONNECT_TIMEOUT_MS;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_OPERATION_TIMEOUT_MS;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsTimeouts.KMS_REQUEST_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class KmsTimeoutsTest {

  @Test
  public void absentKeysMeanNoTimeouts() {
    assertSame(KmsTimeouts.NONE, KmsTimeouts.fromConfigs(null));
    assertSame(KmsTimeouts.NONE, KmsTimeouts.fromConfigs(Collections.emptyMap()));
    assertSame(KmsTimeouts.NONE,
        KmsTimeouts.fromConfigs(Collections.singletonMap("unrelated", "1")));
    assertTrue(KmsTimeouts.NONE.isEmpty());
    assertEquals(Optional.empty(), KmsTimeouts.NONE.connectTimeout());
    assertEquals(Optional.empty(), KmsTimeouts.NONE.requestTimeout());
    assertEquals(Optional.empty(), KmsTimeouts.NONE.operationTimeout());
  }

  @Test
  public void parsesStringsAndNumbers() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KMS_CONNECT_TIMEOUT_MS, "2500");
    configs.put(KMS_REQUEST_TIMEOUT_MS, 7000);
    configs.put(KMS_OPERATION_TIMEOUT_MS, 21000L);
    KmsTimeouts timeouts = KmsTimeouts.fromConfigs(configs);
    assertFalse(timeouts.isEmpty());
    assertEquals(Optional.of(Duration.ofMillis(2500)), timeouts.connectTimeout());
    assertEquals(Optional.of(Duration.ofMillis(7000)), timeouts.requestTimeout());
    assertEquals(Optional.of(Duration.ofMillis(21000)), timeouts.operationTimeout());
  }

  @Test
  public void eitherKeyAloneIsEnough() {
    KmsTimeouts connectOnly =
        KmsTimeouts.fromConfigs(Collections.singletonMap(KMS_CONNECT_TIMEOUT_MS, "100"));
    assertFalse(connectOnly.isEmpty());
    assertEquals(Optional.empty(), connectOnly.requestTimeout());

    KmsTimeouts requestOnly =
        KmsTimeouts.fromConfigs(Collections.singletonMap(KMS_REQUEST_TIMEOUT_MS, "100"));
    assertFalse(requestOnly.isEmpty());
    assertEquals(Optional.empty(), requestOnly.connectTimeout());
    assertEquals(Optional.empty(), requestOnly.operationTimeout());
  }

  @Test
  public void rejectsNonPositiveAndNonNumericValues() {
    for (Object bad : new Object[] {"0", "-1", "abc", ""}) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> KmsTimeouts.fromConfigs(Collections.singletonMap(KMS_REQUEST_TIMEOUT_MS, bad)));
      assertTrue(e.getMessage(), e.getMessage().contains(KMS_REQUEST_TIMEOUT_MS));
    }
  }

  @Test
  public void operationTimeoutRequiresRequestTimeout() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> KmsTimeouts.fromConfigs(Collections.singletonMap(KMS_OPERATION_TIMEOUT_MS, "30000")));
    assertTrue(e.getMessage(), e.getMessage().contains(KMS_REQUEST_TIMEOUT_MS));
  }

  @Test
  public void operationTimeoutMustCoverOneRequest() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KMS_REQUEST_TIMEOUT_MS, "10000");
    configs.put(KMS_OPERATION_TIMEOUT_MS, "9999");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> KmsTimeouts.fromConfigs(configs));
    assertTrue(e.getMessage(), e.getMessage().contains(KMS_OPERATION_TIMEOUT_MS));

    // Equal is allowed: exactly one attempt.
    configs.put(KMS_OPERATION_TIMEOUT_MS, "10000");
    assertEquals(Optional.of(Duration.ofMillis(10000)),
        KmsTimeouts.fromConfigs(configs).operationTimeout());
  }

  @Test
  public void operationTimeoutMustCoverConnectPlusRequest() {
    // Each attempt reconnects, so a budget that covers the request but not the connect in front
    // of it cannot be honoured by any transport: 10s connect + 1s request does not fit in 5s.
    Map<String, Object> configs = new HashMap<>();
    configs.put(KMS_CONNECT_TIMEOUT_MS, "10000");
    configs.put(KMS_REQUEST_TIMEOUT_MS, "1000");
    configs.put(KMS_OPERATION_TIMEOUT_MS, "5000");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> KmsTimeouts.fromConfigs(configs));
    assertTrue(e.getMessage(), e.getMessage().contains(KMS_CONNECT_TIMEOUT_MS));
    assertTrue(e.getMessage(), e.getMessage().contains("11000"));

    // Exactly connect + request is allowed: one attempt, no retries.
    configs.put(KMS_OPERATION_TIMEOUT_MS, "11000");
    assertEquals(Optional.of(Duration.ofMillis(11000)),
        KmsTimeouts.fromConfigs(configs).operationTimeout());
  }
}
