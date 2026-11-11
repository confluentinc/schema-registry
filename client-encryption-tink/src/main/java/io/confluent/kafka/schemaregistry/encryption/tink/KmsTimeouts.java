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

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Opt-in timeouts for the HTTP clients that KMS drivers build. A key that is not set leaves the
 * provider SDK's default untouched.
 */
public final class KmsTimeouts {

  /** Bound on establishing a TCP/TLS connection to the KMS. */
  public static final String KMS_CONNECT_TIMEOUT_MS = "kms.connect.timeout.ms";
  /** Bound on a single request attempt to the KMS, once connected. */
  public static final String KMS_REQUEST_TIMEOUT_MS = "kms.request.timeout.ms";
  /**
   * Deadline on one whole KMS operation, including SDK retries. Requires the other two keys and
   * must be at least their sum (one attempt).
   */
  public static final String KMS_OPERATION_TIMEOUT_MS = "kms.operation.timeout.ms";

  public static final KmsTimeouts NONE = new KmsTimeouts(null, null, null);

  private final Duration connectTimeout;
  private final Duration requestTimeout;
  private final Duration operationTimeout;

  private KmsTimeouts(Duration connectTimeout, Duration requestTimeout, Duration operationTimeout) {
    this.connectTimeout = connectTimeout;
    this.requestTimeout = requestTimeout;
    this.operationTimeout = operationTimeout;
  }

  /** Reads the three timeout keys; each may be absent. */
  public static KmsTimeouts fromConfigs(Map<String, ?> configs) {
    if (configs == null) {
      return NONE;
    }
    Duration connect = parse(configs, KMS_CONNECT_TIMEOUT_MS);
    Duration request = parse(configs, KMS_REQUEST_TIMEOUT_MS);
    Duration operation = parse(configs, KMS_OPERATION_TIMEOUT_MS);
    if (operation != null) {
      if (connect == null || request == null) {
        throw new IllegalArgumentException(KMS_OPERATION_TIMEOUT_MS + " requires both "
            + KMS_CONNECT_TIMEOUT_MS + " and " + KMS_REQUEST_TIMEOUT_MS);
      }
      long attemptMs = connect.toMillis() + request.toMillis();
      if (operation.toMillis() < attemptMs) {
        throw new IllegalArgumentException(KMS_OPERATION_TIMEOUT_MS + " (" + operation.toMillis()
            + ") must cover one attempt, i.e. " + KMS_CONNECT_TIMEOUT_MS + " + "
            + KMS_REQUEST_TIMEOUT_MS + " (" + attemptMs + ")");
      }
    }
    return connect == null && request == null && operation == null
        ? NONE
        : new KmsTimeouts(connect, request, operation);
  }

  private static Duration parse(Map<String, ?> configs, String key) {
    Object value = configs.get(key);
    if (value == null) {
      return null;
    }
    long ms;
    try {
      ms = Long.parseLong(value.toString().trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be a positive integer, got: " + value, e);
    }
    if (ms <= 0) {
      throw new IllegalArgumentException(key + " must be a positive integer, got: " + value);
    }
    return Duration.ofMillis(ms);
  }

  public Optional<Duration> connectTimeout() {
    return Optional.ofNullable(connectTimeout);
  }

  public Optional<Duration> requestTimeout() {
    return Optional.ofNullable(requestTimeout);
  }

  public Optional<Duration> operationTimeout() {
    return Optional.ofNullable(operationTimeout);
  }

  public boolean isEmpty() {
    return connectTimeout == null && requestTimeout == null && operationTimeout == null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KmsTimeouts)) {
      return false;
    }
    KmsTimeouts that = (KmsTimeouts) o;
    return Objects.equals(connectTimeout, that.connectTimeout)
        && Objects.equals(requestTimeout, that.requestTimeout)
        && Objects.equals(operationTimeout, that.operationTimeout);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectTimeout, requestTimeout, operationTimeout);
  }

  @Override
  public String toString() {
    return "KmsTimeouts{connect=" + connectTimeout + ", request=" + requestTimeout
        + ", operation=" + operationTimeout + "}";
  }
}
