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
 * Optional timeouts for the HTTP clients that KMS drivers build.
 *
 * <p>Strictly opt-in: a timeout that is not configured is left to the provider SDK's own default,
 * so callers that set none of the keys (every client application today) get exactly the clients
 * they got before these keys existed. Server-side callers set them to bound how long one KMS
 * operation can take.
 *
 * <p>Three layers, each enforced by the provider SDK itself rather than by a wrapper thread:
 * <ul>
 *   <li>{@link #KMS_CONNECT_TIMEOUT_MS}: establishing the TCP/TLS connection.</li>
 *   <li>{@link #KMS_REQUEST_TIMEOUT_MS}: one request attempt, once connected.</li>
 *   <li>{@link #KMS_OPERATION_TIMEOUT_MS}: the whole wrap/unwrap including the SDK's retries.
 *       AWS enforces it natively and exactly ({@code apiCallTimeout}). Azure derives how many
 *       retries fit in it, costing each attempt at connect + request, so it is a tight bound on
 *       the realistic worst case rather than a hard wall clock. GCP has no SDK-native deadline
 *       either: one attempt is bounded by connect + request, which this deadline must cover, but
 *       a 401 replay after a token refresh can double that, and the token fetch itself is outside
 *       these timeouts.</li>
 * </ul>
 */
public final class KmsTimeouts {

  /** Bound on establishing a TCP/TLS connection to the KMS. */
  public static final String KMS_CONNECT_TIMEOUT_MS = "kms.connect.timeout.ms";
  /**
   * Bound on a single request attempt to the KMS, once connected. SDK-level retries may each take
   * up to this long; see {@link #KMS_OPERATION_TIMEOUT_MS} for the deadline on the whole operation.
   */
  public static final String KMS_REQUEST_TIMEOUT_MS = "kms.request.timeout.ms";
  /**
   * Deadline on one whole KMS operation (wrap or unwrap), including every retry the SDK makes.
   * Requires {@link #KMS_REQUEST_TIMEOUT_MS} and must cover at least one attempt, i.e.
   * {@link #KMS_CONNECT_TIMEOUT_MS} (if set) plus {@link #KMS_REQUEST_TIMEOUT_MS}: that sum is
   * the unit the retry budget is computed in, and a smaller deadline is one no transport can
   * honour.
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

  /**
   * Reads the {@code kms.*.timeout.ms} keys from {@code configs}; each may be absent.
   *
   * @throws IllegalArgumentException if a value is present but not a positive integer, or if
   *     {@link #KMS_OPERATION_TIMEOUT_MS} is set without {@link #KMS_REQUEST_TIMEOUT_MS} or is
   *     smaller than one attempt ({@link #KMS_CONNECT_TIMEOUT_MS}, if set, plus
   *     {@link #KMS_REQUEST_TIMEOUT_MS})
   */
  public static KmsTimeouts fromConfigs(Map<String, ?> configs) {
    if (configs == null) {
      return NONE;
    }
    Duration connect = parse(configs, KMS_CONNECT_TIMEOUT_MS);
    Duration request = parse(configs, KMS_REQUEST_TIMEOUT_MS);
    Duration operation = parse(configs, KMS_OPERATION_TIMEOUT_MS);
    if (operation != null) {
      if (request == null) {
        throw new IllegalArgumentException(
            KMS_OPERATION_TIMEOUT_MS + " requires " + KMS_REQUEST_TIMEOUT_MS);
      }
      // One attempt costs connect + request (each retry reconnects), so an operation timeout that
      // cannot cover a single attempt is one no transport can honour.
      long attemptMs = request.toMillis() + (connect == null ? 0 : connect.toMillis());
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

  /** Deadline for a whole operation including retries; never present without a request timeout. */
  public Optional<Duration> operationTimeout() {
    return Optional.ofNullable(operationTimeout);
  }

  /** True when no timeout is configured, i.e. the SDK defaults must be left untouched. */
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
