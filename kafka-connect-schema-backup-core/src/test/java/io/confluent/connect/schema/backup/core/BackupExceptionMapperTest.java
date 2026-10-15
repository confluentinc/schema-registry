/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.schema.backup.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.Test;

public class BackupExceptionMapperTest {

  private static final String OP = "wrap Avro backup metadata for topic orders";

  @Test
  public void mapsTimeoutExceptionToConnectRetriable() {
    TimeoutException cause = new TimeoutException("SR timed out");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof RetriableException);
    assertSame(cause, result.getCause());
    assertMessage(result);
  }

  @Test
  public void mapsSerializationExceptionWithNetworkCauseToNetworkException() {
    SocketTimeoutException networkCause = new SocketTimeoutException("connect timed out");
    SerializationException cause = new SerializationException("SR unreachable", networkCause);
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof NetworkException);
    assertSame(cause, result.getCause());
    assertTrue(result.getMessage(), result.getMessage().contains("Network connection error"));
    assertTrue(result.getMessage(), result.getMessage().contains("connect timed out"));
  }

  @Test
  public void mapsSerializationExceptionWithoutNetworkCauseToDataException() {
    SerializationException cause =
        new SerializationException("bad bytes", new IllegalArgumentException("truncated"));
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof DataException);
    assertSame(cause, result.getCause());
  }

  @Test
  public void mapsInvalidConfigurationExceptionToConfigException() {
    InvalidConfigurationException cause = new InvalidConfigurationException("bad url");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof ConfigException);
    assertTrue(result.getMessage(), result.getMessage().contains("bad url"));
  }

  @Test
  public void mapsAuthenticationExceptionToConfigException() {
    AuthenticationException cause = new AuthenticationException("bad token");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof ConfigException);
    assertFalse("auth errors must not be retriable", result instanceof RetriableException);
    assertTrue(result.getMessage(), result.getMessage().contains("bad token"));
  }

  @Test
  public void mapsAuthorizationExceptionToConfigException() {
    AuthorizationException cause = new AuthorizationException("no access");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof ConfigException);
    assertFalse(result instanceof RetriableException);
    assertTrue(result.getMessage(), result.getMessage().contains("no access"));
  }

  @Test
  public void mapsPlainIoExceptionToRetriable() {
    IOException cause = new IOException("connection reset");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue("SR transport IOException must retry", result instanceof RetriableException);
    assertSame(cause, result.getCause());
  }

  @Test
  public void mapsInterruptedIoExceptionToRetriable() {
    KafkaException result = BackupExceptionMapper.classify(OP,
        new InterruptedIOException("read interrupted"));
    assertTrue(result instanceof RetriableException);
  }

  @Test
  public void mapsEofExceptionToRetriable() {
    KafkaException result = BackupExceptionMapper.classify(OP, new EOFException("stream closed"));
    assertTrue(result instanceof RetriableException);
  }

  @Test
  public void mapsSocketExceptionToRetriable() {
    KafkaException result = BackupExceptionMapper.classify(OP,
        new SocketException("connection refused"));
    assertTrue(result instanceof RetriableException);
  }

  @Test
  public void mapsSocketTimeoutExceptionToRetriable() {
    KafkaException result = BackupExceptionMapper.classify(OP,
        new SocketTimeoutException("read timed out"));
    assertTrue(result instanceof RetriableException);
  }

  @Test
  public void mapsRestClientException401ToConfigException() {
    RestClientException cause = new RestClientException("unauthorized", 401, 40101);
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof ConfigException);
    assertFalse(result instanceof RetriableException);
    assertTrue(result.getMessage(), result.getMessage().contains("401"));
  }

  @Test
  public void mapsRestClientException403ToConfigException() {
    RestClientException cause = new RestClientException("forbidden", 403, 40301);
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof ConfigException);
    assertFalse(result instanceof RetriableException);
    assertTrue(result.getMessage(), result.getMessage().contains("403"));
  }

  @Test
  public void mapsRetriableStatusesToRetriable() {
    for (int status : new int[] {408, 429, 500, 502, 503, 504}) {
      RestClientException cause = new RestClientException("server error", status, 0);
      KafkaException result = BackupExceptionMapper.classify(OP, cause);
      assertTrue("status " + status + " should be retriable, got " + result.getClass(),
          result instanceof RetriableException);
    }
  }

  @Test
  public void mapsNonRetriableClientErrorsToDataException() {
    for (int status : new int[] {404, 409, 422}) {
      RestClientException cause = new RestClientException("client error", status, 0);
      KafkaException result = BackupExceptionMapper.classify(OP, cause);
      assertTrue("status " + status + " should be DataException, got " + result.getClass(),
          result instanceof DataException);
    }
  }

  @Test
  public void passesThroughConnectRetriableException() {
    RetriableException original = new RetriableException("already classified");
    KafkaException result = BackupExceptionMapper.classify(OP, original);
    assertSame(original, result);
  }

  @Test
  public void passesThroughDataException() {
    DataException original = new DataException("already classified");
    KafkaException result = BackupExceptionMapper.classify(OP, original);
    assertSame(original, result);
  }

  @Test
  public void passesThroughConfigException() {
    ConfigException original = new ConfigException("bad url");
    KafkaException result = BackupExceptionMapper.classify(OP, original);
    assertSame(original, result);
  }

  @Test
  public void mapsThrottlingQuotaExceededExceptionToRetriable() {
    ThrottlingQuotaExceededException cause =
        new ThrottlingQuotaExceededException("too many requests");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof RetriableException);
    assertSame(cause, result.getCause());
  }

  @Test
  public void mapsDisconnectExceptionToRetriable() {
    DisconnectException cause = new DisconnectException("bad gateway");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof RetriableException);
    assertSame(cause, result.getCause());
  }

  @Test
  public void mapsCommonNetworkExceptionToRetriable() {
    NetworkException cause = new NetworkException("connection reset");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof RetriableException);
    assertSame(cause, result.getCause());
  }

  @Test
  public void passesThroughUnknownKafkaException() {
    KafkaException original = new KafkaException("something bespoke");
    KafkaException result = BackupExceptionMapper.classify(OP, original);
    assertSame(original, result);
  }

  @Test
  public void idempotentOnDoubleClassifyOfTimeoutException() {
    TimeoutException cause = new TimeoutException("SR timed out");
    KafkaException first = BackupExceptionMapper.classify(OP, cause);
    KafkaException second = BackupExceptionMapper.classify(OP, first);
    assertSame(first, second);
    assertTrue(second instanceof RetriableException);
  }

  @Test(expected = IllegalStateException.class)
  public void rethrowsUnknownRuntimeExceptionAsIs() {
    BackupExceptionMapper.classify(OP, new IllegalStateException("unexpected"));
  }

  @Test(expected = NullPointerException.class)
  public void rethrowsUnknownNullPointerExceptionAsIs() {
    BackupExceptionMapper.classify(OP, new NullPointerException("nope"));
  }

  @Test
  public void wrapsCheckedThrowableInConnectException() {
    Exception cause = new Exception("some checked");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof ConnectException);
    assertSame(cause, result.getCause());
  }

  @Test
  public void messageFollowsRepoConvention() {
    IOException cause = new IOException("boom");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertEquals("Failed to " + OP, result.getMessage());
  }

  private static void assertMessage(KafkaException result) {
    assertTrue(result.getMessage(), result.getMessage().startsWith("Failed to " + OP));
  }
}
