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
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.Test;

public class BackupExceptionMapperTest {

  private static final String OP = "wrap Avro backup metadata for topic orders";

  @Test
  public void passesThroughRetriableException() {
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
  public void mapsKafkaTimeoutExceptionToRetriable() {
    TimeoutException cause = new TimeoutException("SR timed out");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof RetriableException);
    assertSame(cause, result.getCause());
    assertMessage(result);
  }

  @Test
  public void mapsSerializationExceptionWithNetworkCauseToNetworkException() {
    SerializationException cause =
        new SerializationException("SR unreachable", new SocketTimeoutException("connect timed out"));
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
    assertTrue(result.getMessage().contains("bad url"));
  }

  @Test
  public void mapsPlainIoExceptionToDataException() {
    IOException cause = new IOException("connection reset");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof DataException);
    assertSame(cause, result.getCause());
  }

  @Test
  public void mapsInterruptedIoExceptionToDataException() {
    InterruptedIOException cause = new InterruptedIOException("read interrupted");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof DataException);
    assertSame(cause, result.getCause());
  }

  @Test
  public void mapsEofExceptionToDataException() {
    EOFException cause = new EOFException("stream closed");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof DataException);
    assertSame(cause, result.getCause());
  }

  @Test
  public void mapsSocketExceptionToNetworkException() {
    SocketException cause = new SocketException("connection refused");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof NetworkException);
    assertSame(cause, result.getCause());
    assertTrue(result.getMessage(), result.getMessage().contains("Network connection error"));
    assertTrue(result.getMessage(), result.getMessage().contains("connection refused"));
  }

  @Test
  public void mapsSocketTimeoutExceptionToNetworkException() {
    SocketTimeoutException cause = new SocketTimeoutException("read timed out");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof NetworkException);
    assertSame(cause, result.getCause());
    assertTrue(result.getMessage(), result.getMessage().contains("Network connection error"));
    assertTrue(result.getMessage(), result.getMessage().contains("read timed out"));
  }

  @Test
  public void mapsRestClientException401ToConnectException() {
    RestClientException cause = new RestClientException("unauthorized", 401, 40101);
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof ConnectException);
    assertTrue(result.getMessage().contains("authentication"));
    assertTrue(result.getMessage().contains("401"));
    assertSame(cause, result.getCause());
  }

  @Test
  public void mapsRestClientException403ToConnectException() {
    RestClientException cause = new RestClientException("forbidden", 403, 40301);
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof ConnectException);
    assertTrue(result.getMessage().contains("403"));
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
  public void mapsUnknownExceptionToConnectException() {
    IllegalStateException cause = new IllegalStateException("unexpected");
    KafkaException result = BackupExceptionMapper.classify(OP, cause);
    assertTrue(result instanceof ConnectException);
    assertTrue(result.getMessage().contains("unclassified"));
    assertTrue(result.getMessage().contains("IllegalStateException"));
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
