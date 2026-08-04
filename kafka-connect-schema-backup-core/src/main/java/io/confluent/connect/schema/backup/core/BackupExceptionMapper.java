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

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.ExceptionUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;

import java.io.IOException;

/**
 * Classifies backup and restore exceptions into the correct Connect type
 * so transient failures are retried and permanent ones fail fast.
 */
public final class BackupExceptionMapper {

  private BackupExceptionMapper() {
  }

  public static KafkaException classify(String op, Throwable cause) {
    String msg = String.format("Failed to %s", op);

    if (cause instanceof RetriableException) {
      return (RetriableException) cause;
    }
    if (cause instanceof DataException) {
      return (DataException) cause;
    }

    if (cause instanceof TimeoutException) {
      return new RetriableException(msg, cause);
    }
    if (cause instanceof SerializationException) {
      if (ExceptionUtils.isNetworkConnectionException(cause.getCause())) {
        return new NetworkException(networkMsg(op, cause.getCause()), cause);
      }
      return new DataException(msg, cause);
    }
    if (cause instanceof InvalidConfigurationException) {
      return new ConfigException(String.format("%s: %s", msg, cause.getMessage()));
    }

    if (cause instanceof IOException) {
      if (ExceptionUtils.isNetworkConnectionException(cause)) {
        return new NetworkException(networkMsg(op, cause), cause);
      }
      return new DataException(msg, cause);
    }

    if (cause instanceof RestClientException) {
      RestClientException rce = (RestClientException) cause;
      int status = rce.getStatus();
      if (status == 401 || status == 403) {
        return new ConnectException(String.format(
            "%s: Schema Registry authentication error (status %d)", msg, status), cause);
      }
      if (RestService.isRestClientExceptionRetriable(rce)) {
        return new RetriableException(msg, cause);
      }
      return new DataException(msg, cause);
    }

    return new ConnectException(
        String.format("%s: unclassified %s", msg, cause.getClass().getName()), cause);
  }

  private static String networkMsg(String op, Throwable networkCause) {
    return String.format("Network connection error during %s: %s",
        op, networkCause.getMessage());
  }
}
