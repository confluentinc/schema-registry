/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.storage.exceptions;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;

public class InvalidKeyException extends SchemaRegistryException {

  public InvalidKeyException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidKeyException(String message) {
    super(message);
  }

  public InvalidKeyException(Throwable cause) {
    super(cause);
  }

  public InvalidKeyException() {
    super();
  }
}
