/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.storage.exceptions;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;

public class AlreadyExistsException extends SchemaRegistryException {

  public AlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  public AlreadyExistsException(String message) {
    super(message);
  }

  public AlreadyExistsException(Throwable cause) {
    super(cause);
  }

  public AlreadyExistsException() {
    super();
  }
}
