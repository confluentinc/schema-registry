/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.storage.exceptions;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;

public class TooManyKeysException extends SchemaRegistryException {

  public TooManyKeysException(String message, Throwable cause) {
    super(message, cause);
  }

  public TooManyKeysException(String message) {
    super(message);
  }

  public TooManyKeysException(Throwable cause) {
    super(cause);
  }

  public TooManyKeysException() {
    super();
  }
}
