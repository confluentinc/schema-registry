package io.confluent.kafka.schemaregistry.storage.exceptions;

public class SchemaRegistryException extends Exception {

  public SchemaRegistryException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaRegistryException(String message) {
    super(message);
  }

  public SchemaRegistryException(Throwable cause) {
    super(cause);
  }

  public SchemaRegistryException() {
    super();
  }
}
