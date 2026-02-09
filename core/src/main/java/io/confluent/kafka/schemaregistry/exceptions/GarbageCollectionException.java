package io.confluent.kafka.schemaregistry.exceptions;

public class GarbageCollectionException extends SchemaRegistryException {
  public GarbageCollectionException(String message, Throwable cause) {
    super(message, cause);
  }

  public GarbageCollectionException(String message) {
    super(message);
  }

  public GarbageCollectionException(Throwable cause) {
    super(cause);
  }

  public GarbageCollectionException() {
    super();
  }
}
