package io.confluent.kafka.schemaregistry.exceptions;

public class GarbageCollectionInitializationException extends GarbageCollectionException {
  public GarbageCollectionInitializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public GarbageCollectionInitializationException(String message) {
    super(message);
  }

  public GarbageCollectionInitializationException(Throwable cause) {
    super(cause);
  }

  public GarbageCollectionInitializationException() {
    super();
  }
}
