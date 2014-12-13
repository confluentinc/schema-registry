package io.confluent.kafka.schemaregistry.storage.exceptions;

public class StoreException extends Exception {

  public StoreException(String message, Throwable cause) {
    super(message, cause);
  }

  public StoreException(String message) {
    super(message);
  }

  public StoreException(Throwable cause) {
    super(cause);
  }

  public StoreException() {
    super();
  }
}
