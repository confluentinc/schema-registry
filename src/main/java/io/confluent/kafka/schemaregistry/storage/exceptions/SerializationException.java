package io.confluent.kafka.schemaregistry.storage.exceptions;

public class SerializationException extends Exception {

  public SerializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public SerializationException(String message) {
    super(message);
  }

  public SerializationException(Throwable cause) {
    super(cause);
  }

  public SerializationException() {
    super();
  }
}
