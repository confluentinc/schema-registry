package io.confluent.kafka.schemaregistry.storage.exceptions;

/**
 * Error while initializing a <code>io.confluent.kafka.schemaregistry.storage.Store</code>
 * @author nnarkhed
 *
 */
public class StoreInitializationException extends Exception {

  public StoreInitializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public StoreInitializationException(String message) {
    super(message);
  }

  public StoreInitializationException(Throwable cause) {
    super(cause);
  }

  public StoreInitializationException() {
    super();
  }
}
