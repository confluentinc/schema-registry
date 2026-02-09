package io.confluent.kafka.schemaregistry.exceptions;

public class InconsistentSnapshotPageException extends GarbageCollectionException{
  public InconsistentSnapshotPageException(String name, Throwable cause) {
    super(name, cause);
  }

  public InconsistentSnapshotPageException(String name) {
    super(name);
  }

  public InconsistentSnapshotPageException(Throwable cause) {
    super(cause);
  }

  public InconsistentSnapshotPageException() {
    super();
  }
}
