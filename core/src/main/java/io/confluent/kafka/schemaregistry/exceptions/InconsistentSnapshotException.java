package io.confluent.kafka.schemaregistry.exceptions;

public class InconsistentSnapshotException extends GarbageCollectionException {
  public InconsistentSnapshotException(String name, Throwable cause) {
    super(name, cause);
  }

  public InconsistentSnapshotException(String name) {
    super(name);
  }

  public InconsistentSnapshotException(Throwable cause) {
    super(cause);
  }

  public InconsistentSnapshotException() {
    super();
  }
}
