/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.encryption.tink;

public enum DekFormat {
  AES128_GCM(false),
  AES256_GCM(false),
  AES256_SIV(true);

  private boolean isDeterministic;

  DekFormat(boolean isDeterministic) {
    this.isDeterministic = isDeterministic;
  }

  public boolean isDeterministic() {
    return isDeterministic;
  }
}