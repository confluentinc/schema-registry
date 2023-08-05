/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.client.rest.entities;

import com.google.common.collect.EnumHashBiMap;
import java.util.Set;

// TODO remove
public enum KmsType {
  AWS("aws-kms"),
  AZURE("azure-kms"),
  GCP("gcp-kms"),
  HCVAULT("hcvault"),
  LOCAL("local-kms");

  private static final EnumHashBiMap<KmsType, String> lookup =
      EnumHashBiMap.create(KmsType.class);

  static {
    for (KmsType type : KmsType.values()) {
      lookup.put(type, type.symbol());
    }
  }

  private final String symbol;

  KmsType(String symbol) {
    this.symbol = symbol;
  }

  public String symbol() {
    return symbol;
  }

  public static KmsType get(String symbol) {
    return lookup.inverse().get(symbol);
  }

  public static Set<String> symbols() {
    return lookup.inverse().keySet();
  }

  @Override
  public String toString() {
    return symbol();
  }
}