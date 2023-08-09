/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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