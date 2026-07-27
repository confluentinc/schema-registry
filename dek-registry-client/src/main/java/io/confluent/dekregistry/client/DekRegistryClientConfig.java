/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.dekregistry.client;

import java.util.Map;

public class DekRegistryClientConfig {

  public static final String MISSING_KEK_CACHE_TTL_CONFIG = "missing.kek.cache.ttl.sec";
  public static final String MISSING_DEK_CACHE_TTL_CONFIG = "missing.dek.cache.ttl.sec";

  public static long getMissingKekTTL(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MISSING_KEK_CACHE_TTL_CONFIG)
        ? Long.parseLong(configs.get(MISSING_KEK_CACHE_TTL_CONFIG).toString())
        : 0L;
  }

  public static long getMissingDekTTL(Map<String, ?> configs) {
    return configs != null && configs.containsKey(MISSING_DEK_CACHE_TTL_CONFIG)
        ? Long.parseLong(configs.get(MISSING_DEK_CACHE_TTL_CONFIG).toString())
        : 0L;
  }
}
