/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.schema.backup.core;

import com.google.common.cache.CacheBuilder;

import java.util.Map;

/**
 * Size-bounded caches for the backup and restore paths.
 */
public final class BackupCaches {

  /**
   * Default size, the same as {@code schemas.cache.config}.
   */
  public static final int MAX_ENTRIES = 1000;

  private BackupCaches() {
  }

  /**
   * Thread-safe map holding at most {@link #MAX_ENTRIES} entries.
   */
  public static <K, V> Map<K, V> newBoundedMap() {
    return newBoundedMap(MAX_ENTRIES);
  }

  static <K, V> Map<K, V> newBoundedMap(int maxEntries) {
    return CacheBuilder.newBuilder().maximumSize(maxEntries).<K, V>build().asMap();
  }
}
