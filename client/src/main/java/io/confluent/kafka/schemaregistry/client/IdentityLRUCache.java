/**
 * Copyright 2018 Confluent Inc.
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
 **/

package io.confluent.kafka.schemaregistry.client;

import org.apache.kafka.common.cache.Cache;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A cache implementing a least recently used policy based on the identity of the keys rather than
 * normal equality.
 */
public class IdentityLRUCache<K, V> implements Cache<K, V> {
  private final LinkedHashMap<Identity<K>, V> cache;

  public IdentityLRUCache(final int maxSize) {
    cache = new LinkedHashMap<Identity<K>, V>(16, .75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<Identity<K>, V> eldest) {
        return this.size() > maxSize;
      }
    };
  }

  @Override
  public V get(K key) {
    return cache.get(new Identity<>(key));
  }

  @Override
  public void put(K key, V value) {
    cache.put(new Identity<>(key), value);
  }

  @Override
  public boolean remove(K key) {
    return cache.remove(new Identity<>(key)) != null;
  }

  @Override
  public long size() {
    return cache.size();
  }

  public Collection<V> values() {
    return cache.values();
  }

  private static class Identity<K> {
    private final K object;

    public Identity(K object) {
      this.object = object;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Identity<?> identity = (Identity<?>) o;
      return object == identity.object;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(object);
    }
  }
}
