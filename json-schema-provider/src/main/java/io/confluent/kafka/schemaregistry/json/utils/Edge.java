/*
 * Copyright 2014-2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.json.utils;

import java.util.Objects;

public class Edge<V, T> {

  private final V source;
  private final V target;
  private final T value;

  public Edge(V source, V target, T value) {
    this.source = source;
    this.target = target;
    this.value = value;
  }

  public Edge<V, T> reverse() {
    return new Edge<>(target(), source(), value());
  }

  public V source() {
    return source;
  }

  public V target() {
    return target;
  }

  public T value() {
    return value;
  }

  public String toString() {
    return "Edge{src=" + source + ",tgt=" + target + ",val=" + value + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Edge<?, ?> that = (Edge<?, ?>) o;
    return Objects.equals(source, that.source)
        && Objects.equals(target, that.target)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, target, value);
  }
}
