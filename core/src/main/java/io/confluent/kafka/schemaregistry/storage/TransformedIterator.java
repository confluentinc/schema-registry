/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import java.util.function.Function;

public abstract class TransformedIterator<F, T> implements CloseableIterator<T> {
  final CloseableIterator<? extends F> backingIterator;

  public static <F, T> CloseableIterator<T> transform(
      final CloseableIterator<F> fromIterator, final Function<? super F, ? extends T> function) {
    return new TransformedIterator<F, T>(fromIterator) {
      @Override
      T transform(F from) {
        return function.apply(from);
      }
    };
  }

  TransformedIterator(CloseableIterator<? extends F> backingIterator) {
    this.backingIterator = backingIterator;
  }

  abstract T transform(F from);

  @Override
  public final boolean hasNext() {
    return backingIterator.hasNext();
  }

  @Override
  public final T next() {
    return transform(backingIterator.next());
  }

  @Override
  public final void remove() {
    backingIterator.remove();
  }

  @Override
  public final void close() {
    backingIterator.close();
  }
}