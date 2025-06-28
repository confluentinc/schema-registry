/*
 * Copyright 2024 Confluent Inc.
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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import java.util.Iterator;

public class FilteredIterator<T> implements CloseableIterator<T> {
  final CloseableIterator<T> backingIterator;
  final Iterator<T> filteredIterator;

  public static <T> CloseableIterator<T> filter(
      final CloseableIterator<T> fromIterator, final Predicate<? super T> predicate) {
    return new FilteredIterator<T>(fromIterator, predicate);
  }

  FilteredIterator(CloseableIterator<T> backingIterator, Predicate<? super T> predicate) {
    this.backingIterator = backingIterator;
    this.filteredIterator = Iterators.filter(backingIterator, predicate);
  }

  @Override
  public final boolean hasNext() {
    return filteredIterator.hasNext();
  }

  @Override
  public final T next() {
    return filteredIterator.next();
  }

  @Override
  public final void remove() {
    filteredIterator.remove();
  }

  @Override
  public final void close() {
    backingIterator.close();
  }
}