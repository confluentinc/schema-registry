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

package io.confluent.kafka.schemaregistry.type.logical.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.kafka.schemaregistry.type.logical.avro.AvroTypeNames.Kind;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AvroTypeNamesTest {

  // A body serializes the type's whole subtree, so only names that collide compare them.
  @Test
  void testUniqueNamesAreNotSerialized() {
    AtomicInteger serialized = new AtomicInteger();
    AvroTypeNames names = new AvroTypeNames();
    for (Kind kind : Kind.values()) {
      names.allocate("t_" + kind, kind);
      names.attach(body(serialized, kind.name()));
    }
    assertNull(names.forSuffixing());
    assertEquals(0, serialized.get());

    names.allocate("t_" + Kind.PINNED, Kind.DERIVED);
    names.attach(body(serialized, "other"));
    assertNotNull(names.forSuffixing());
    assertEquals(2, serialized.get());
  }

  private static Object body(AtomicInteger serialized, String text) {
    return new Object() {
      @Override
      public String toString() {
        serialized.incrementAndGet();
        return text;
      }
    };
  }
}
