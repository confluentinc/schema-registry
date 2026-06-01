/*
 * Copyright 2026 Confluent Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import java.lang.reflect.Field;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

public class AbstractSchemaRegistryNullEncoderTest {

  private static final String SCHEMA_STRING = "{\"type\":\"string\"}";

  @Test
  public void toSchemaEntity_skipsDecodeWhenEncoderIsNull() throws SchemaRegistryStoreException {
    // CALLS_REAL_METHODS with Mockito bypasses the constructor (Objenesis), so fields
    // — including metadataEncoder — remain at their default null value.
    AbstractSchemaRegistry registry = mock(AbstractSchemaRegistry.class, CALLS_REAL_METHODS);
    SchemaValue schemaValue = new SchemaValue(
        "subject-1", 1, 42, null, null, SCHEMA_STRING, false);

    Schema entity = registry.toSchemaEntity(schemaValue);

    assertEquals("subject-1", entity.getSubject());
    assertEquals(42, entity.getId());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void allVersions_skipsDecodeWhenEncoderIsNull() throws Exception {
    AbstractSchemaRegistry registry = mock(AbstractSchemaRegistry.class, CALLS_REAL_METHODS);
    SchemaValue schemaValue = new SchemaValue(
        "subject-1", 1, 42, null, null, SCHEMA_STRING, false);

    Store<SchemaRegistryKey, SchemaRegistryValue> store = mock(Store.class);
    when(store.getAll(any(), any())).thenReturn(singletonIterator(schemaValue));
    setField(registry, "store", store);

    Iterator<SchemaKey> versions = registry.getAllVersions("subject-1", LookupFilter.DEFAULT);

    assertNotNull(versions);
    assertEquals(1, versions.next().getVersion());
  }

  @Test
  public void getMetadataEncoder_returnsNullWhenUnwired() {
    AbstractSchemaRegistry registry = mock(AbstractSchemaRegistry.class, CALLS_REAL_METHODS);
    assertNull(registry.getMetadataEncoder());
  }

  private static CloseableIterator<SchemaRegistryValue> singletonIterator(SchemaRegistryValue v) {
    return new CloseableIterator<SchemaRegistryValue>() {
      private boolean consumed = false;

      @Override
      public boolean hasNext() {
        return !consumed;
      }

      @Override
      public SchemaRegistryValue next() {
        consumed = true;
        return v;
      }

      @Override
      public void close() {
      }
    };
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field f = AbstractSchemaRegistry.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
}
