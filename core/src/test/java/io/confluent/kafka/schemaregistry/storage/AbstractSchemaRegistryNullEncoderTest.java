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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import org.junit.jupiter.api.Test;

public class AbstractSchemaRegistryNullEncoderTest {

  private static final String SCHEMA_STRING = "{\"type\":\"string\"}";

  @Test
  public void toSchemaEntity_skipsDecodeWhenEncoderIsNull() throws SchemaRegistryStoreException {
    AbstractSchemaRegistry registry = mock(AbstractSchemaRegistry.class, CALLS_REAL_METHODS);
    SchemaValue schemaValue = new SchemaValue(
        "subject-1", 1, 42, null, null, SCHEMA_STRING, false);

    Schema entity = registry.toSchemaEntity(schemaValue);

    assertEquals("subject-1", entity.getSubject());
    assertEquals(42, entity.getId());
  }

  @Test
  public void getMetadataEncoder_returnsNullWhenUnwired() {
    AbstractSchemaRegistry registry = mock(AbstractSchemaRegistry.class, CALLS_REAL_METHODS);
    assertNull(registry.getMetadataEncoder());
  }
}
