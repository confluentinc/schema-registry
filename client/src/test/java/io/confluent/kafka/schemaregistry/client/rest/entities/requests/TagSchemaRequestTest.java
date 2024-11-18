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

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTags;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TagSchemaRequestTest {

  @Test
  public void testMergeSchemaTags() {
    SchemaEntity entity1 =
        new SchemaEntity("SampleRecord.f1", SchemaEntity.EntityType.SR_FIELD);
    SchemaEntity entity2 =
        new SchemaEntity(".SampleRecord.f1", SchemaEntity.EntityType.SR_FIELD);
    SchemaTags tag1 = new SchemaTags(entity1, Collections.singletonList("tag1"));
    SchemaTags tag2 = new SchemaTags(entity2, Collections.singletonList("tag2"));

    Map<SchemaEntity, Set<String>> result =
        TagSchemaRequest.schemaTagsListToMap(Arrays.asList(tag1, tag2));
    assertEquals(1, result.size());
    assertTrue(result.containsKey(entity1));
    assertTrue(result.containsKey(entity2));
    assertEquals(ImmutableSet.of("tag1", "tag2"), result.get(entity1));
  }

  @Test
  public void testMergeSchemaTagsOrder() {
    SchemaEntity entity1 =
        new SchemaEntity("com.example.SampleRecord.f1", SchemaEntity.EntityType.SR_FIELD);
    SchemaEntity entity2 =
        new SchemaEntity("SampleRecord.f1", SchemaEntity.EntityType.SR_FIELD);
    SchemaEntity entity3 =
        new SchemaEntity("SampleRecord.f2", SchemaEntity.EntityType.SR_FIELD);
    SchemaTags tag1 = new SchemaTags(entity1, Collections.singletonList("tag1"));
    SchemaTags tag2 = new SchemaTags(entity2, Collections.singletonList("tag2"));
    SchemaTags tag3 = new SchemaTags(entity3, Collections.singletonList("tag3"));

    Map<SchemaEntity, Set<String>> result =
        TagSchemaRequest.schemaTagsListToMap(Arrays.asList(tag1, tag2, tag3));
    assertEquals(3, result.size());
    Iterator<SchemaEntity> mapIter = result.keySet().iterator();
    assertEquals("com.example.SampleRecord.f1", mapIter.next().getEntityPath());
    assertEquals("SampleRecord.f1", mapIter.next().getEntityPath());
    assertEquals("SampleRecord.f2", mapIter.next().getEntityPath());
  }
}
