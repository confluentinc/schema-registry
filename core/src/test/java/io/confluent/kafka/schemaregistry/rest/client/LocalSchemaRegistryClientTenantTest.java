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

package io.confluent.kafka.schemaregistry.rest.client;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LocalSchemaRegistryClientTenantTest {

  private static final String QUALIFIED_SUBJECT = "mytenant_subject1";

  @Mock
  private KafkaSchemaRegistry schemaRegistry;

  private LocalSchemaRegistryClient client;

  @Before
  public void setUp() {
    when(schemaRegistry.tenant()).thenReturn("mytenant");
    client = new LocalSchemaRegistryClient(schemaRegistry);
  }

  @Test
  public void testDeleteSubject() throws Exception {
    when(schemaRegistry.hasSubjects(QUALIFIED_SUBJECT, true)).thenReturn(true);
    when(schemaRegistry.hasSubjects(QUALIFIED_SUBJECT, false)).thenReturn(true);
    when(schemaRegistry.deleteSubject(QUALIFIED_SUBJECT, false))
        .thenReturn(Collections.singletonList(1));

    assertEquals(Collections.singletonList(1), client.deleteSubject("subject1", false));
  }

  @Test
  public void testPermanentDeleteSubject() throws Exception {
    when(schemaRegistry.hasSubjects(QUALIFIED_SUBJECT, true)).thenReturn(true);
    when(schemaRegistry.deleteSubject(QUALIFIED_SUBJECT, true))
        .thenReturn(Collections.singletonList(1));

    assertEquals(Collections.singletonList(1), client.deleteSubject("subject1", true));
  }
}
