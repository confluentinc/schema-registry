/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.testutil;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.junit.Assert;
import org.junit.Test;

public class MockSchemaRegistryTest {
    @Test
    public void testGetClient() {
        final SchemaRegistryClient client = MockSchemaRegistry.getClientForScope("testGetClient");
        Assert.assertNotNull(client);
    }

    @Test
    public void testGetSameClient() {
        final String scope = "testGetSameClient";
        final SchemaRegistryClient client = MockSchemaRegistry.getClientForScope(scope);
        Assert.assertNotNull(client);
        Assert.assertSame(client, MockSchemaRegistry.getClientForScope(scope));
    }

    @Test
    public void testDropScope() {
        final String scope = "testDropScope";
        final SchemaRegistryClient client = MockSchemaRegistry.getClientForScope(scope);
        Assert.assertNotNull(client);
        MockSchemaRegistry.dropScope(scope);
        Assert.assertNotSame(client, MockSchemaRegistry.getClientForScope(scope));
    }
}
