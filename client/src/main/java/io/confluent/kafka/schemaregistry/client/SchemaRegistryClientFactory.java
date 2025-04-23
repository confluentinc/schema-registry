/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.util.List;
import java.util.Map;

public class SchemaRegistryClientFactory {

  public static SchemaRegistryClient newClient(
      List<String> baseUrls,
      int cacheCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    String mockScope = MockSchemaRegistry.validateAndMaybeGetMockScope(baseUrls);
    if (mockScope != null) {
      return MockSchemaRegistry.getClientForScope(mockScope, providers);
    } else {
      return new CachedSchemaRegistryClient(
          baseUrls,
          cacheCapacity,
          providers,
          configs,
          httpHeaders
      );
    }
  }
}
