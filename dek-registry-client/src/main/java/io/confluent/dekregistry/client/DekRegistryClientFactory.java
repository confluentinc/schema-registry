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

package io.confluent.dekregistry.client;

import java.util.List;
import java.util.Map;

public class DekRegistryClientFactory {

  public static DekRegistryClient newClient(
      List<String> baseUrls,
      int cacheCapacity,
      int cacheExpirySecs,
      Map<String, ?> configs,
      Map<String, String> httpHeaders) {
    String mockScope = MockDekRegistryClientFactory.validateAndMaybeGetMockScope(baseUrls);
    if (mockScope != null) {
      return MockDekRegistryClientFactory.getClientForScope(mockScope, configs);
    } else {
      return new CachedDekRegistryClient(
          baseUrls,
          cacheCapacity,
          cacheExpirySecs,
          configs,
          httpHeaders
      );
    }
  }
}
