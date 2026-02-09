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

package io.confluent.kafka.schemaregistry.storage.garbagecollection;

import io.confluent.kafka.schemaregistry.exceptions.GarbageCollectionException;

import java.io.Closeable;
import java.util.Set;

public interface GarbageCollector extends Closeable {
  void init() throws GarbageCollectionException;

  void processDeletedResource(String tenant, String resourceId) throws GarbageCollectionException;

  void processDeletedResourceNamespace(String tenant, String resourceNamespace)
          throws GarbageCollectionException;

  void processResourceSnapshot(String tenant, Set<String> resourceIds, long cutOffTimestamp)
          throws GarbageCollectionException;

  void processResourceNamespaceSnapshot(String tenant, Set<String> resourceNamespaces,
                                        long cutOffTimestamp)
          throws GarbageCollectionException;
}
