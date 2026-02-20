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

package io.confluent.kafka.schemaregistry.storage.garbagecollection;

import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.exceptions.GarbageCollectionException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.AssociationKey;
import io.confluent.kafka.schemaregistry.storage.AssociationValue;
import io.confluent.kafka.schemaregistry.storage.CloseableIterator;
import io.confluent.kafka.schemaregistry.storage.KafkaStore;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class AssociationKafkaGarbageCollector implements GarbageCollector {
  private SchemaRegistry schemaRegistry;
  private KafkaStore kafkaStore;
  private static final String TOPIC = "topic";
  private static final Boolean TRUE = Boolean.TRUE;
  private static final Boolean FALSE = Boolean.FALSE;
  private static final Logger log = LoggerFactory.getLogger(AssociationKafkaGarbageCollector.class);
  ThreadLocal<String> tenantId = new ThreadLocal<>();

  public AssociationKafkaGarbageCollector(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
    this.kafkaStore = schemaRegistry.getKafkaStore();
  }

  @Override
  public void init() throws GarbageCollectionException {
  }

  @Override
  public void processDeletedResource(String tenant, String resourceId)
          throws GarbageCollectionException {
    log.info("Processing resource garbage collection for tenant {}, resourceId {}",
            tenant, resourceId);
    try {
      tenantId.set(tenant);
      // Use schemaRegistry.deleteAssociationsOrForward which has lock
      List<Association> oldAssociations = schemaRegistry.getAssociationsByResourceId(
              resourceId, TOPIC, null, null);
      if (oldAssociations.isEmpty()) {
        return;
      }
      String unqualifiedSubject = oldAssociations.get(0).getSubject();
      QualifiedSubject qs =
              QualifiedSubject.createFromUnqualified(tenant, unqualifiedSubject);
      String qualifiedSubject = qs.toQualifiedSubject();
      schemaRegistry.deleteAssociationsOrForward(qualifiedSubject,
              resourceId, TOPIC, null, TRUE, FALSE,
              null);
    } catch (SchemaRegistryException e) {
      throw new GarbageCollectionException(String.format(
              "Failed to garbage collect resourceId %s from tenant %s.",
              resourceId, tenant), e);
    } finally {
      tenantId.remove();
    }
  }

  @Override
  public void processDeletedResourceNamespace(String tenant, String resourceNamespace)
          throws GarbageCollectionException {
    log.info("Processing resource namespace garbage collection for tenant {}, kafka cluster id {}",
            tenant, resourceNamespace);
    // Scan the whole table for this tenant
    List<String> resourceIdsToDelete = new LinkedList<>();
    try (CloseableIterator<AssociationValue> iter = getAllForTenant(tenant)) {
      while (iter.hasNext()) {
        AssociationValue value = iter.next();
        if (value.getResourceNamespace().equals(resourceNamespace)) {
          resourceIdsToDelete.add(value.getResourceId());
        }
      }
    } catch (StoreException e) {
      throw new GarbageCollectionException(
          String.format("Error while getting all association values for tenant %s "
              + "from the backing Kafka store when processing deleted cluster id %s",
              tenant, resourceNamespace), e);
    }
    for (String resourceId : resourceIdsToDelete) {
      processDeletedResource(tenant, resourceId);
    }
    log.info("Processed deleted kafka cluster for tenant {}, cluster id: {}",
            tenant, resourceNamespace);
  }

  @Override
  public void processResourceSnapshot(String tenant, Set<String> resourceIds, long cutOffTimestamp)
          throws GarbageCollectionException {
    log.info("Processing topic snapshot garbage collection for tenant {}", tenant);
    List<String> resourceIdsToDelete = new LinkedList<>();
    try (CloseableIterator<AssociationValue> iter = getAllForTenant(tenant)) {
      while (iter.hasNext()) {
        AssociationValue value = iter.next();
        // FIXME change the timestamp to createTs after SR supports it
        if (!resourceIds.contains(value.getResourceId())
            && value.getTimestamp() <= cutOffTimestamp) {
          resourceIdsToDelete.add(value.getResourceId());
        }
      }
    } catch (StoreException e) {
      throw new GarbageCollectionException(
          String.format("Error while getting all association values for tenant %s "
              + "from the backing Kafka store when processing topic snapshot", tenant), e);
    }
    for (String resourceId : resourceIdsToDelete) {
      processDeletedResource(tenant, resourceId);
    }
  }

  @Override
  public void processResourceNamespaceSnapshot(String tenant, Set<String> resourceNamespaces,
                                               long cutOffTimestamp)
          throws GarbageCollectionException {
    log.info("Processing kafka cluster snapshot garbage collection for tenant {}", tenant);
    List<String> resourceIdsToDelete = new LinkedList<>();
    try (CloseableIterator<AssociationValue> iter = getAllForTenant(tenant)) {
      if (iter.hasNext()) {
        AssociationValue value = iter.next();
        // FIXME change the timestamp to createTs after SR supports it
        if (!resourceNamespaces.contains(value.getResourceNamespace())
                && value.getTimestamp() <= cutOffTimestamp) {
          resourceIdsToDelete.add(value.getResourceId());
        }
      }
    } catch (StoreException e) {
      throw new GarbageCollectionException(
          String.format("Error while getting all association values for tenant %s "
              + "from the backing Kafka store when processing kafka cluster snapshot",
              tenant), e);
    }
    for (String resourceId : resourceIdsToDelete) {
      processDeletedResource(tenant, resourceId);
    }
  }

  @Override
  public void close() throws IOException {
  }

  private CloseableIterator<AssociationValue> getAllForTenant(String tenant) throws StoreException {
    String minResourceName = String.valueOf(Character.MIN_VALUE);
    String maxResourceName = String.valueOf(Character.MAX_VALUE);
    String minResourceNamespace = String.valueOf(Character.MIN_VALUE);
    String maxResourceNamespace = String.valueOf(Character.MAX_VALUE);
    String minResourceType = String.valueOf(Character.MIN_VALUE);
    String maxResourceType = String.valueOf(Character.MAX_VALUE);
    String minAssociationType = String.valueOf(Character.MIN_VALUE);
    String maxAssociationType = String.valueOf(Character.MAX_VALUE);
    String minSubject = String.valueOf(Character.MIN_VALUE);
    String maxSubject = String.valueOf(Character.MAX_VALUE);

    AssociationKey key1 = new AssociationKey(tenant, minResourceName, minResourceNamespace,
            minResourceType, minAssociationType, minSubject);
    AssociationKey key2 = new AssociationKey(tenant, maxResourceName, maxResourceNamespace,
            maxResourceType, maxAssociationType, maxSubject);

    return kafkaStore.getAll(key1, key2);
  }
}
