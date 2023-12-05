/*
 * Copyright 2018 Confluent Inc.
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

import static io.confluent.kafka.schemaregistry.storage.SchemaRegistry.DEFAULT_TENANT;

import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;

/**
 * Internal interface that provides various indexed methods that help lookup the underlying schemas.
 * The interface has also callback methods for various schema lifecycle events like register,
 * delete, etc. It is important to note that these callbacks block the corresponding API that
 * lead to the callback. Hence sufficient care must be taken to ensure that the callbacks are
 * light weight.
 *
 * @param <K> key of the store
 * @param <V> value of the store
 */
public interface LookupCache<K,V> extends Store<K,V> {

  /**
   * Provides SchemaIdAndSubjects associated with the schema.
   *
   * @param schema schema object; never {@code null}
   * @return the schema id and subjects associated with the schema, null otherwise.
   */
  SchemaIdAndSubjects schemaIdAndSubjects(Schema schema) throws StoreException;

  /**
   * Checks if a schema is registered in any subject.
   *
   * @param schema schema object
   * @return true if the schema is already registered else false
   */
  boolean containsSchema(Schema schema) throws StoreException;

  /**
   * Returns schemas that reference the given schema.
   *
   * @param schema schema object
   * @return the ids of schemas that reference the given schema
   */
  Set<Integer> referencesSchema(SchemaKey schema) throws StoreException;

  /**
   * Provides the {@link SchemaKey} for the provided schema id.
   *
   * @param id the schema id; never {@code null}
   * @param subject the qualified context or subject
   * @return the {@link SchemaKey} if found, otherwise null.
   */
  SchemaKey schemaKeyById(Integer id, String subject) throws StoreException;

  /**
   * Callback that is invoked when a schema is registered.
   * This can be used to update any internal data structure.
   * This is invoked synchronously during register.
   *
   * @param schemaKey   the registered SchemaKey; never {@code null}
   * @param schemaValue the registered SchemaValue; never {@code null}
   * @param oldSchemaValue the previous SchemaValue
   */
  void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue, SchemaValue oldSchemaValue);

  /**
   * Callback that is invoked when a schema is deleted.
   * This can be used to update any internal data structure.
   * This is invoked synchronously during delete.
   *
   * @param schemaKey   the deleted SchemaKey; never {@code null}
   * @param schemaValue the deleted SchemaValue; never {@code null}
   * @param oldSchemaValue the previous SchemaValue
   */
  void schemaDeleted(SchemaKey schemaKey, SchemaValue schemaValue, SchemaValue oldSchemaValue);

  /**
   * Callback that is invoked when a schema is tombstoned.
   *
   * @param schemaKey   the tombstoned SchemaKey; never {@code null}
   * @param schemaValue the tombstoned SchemaValue
   */
  void schemaTombstoned(SchemaKey schemaKey, SchemaValue schemaValue);

  /**
   * Retrieves the config for a subject.
   *
   * @param subject the subject
   * @param returnTopLevelIfNotFound whether to return the top level scope if not found
   * @param defaultForTopLevel default value for the top level scope
   * @return the compatibility level if found, otherwise null
   */
  CompatibilityLevel compatibilityLevel(String subject,
                                        boolean returnTopLevelIfNotFound,
                                        CompatibilityLevel defaultForTopLevel)
      throws StoreException;

  /**
   * Retrieves the mode for a subject.
   *
   * @param subject the subject
   * @param returnTopLevelIfNotFound whether to return the top level scope if not found
   * @param defaultForTopLevel default value for the top level scope
   * @return the mode if found, otherwise null.
   */
  Mode mode(String subject, boolean returnTopLevelIfNotFound, Mode defaultForTopLevel)
      throws StoreException;

  /**
   * Returns subjects that have schemas (that are not deleted) that match the given subject.
   *
   * @param subject the subject, or null for all subjects
   * @return the subjects with matching schemas
   */
  Set<String> subjects(String subject, boolean lookupDeletedSubjects) throws StoreException;

  /**
   * Returns whether there exist schemas (that are not deleted) that match the given subject.
   *
   * @param subject the subject, or null for all subjects
   * @return whether there exist matching schemas
   */
  boolean hasSubjects(String subject, boolean lookupDeletedSubjects) throws StoreException;

  /**
   * Clears the cache of deleted schemas that match the given subject.
   *
   * @param subject the subject, or null for all subjects
   * @return the number of schemas cleared by schema type
   */
  Map<String, Integer> clearSubjects(String subject) throws StoreException;

  default String tenant() {
    return DEFAULT_TENANT;
  }

  /**
   * Can be used by subclasses to implement multi-tenancy
   *
   * @param tenant the tenant
   */
  default void setTenant(String tenant) {
  }
}
