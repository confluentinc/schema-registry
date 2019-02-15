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

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;

import java.util.List;

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
  SchemaIdAndSubjects schemaIdAndSubjects(Schema schema);

  /**
   * Checks is a schema is registered in any subject.
   *
   * @param schema schema object
   * @return true if the schema is already registered else false
   */
  boolean containsSchema(Schema schema);

  /**
   * Provides the {@link SchemaKey} for the provided schema id.
   *
   * @param id the schema id; never {@code null}
   * @return the {@link SchemaKey} if found, otherwise null.
   */
  SchemaKey schemaKeyById(Integer id);

  /**
   * Provides the list of {@link SchemaKey} that have been deleted for the registered {SchemaValue}
   *
   * @param schemaValue the SchemaValue being registered; never {@code null}
   * @return the list of {@link SchemaKey} that have been marked to be deleted, can be null or empty
   */
  List<SchemaKey> deletedSchemaKeys(SchemaValue schemaValue);

  /**
   * Callback that is invoked when a schema is registered.
   * This can be used to update any internal data structure.
   * This is invoked synchronously during register.
   *
   * @param schemaKey   the registered SchemaKey; never {@code null}
   * @param schemaValue the registered SchemaValue; never {@code null}
   */
  void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue);

  /**
   * Callback that is invoked when a schema is deleted.
   * This can be used to update any internal data structure.
   * This is invoked synchronously during delete.
   *
   * @param schemaKey   the deleted SchemaKey; never {@code null}
   * @param schemaValue the deleted SchemaValue; never {@code null}
   */
  void schemaDeleted(SchemaKey schemaKey, SchemaValue schemaValue);
}
