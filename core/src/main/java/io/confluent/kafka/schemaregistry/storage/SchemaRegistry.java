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

import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import java.util.function.Predicate;

public interface SchemaRegistry extends SchemaVersionFetcher {

  String DEFAULT_TENANT = QualifiedSubject.DEFAULT_TENANT;

  void init() throws SchemaRegistryException;

  Set<String> schemaTypes();

  default Schema register(String subject, Schema schema)
      throws SchemaRegistryException {
    return register(subject, schema, false);
  }

  default Schema register(String subject, Schema schema, boolean normalize)
      throws SchemaRegistryException {
    return register(subject, schema, normalize, false);
  }

  Schema register(String subject, Schema schema, boolean normalize, boolean propagateSchemaTags)
      throws SchemaRegistryException;

  default Schema getByVersion(String subject, int version, boolean returnDeletedSchema) {
    try {
      return get(subject, version, returnDeletedSchema);
    } catch (SchemaRegistryException e) {
      throw new RuntimeException(e);
    }
  }

  Schema get(String subject, int version, boolean returnDeletedSchema)
      throws SchemaRegistryException;

  SchemaString get(int id, String subject) throws SchemaRegistryException;

  default Set<String> listSubjects() throws SchemaRegistryException {
    return listSubjects(LookupFilter.DEFAULT);
  }

  Set<String> listSubjects(LookupFilter filter)
          throws SchemaRegistryException;

  Set<String> listSubjectsForId(int id, String subject, boolean returnDeleted)
      throws SchemaRegistryException;

  Iterator<SchemaKey> getAllVersions(String subject, LookupFilter filter)
      throws SchemaRegistryException;

  Iterator<ExtendedSchema> getVersionsWithSubjectPrefix(
      String prefix, boolean includeAliases, LookupFilter filter,
      boolean latestOnly, Predicate<Schema> postFilter)
      throws SchemaRegistryException;

  Schema getLatestVersion(String subject) throws SchemaRegistryException;

  List<Integer> deleteSubject(String subject, boolean permanentDelete)
      throws SchemaRegistryException;

  void deleteContext(String delimitedContext) throws SchemaRegistryException;

  default Schema lookUpSchemaUnderSubject(
      String subject, Schema schema, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    return lookUpSchemaUnderSubject(subject, schema, false, lookupDeletedSchema);
  }

  Schema lookUpSchemaUnderSubject(
      String subject, Schema schema, boolean normalize, boolean lookupDeletedSchema)
      throws SchemaRegistryException;

  Schema getLatestWithMetadata(
      String subject, Map<String, String> metadata, boolean lookupDeletedSchema)
      throws SchemaRegistryException;

  List<String> isCompatible(String subject,
                            Schema newSchema,
                            List<SchemaKey> previousSchemas,
                            boolean normalize) throws SchemaRegistryException;

  void close() throws IOException;

  void deleteSchemaVersion(String subject, Schema schema,
                           boolean permanentDelete) throws SchemaRegistryException;

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

  SchemaRegistryConfig config();

  // Can be used to pass values between extensions
  Map<String, Object> properties();
}
