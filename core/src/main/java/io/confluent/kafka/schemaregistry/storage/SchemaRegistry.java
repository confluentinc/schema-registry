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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;

public interface SchemaRegistry extends SchemaVersionFetcher {

  String DEFAULT_TENANT = QualifiedSubject.DEFAULT_TENANT;
  
  // Subject name under which global permissions are stored.
  public static final String GLOBAL_RESOURCE_NAME = "__GLOBAL";

  void init() throws SchemaRegistryException;

  Set<String> schemaTypes();

  default int register(String subject, Schema schema) throws SchemaRegistryException {
    return register(subject, schema, false);
  }

  int register(String subject, Schema schema, boolean normalize) throws SchemaRegistryException;

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
  
  SchemaString get(int id, String subject, String format, boolean fetchMaxId)
          throws SchemaRegistryException;

  default Set<String> listSubjects() throws SchemaRegistryException {
    return listSubjects(false);
  }

  Set<String> listSubjects(boolean returnDeletedSubjects)
          throws SchemaRegistryException;
  
  Set<String> listSubjectsForId(int id, String subject) throws SchemaRegistryException;
  
  Set<String> listSubjectsForId(int id, String subject, boolean returnDeleted) 
          throws SchemaRegistryException;
  
  Iterator<Schema> getAllVersions(String subject, boolean returnDeletedSchemas)
      throws SchemaRegistryException;

  Iterator<Schema> getVersionsWithSubjectPrefix(
      String prefix, boolean returnDeletedSchemas, boolean latestOnly)
      throws SchemaRegistryException;

  Schema getLatestVersion(String subject) throws SchemaRegistryException;

  List<Integer> deleteSubject(String subject, boolean permanentDelete)
      throws SchemaRegistryException;

  default Schema lookUpSchemaUnderSubject(
      String subject, Schema schema, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    return lookUpSchemaUnderSubject(subject, schema, false, lookupDeletedSchema);
  }

  Schema lookUpSchemaUnderSubject(
      String subject, Schema schema, boolean normalize, boolean lookupDeletedSchema)
      throws SchemaRegistryException;

  List<String> isCompatible(String subject,
                            Schema newSchema,
                            Schema targetSchema) throws SchemaRegistryException;

  List<String> isCompatible(String subject,
                            Schema newSchema,
                            List<Schema> previousSchemas) throws SchemaRegistryException;

  void close();

  void deleteSchemaVersion(String subject, Schema schema,
                           boolean permanentDelete) throws SchemaRegistryException;

  
  void updateConfigOrForward(String subject, CompatibilityLevel newCompatibilityLevel,
                           Map<String, String> headerProperties)
              throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
                     UnknownLeaderException, OperationNotPermittedException;

  void updateCompatibilityLevel(String subject, CompatibilityLevel newCompatibilityLevel)
              throws SchemaRegistryStoreException, OperationNotPermittedException,
               UnknownLeaderException;
  
  CompatibilityLevel getCompatibilityLevel(String subject)
              throws SchemaRegistryStoreException;
  
  CompatibilityLevel getCompatibilityLevelInScope(String subject)
              throws SchemaRegistryStoreException;
  
  public void deleteCompatibilityConfigOrForward(String subject,
          Map<String, String> headerProperties) 
                  throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
                  OperationNotPermittedException, UnknownLeaderException;

  boolean hasSubjects(String subject,
          boolean lookupDeletedSubjects)
        throws SchemaRegistryStoreException;
  
  
  void deleteSubjectModeOrForward(String subject, Map<String, String> headerProperties)
          throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
          OperationNotPermittedException, UnknownLeaderException;
          
  List<Integer> deleteSubjectOrForward(
          Map<String, String> requestProperties,
          String subject,
          boolean permanentDelete) throws SchemaRegistryException;
  

  
  List<Integer> getReferencedBy(String subject, VersionId versionId)
          throws SchemaRegistryException;
  
  public List<String> listContexts() throws SchemaRegistryException;
  
  public Schema lookUpSchemaUnderSubjectUsingContexts(
          String subject, Schema schema, boolean normalize, boolean lookupDeletedSchema)
          throws SchemaRegistryException;

  public Set<String> listSubjectsWithPrefix(String prefix, boolean returnDeletedSubjects)
          throws SchemaRegistryException;
  
  public String getGroupId();
  
  public int registerOrForward(String subject,
          Schema schema,
          boolean normalize,
          Map<String, String> headerProperties)
          throws SchemaRegistryException;
  
  public void setModeOrForward(String subject, Mode mode, boolean force,
          Map<String, String> headerProperties)
          throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
          OperationNotPermittedException, UnknownLeaderException; 
  
  Mode getModeInScope(String subject) throws SchemaRegistryStoreException;
  
  Mode getMode(String subject) throws SchemaRegistryStoreException;
  
  //String getKafkaClusterId();
  
  boolean schemaVersionExists(String subject, VersionId versionId, boolean
          returnDeletedSchema) throws SchemaRegistryException;

  List<SubjectVersion> listVersionsForId(int id, String subject)
          throws SchemaRegistryException;
  
  List<SubjectVersion> listVersionsForId(int id, String subject, boolean lookupDeleted)
          throws SchemaRegistryException;
  
  void deleteSchemaVersionOrForward(
          Map<String, String> headerProperties, String subject,
          Schema schema, boolean permanentDelete) throws SchemaRegistryException;
  
  MetricsContainer getMetricsContainer(); // TODO maybe move to abstract class

  default String tenant() {
    return DEFAULT_TENANT;
  }

  public Schema getUsingContexts(String subject, int version, boolean
          returnDeletedSchema) throws SchemaRegistryException;
   

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
