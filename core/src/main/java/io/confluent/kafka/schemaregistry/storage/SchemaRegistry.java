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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.RestService;

import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchCreateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpdateRequest;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.ContextId;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.rest.handlers.UpdateRequestHandler;
import io.confluent.kafka.schemaregistry.storage.encoder.MetadataEncoderService;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import org.apache.avro.reflect.Nullable;
import org.eclipse.jetty.server.Handler;

import javax.net.ssl.HostnameVerifier;
import java.util.function.Consumer;
import java.util.function.Predicate;

public interface SchemaRegistry extends SchemaVersionFetcher {

  /**
   * Schema versions under a particular subject are indexed from MIN_VERSION.
   */
  int MIN_VERSION = 1;
  int MAX_VERSION = Integer.MAX_VALUE;
  String DEFAULT_TENANT = QualifiedSubject.DEFAULT_TENANT;
  String RESERVED_FIELD_REMOVED = "The new schema has reserved field %s "
          + "removed from its metadata which is present in the old schema's metadata.";
  String FIELD_CONFLICTS_WITH_RESERVED_FIELD = "The new schema has field that"
          + " conflicts with the reserved field %s.";

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

  SchemaString get(int id, String subject, String format, boolean fetchMaxId) throws
          SchemaRegistryException;

  default Set<String> listSubjects() throws SchemaRegistryException {
    return listSubjects(LookupFilter.DEFAULT);
  }

  Set<String> listSubjects(LookupFilter filter)
          throws SchemaRegistryException;

  Set<String> listSubjectsForId(int id, String subject) throws SchemaRegistryException;

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

  MetadataEncoderService getMetadataEncoder();

  void addUpdateRequestHandler(UpdateRequestHandler updateRequestHandler);

  Mode getModeInScope(String subject) throws SchemaRegistryStoreException;

  List<Handler.Singleton> getCustomHandler();

  void postInit() throws SchemaRegistryException;

  UpdateRequestHandler getCompositeUpdateRequestHandler();

  int normalizeContextLimit(int suppliedLimit);

  Config getConfigInScope(String subject) throws SchemaRegistryStoreException;

  Config getConfig(String subject) throws SchemaRegistryStoreException;

  List<String> listContexts() throws SchemaRegistryException;

  Schema lookUpSchemaUnderSubjectUsingContexts(String subject, Schema schema, boolean normalize,
                                               boolean lookupDeletedSchema) throws
          SchemaRegistryException;

  boolean hasSubjects(String subject, boolean lookupDeletedSubjects) throws
          SchemaRegistryStoreException;

  ParsedSchema parseSchema(Schema schema) throws InvalidSchemaException;

  ParsedSchema parseSchema(Schema schema, boolean isNew, boolean normalize) throws
          InvalidSchemaException;

  Set<String> listSubjectsWithPrefix(String prefix, LookupFilter filter) throws
          SchemaRegistryException;

  int normalizeSubjectLimit(int suppliedLimit);

  int normalizeSchemaLimit(int suppliedLimit);

  void extractSchemaTags(Schema schema, List<String> tags) throws SchemaRegistryException;

  List<SubjectVersion> listVersionsForId(int id, String subject, boolean lookupDeleted) throws
          SchemaRegistryException;

  int normalizeSubjectVersionLimit(int suppliedLimit);

  SchemaString getByGuid(String guid, String format) throws SchemaRegistryException;

  List<ContextId> listIdsForGuid(String guid) throws SchemaRegistryException;

  Schema getUsingContexts(String subject, int version, boolean returnDeletedSchema) throws
          SchemaRegistryException;

  List<Integer> getReferencedBy(String subject, VersionId versionId) throws
          SchemaRegistryException;

  boolean schemaVersionExists(String subject, VersionId versionId, boolean returnDeletedSchema)
          throws SchemaRegistryException;

  Mode getMode(String subject) throws SchemaRegistryStoreException;

  MetricsContainer getMetricsContainer();

  List<SchemaRegistryResourceExtension> getResourceExtensions();

  Config updateConfig(String subject, ConfigUpdateRequest config) throws
          SchemaRegistryStoreException, OperationNotPermittedException, UnknownLeaderException;

  void deleteSubjectConfig(String subject) throws SchemaRegistryStoreException,
          OperationNotPermittedException;

  void setMode(String subject, ModeUpdateRequest request, boolean force) throws
          SchemaRegistryException;

  void setMode(String subject, ModeUpdateRequest mode) throws SchemaRegistryException;

  SchemaProvider schemaProvider(String schemaType);

  Schema toSchemaEntity(SchemaValue schemaValue) throws SchemaRegistryStoreException;

  SchemaValue getSchemaValue(SchemaKey key) throws SchemaRegistryException;

  Set<String> subjects(String subject, boolean lookupDeletedSubjects) throws
          SchemaRegistryStoreException;

  void addCustomHandler(Handler.Singleton handler);

  void deleteSubjectMode(String subject) throws SchemaRegistryStoreException,
          OperationNotPermittedException;

  RuleSetHandler getRuleSetHandler();

  void setRuleSetHandler(RuleSetHandler ruleSetHandler);

  HostnameVerifier getHostnameVerifier() throws SchemaRegistryStoreException;

  SchemaRegistryIdentity myIdentity();

  boolean initialized();

  boolean healthy();

  SslFactory getSslFactory();

  LookupCache<SchemaRegistryKey, SchemaRegistryValue> getLookupCache();

  default KafkaStore<SchemaRegistryKey, SchemaRegistryValue> getKafkaStore() {
    return null;
  }

  default void clearOldSchemaCache() {}

  default void clearNewSchemaCache() {}

  default void invalidateFromNewSchemaCache(Schema schemaKey) {}

  default String getGroupId() {
    return null;
  }

  default String getKafkaClusterId() {
    return null;
  }

  default void deleteSubjectModeOrForward(String subject, Map<String, String> headerProperties)
          throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
          OperationNotPermittedException, UnknownLeaderException {}

  default void setModeOrForward(String subject, ModeUpdateRequest mode, boolean force,
                                Map<String, String> headerProperties) throws
          SchemaRegistryException {}

  default void deleteSchemaVersionOrForward(Map<String, String> headerProperties, String subject,
                                            Schema schema, boolean permanentDelete) throws
          SchemaRegistryException {}

  default Schema modifySchemaTagsOrForward(String subject, Schema schema, TagSchemaRequest request,
                                           Map<String, String> headerProperties) throws
          SchemaRegistryException {
    return null;
  }

  default Schema registerOrForward(String subject, RegisterSchemaRequest request,
                                   boolean normalize, Map<String, String> headerProperties) throws
          SchemaRegistryException {
    return null;
  }

  default List<Integer> deleteSubjectOrForward(Map<String, String> requestProperties,
                                               String subject, boolean permanentDelete) throws
          SchemaRegistryException {
    return null;
  }

  default Config updateConfigOrForward(String subject, ConfigUpdateRequest newConfig,
                                       Map<String, String> headerProperties) throws
          SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
          UnknownLeaderException, OperationNotPermittedException {
    return null;
  }

  default void deleteConfigOrForward(String subject, Map<String, String> headerProperties) throws
          SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
          OperationNotPermittedException, UnknownLeaderException {}

  default void deleteContextOrForward(Map<String, String> requestProperties,
                                      String delimitedContext) throws SchemaRegistryException {}

  default AssociationResponse createAssociation(
      String context, boolean dryRun, AssociationCreateRequest request)
      throws SchemaRegistryException {
    return null;
  }

  default AssociationResponse createAssociationOrForward(String context, boolean dryRun,
      AssociationCreateRequest request,
      Map<String, String> headerProperties)
      throws SchemaRegistryException {
    return null;
  }

  default AssociationResponse updateAssociation(
      String context, boolean dryRun, AssociationUpdateRequest request)
      throws SchemaRegistryException {
    return null;
  }

  default AssociationResponse updateAssociationOrForward(String context, boolean dryRun,
      AssociationUpdateRequest request, Map<String, String> headerProperties)
      throws SchemaRegistryException {
    return null;
  }

  default AssociationBatchResponse batchCreateAssociations(
      String context, boolean dryRun, AssociationBatchCreateRequest request) {
    return null;
  }

  default AssociationBatchResponse createAssociationsOrForward(String context, boolean dryRun,
      AssociationBatchCreateRequest request,
      Map<String, String> headerProperties)
      throws SchemaRegistryException {
    return null;
  }

  default AssociationBatchResponse batchUpdateAssociations(
      String context, boolean dryRun, AssociationBatchUpdateRequest request)
      throws SchemaRegistryException {
    return null;
  }

  default AssociationBatchResponse updateAssociationsOrForward(String context, boolean dryRun,
      AssociationBatchUpdateRequest request,
      Map<String, String> headerProperties)
      throws SchemaRegistryException {
    return null;
  }

  default Association getAssociationByGuid(String guid)
      throws SchemaRegistryException {
    return null;
  }

  default List<Association> getAssociationsBySubject(
      String subject, String resourceType, List<String> associationTypes,
      LifecyclePolicy lifecycle) throws SchemaRegistryException {
    return null;
  }

  default List<Association> getAssociationsByResourceId(
      String resourceId, String resourceType, List<String> associationTypes,
      LifecyclePolicy lifecycle) throws SchemaRegistryException {
    return null;
  }

  default List<Association> getAssociationsByResourceName(
      String resourceName, String resourceNamespace,
      String resourceType, List<String> associationTypes, LifecyclePolicy lifecycle)
      throws SchemaRegistryException {
    return null;
  }

  default void deleteAssociations(
      String resourceId, String resourceType, List<String> associationTypes,
      boolean cascadeLifecycle)
      throws SchemaRegistryException {
  }

  default void deleteAssociationsOrForward(
      String subject,  // subject is only used for locking per tenant
      String resourceId, String resourceType, List<String> associationTypes,
      boolean cascadeLifecycle, Map<String, String> headerProperties)
      throws SchemaRegistryException {
  }

  default void addLeaderChangeListener(Consumer<Boolean> listener) {}

  default boolean isLeader() {
    return false;
  }

  default SchemaRegistryIdentity leaderIdentity() {
    return null;
  }

  default RestService leaderRestService() {
    return null;
  }

  default void setLeader(@Nullable SchemaRegistryIdentity newLeader) throws
          SchemaRegistryTimeoutException, SchemaRegistryStoreException, IdGenerationException {}
}
