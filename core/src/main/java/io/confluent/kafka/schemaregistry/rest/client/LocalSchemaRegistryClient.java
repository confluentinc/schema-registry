/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.client;

import static io.confluent.kafka.schemaregistry.storage.SchemaRegistry.DEFAULT_TENANT;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.TENANT_DELIMITER;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.exceptions.IdDoesNotMatchException;
import io.confluent.kafka.schemaregistry.exceptions.IncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.ReferenceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaVersionNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.SubjectNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidCompatibilityException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidModeException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.LookupFilter;
import io.confluent.kafka.schemaregistry.storage.Mode;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local implementation of SchemaRegistryClient that can be used without going through HTTP.
 */
public class LocalSchemaRegistryClient implements SchemaRegistryClient {

  private static final Logger log = LoggerFactory.getLogger(LocalSchemaRegistryClient.class);

  private final KafkaSchemaRegistry schemaRegistry;
  private final Map<String, SchemaProvider> providers;

  public LocalSchemaRegistryClient(KafkaSchemaRegistry schemaRegistry) {
    this(schemaRegistry, null);
  }

  public LocalSchemaRegistryClient(
      KafkaSchemaRegistry schemaRegistry, List<SchemaProvider> providers) {
    this.schemaRegistry = schemaRegistry;
    this.providers = providers != null && !providers.isEmpty()
                     ? providers.stream().collect(Collectors.toMap(p -> p.schemaType(), p -> p))
                     : Collections.singletonMap(AvroSchema.TYPE, new AvroSchemaProvider());
    Map<String, Object> schemaProviderConfigs = new HashMap<>();
    schemaProviderConfigs.put(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, this);
    for (SchemaProvider provider : this.providers.values()) {
      provider.configure(schemaProviderConfigs);
    }
  }

  @Override
  public Optional<ParsedSchema> parseSchema(
      String schemaType,
      String schemaString,
      List<SchemaReference> references) {
    if (schemaType == null) {
      schemaType = AvroSchema.TYPE;
    }
    SchemaProvider schemaProvider = providers.get(schemaType);
    if (schemaProvider == null) {
      log.error("No provider found for schema type {}", schemaType);
      return Optional.empty();
    }
    return schemaProvider.parseSchema(schemaString, references);
  }

  @Override
  public Optional<ParsedSchema> parseSchema(Schema schema) {
    String schemaType = schema.getSchemaType();
    if (schemaType == null) {
      schemaType = AvroSchema.TYPE;
    }
    SchemaProvider schemaProvider = providers.get(schemaType);
    if (schemaProvider == null) {
      log.error("No provider found for schema type {}", schemaType);
      return Optional.empty();
    }
    return schemaProvider.parseSchema(schema, false);
  }

  @Override
  public synchronized List<Integer> getAllVersions(String subject)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    Iterator<SchemaKey> resultSchemas = null;
    List<Integer> allVersions = new ArrayList<>();
    String errorMessage = "Error while validating that subject "
        + subject
        + " exists in the registry";
    try {
      if (!schemaRegistry.hasSubjects(subject, false)) {
        throw Errors.subjectNotFoundException(subject);
      }
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    errorMessage = "Error while listing all versions for subject "
        + subject;
    try {
      resultSchemas = schemaRegistry.getAllVersions(subject, LookupFilter.DEFAULT);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    while (resultSchemas.hasNext()) {
      SchemaKey schema = resultSchemas.next();
      allVersions.add(schema.getVersion());
    }
    return allVersions;
  }

  @Override
  public synchronized int register(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return register(subject, schema, false);
  }

  @Override
  public synchronized int register(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return registerWithResponse(subject, schema, 0, -1, normalize).getId();
  }

  @Override
  public synchronized int register(String subject, ParsedSchema schema, int version, int id)
      throws IOException, RestClientException {
    return registerWithResponse(subject, schema, version, id, false).getId();
  }

  @Override
  public synchronized RegisterSchemaResponse registerWithResponse(
      String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return registerWithResponse(subject, schema, 0, -1, normalize);
  }

  private synchronized RegisterSchemaResponse registerWithResponse(
      String subject, ParsedSchema schema, int version, int id, boolean normalize)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    Schema s = new Schema(subject, version, id, schema);
    try {
      return new RegisterSchemaResponse(schemaRegistry.register(subject, s, normalize));
    } catch (IdDoesNotMatchException e) {
      throw Errors.idDoesNotMatchException(e);
    } catch (InvalidSchemaException e) {
      throw Errors.invalidSchemaException(e);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Register operation timed out", e);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Register schema operation failed while writing"
          + " to the Kafka store", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding register schema request"
          + " to the leader", e);
    } catch (IncompatibleSchemaException e) {
      throw Errors.incompatibleSchemaException("Schema being registered is incompatible with an"
          + " earlier schema for subject "
          + "\"" + subject + "\"", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while registering schema", e);
    }
  }

  @Override
  public synchronized ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    SchemaString s = null;
    String errorMessage = "Error while retrieving schema with id " + id + " from the schema "
        + "registry";
    try {
      s = schemaRegistry.get(id, subject);
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    if (s == null) {
      throw Errors.schemaNotFoundException(id);
    }
    return parseSchema(new Schema(null, null, null, s)).get();
  }

  @Override
  public synchronized List<ParsedSchema> getSchemas(
      String subjectPrefix,
      boolean lookupDeletedSchema,
      boolean latestOnly)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subjectPrefix = schemaRegistry.tenant() + TENANT_DELIMITER + subjectPrefix;
    }
    Iterator<ExtendedSchema> schemas = null;
    List<ParsedSchema> result = new ArrayList<>();
    String errorMessage = "Error while getting schemas for prefix " + subjectPrefix;
    try {
      schemas = schemaRegistry.getVersionsWithSubjectPrefix(
          subjectPrefix,
          lookupDeletedSchema ? LookupFilter.INCLUDE_DELETED : LookupFilter.DEFAULT,
          latestOnly,
          null);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    while (schemas.hasNext()) {
      Schema s = schemas.next();
      result.add(parseSchema(s).get());
    }
    return result;
  }

  @Override
  public Collection<String> getAllSubjectsById(int id) throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<SubjectVersion> getAllVersionsById(int id) throws IOException,
      RestClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
    try {
      SchemaMetadata schema = getSchemaMetadata(subject, version, lookupDeletedSchema);
      return new Schema(subject, schema);
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException {
    return getSchemaMetadata(subject, version, false);
  }

  public synchronized SchemaMetadata getSchemaMetadata(
      String subject, int version, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    VersionId versionId = null;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }
    Schema s = null;
    String errorMessage = "Error while retrieving schema for subject "
        + subject
        + " with version "
        + version
        + " from the schema registry";
    try {
      s = schemaRegistry.get(subject, versionId.getVersionId(), lookupDeletedSchema);
      if (s == null) {
        if (!schemaRegistry.hasSubjects(subject, true)) {
          throw Errors.subjectNotFoundException(subject);
        } else {
          throw Errors.versionNotFoundException(versionId.getVersionId());
        }
      }
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    return new SchemaMetadata(s);
  }

  @Override
  public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    return getSchemaMetadata(subject, -1);
  }

  @Override
  public synchronized int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return getVersion(subject, schema, false);
  }

  @Override
  public synchronized int getVersion(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    Schema s = new Schema(subject, 0, -1, schema);
    Schema matchingSchema = null;
    try {
      if (!schemaRegistry.hasSubjects(subject, false)) {
        throw Errors.subjectNotFoundException(subject);
      }
      matchingSchema =
          schemaRegistry.lookUpSchemaUnderSubject(subject, s, normalize, false);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while looking up schema under subject " + subject,
          e);
    }
    if (matchingSchema == null) {
      throw Errors.schemaNotFoundException();
    }
    return matchingSchema.getVersion();
  }

  @Override
  public boolean testCompatibility(String subject, ParsedSchema newSchema) throws IOException,
                                                                            RestClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> testCompatibilityVerbose(String subject, ParsedSchema newSchema)
          throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Config updateConfig(String subject, Config config)
      throws IOException,
             RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    CompatibilityLevel compatibilityLevel =
        CompatibilityLevel.forName(config.getCompatibilityLevel());
    if (config.getCompatibilityLevel() != null && compatibilityLevel == null) {
      throw new RestInvalidCompatibilityException();
    }
    try {
      schemaRegistry.updateConfig(subject, config);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update compatibility level", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Failed to update compatibility level", e);
    }
    return config;
  }

  @Override
  public Config getConfig(String subject) throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    try {
      Config config = schemaRegistry.getConfig(subject);
      if (config == null) {
        throw Errors.subjectLevelCompatibilityNotConfiguredException(subject);
      }
      return config;
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to get the configs for subject "
          + subject, e);
    }
  }

  @Override
  public void deleteConfig(String subject) throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    try {
      schemaRegistry.deleteSubjectConfig(subject);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to get the configs for subject "
          + subject, e);
    }
  }

  @Override
  public String setMode(String mode)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String setMode(String mode, String subject)
      throws IOException, RestClientException {
    return setMode(mode, subject, false);
  }

  @Override
  public String setMode(String mode, String subject, boolean force)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    Mode m;
    try {
      m = Enum.valueOf(Mode.class,
          mode.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new RestInvalidModeException();
    }
    try {
      schemaRegistry.setMode(subject, m, force);
    } catch (ReferenceExistsException e) {
      throw Errors.referenceExistsException(e.getMessage());
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update mode", e);
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Update mode operation timed out", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while updating the mode", e);
    }
    return m.name();
  }

  @Override
  public String getMode() throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMode(String subject) throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    try {
      Mode mode = schemaRegistry.getMode(subject);
      if (mode == null) {
        throw Errors.subjectLevelModeNotConfiguredException(subject);
      }
      return mode.name();
    } catch (SchemaRegistryException e) {
      throw Errors.storeException("Failed to get mode", e);
    }
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getId(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return getId(subject, schema, false);
  }

  @Override
  public int getId(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    Schema s = new Schema(subject, 0, -1, schema);
    Schema matchingSchema = null;
    try {
      if (!schemaRegistry.hasSubjects(subject, false)) {
        throw Errors.subjectNotFoundException(subject);
      }
      matchingSchema =
          schemaRegistry.lookUpSchemaUnderSubject(subject, s, normalize, false);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while looking up schema under subject " + subject,
          e);
    }
    if (matchingSchema == null) {
      throw Errors.schemaNotFoundException();
    }
    return matchingSchema.getId();
  }

  @Override
  public List<Integer> deleteSubject(String subject, boolean isPermanent)
          throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    return deleteSubject(null, subject, isPermanent);
  }

  @Override
  public List<Integer> deleteSubject(
      Map<String, String> requestProperties,
      String subject,
      boolean isPermanent)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    try {
      if (!schemaRegistry.hasSubjects(subject, true)) {
        throw Errors.subjectNotFoundException(subject);
      }
      if (!isPermanent && !schemaRegistry.hasSubjects(subject, false)) {
        throw Errors.subjectSoftDeletedException(subject);
      }
      return schemaRegistry.deleteSubject(subject, isPermanent);
    } catch (ReferenceExistsException e) {
      throw Errors.referenceExistsException(e.getMessage());
    } catch (SubjectNotSoftDeletedException e) {
      throw Errors.subjectNotSoftDeletedException(subject);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting the subject " + subject,
          e);
    }
  }

  @Override
  public Integer deleteSchemaVersion(String subject, String version, boolean isPermanent)
      throws IOException, RestClientException {
    return deleteSchemaVersion(null, subject, version, isPermanent);
  }

  @Override
  public Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version,
      boolean isPermanent)
      throws IOException, RestClientException {
    if (!DEFAULT_TENANT.equals(schemaRegistry.tenant())) {
      subject = schemaRegistry.tenant() + TENANT_DELIMITER + subject;
    }
    VersionId versionId = null;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }
    Schema schema = null;
    String errorMessage =
        "Error while retrieving schema for subject "
            + subject
            + " with version "
            + version
            + " from the schema registry";
    try {
      if (schemaRegistry.schemaVersionExists(subject, versionId, true)) {
        if (!isPermanent
            && !schemaRegistry.schemaVersionExists(subject,
            versionId, false)) {
          throw Errors.schemaVersionSoftDeletedException(subject, version);
        }
      }
      schema = schemaRegistry.get(subject, versionId.getVersionId(), true);
      if (schema == null) {
        if (!schemaRegistry.hasSubjects(subject, true)) {
          throw Errors.subjectNotFoundException(subject);
        } else {
          throw Errors.versionNotFoundException(versionId.getVersionId());
        }
      }
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }

    try {
      schemaRegistry.deleteSchemaVersion(subject, schema, isPermanent);
    } catch (SchemaVersionNotSoftDeletedException e) {
      throw Errors.schemaVersionNotSoftDeletedException(e.getSubject(),
          e.getVersion());
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Delete Schema Version operation timed out", e);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Delete Schema Version operation failed while writing"
          + " to the Kafka store", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors
          .requestForwardingFailedException("Error while forwarding delete schema version request"
              + " to the leader", e);
    } catch (ReferenceExistsException e) {
      throw Errors.referenceExistsException(e.getMessage());
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting Schema Version", e);
    }
    return schema.getVersion();
  }

  @Override
  public void reset() {
  }
}
