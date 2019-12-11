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

import org.apache.avro.reflect.Nullable;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import io.confluent.common.metrics.JmxReporter;
import io.confluent.common.metrics.MetricConfig;
import io.confluent.common.metrics.MetricName;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.MetricsReporter;
import io.confluent.common.metrics.Sensor;
import io.confluent.common.metrics.stats.Gauge;
import io.confluent.common.utils.SystemTime;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.exceptions.IdDoesNotMatchException;
import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.exceptions.IncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryInitializationException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownMasterException;
import io.confluent.kafka.schemaregistry.id.IdGenerator;
import io.confluent.kafka.schemaregistry.id.IncrementalIdGenerator;
import io.confluent.kafka.schemaregistry.id.ZookeeperIdGenerator;
import io.confluent.kafka.schemaregistry.masterelector.kafka.KafkaGroupMasterElector;
import io.confluent.kafka.schemaregistry.masterelector.zookeeper.ZookeeperMasterElector;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SslFactory;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import io.confluent.rest.exceptions.RestException;

import javax.net.ssl.HostnameVerifier;

public class KafkaSchemaRegistry implements SchemaRegistry, MasterAwareSchemaRegistry {

  /**
   * Schema versions under a particular subject are indexed from MIN_VERSION.
   */
  public static final int MIN_VERSION = 1;
  public static final int MAX_VERSION = Integer.MAX_VALUE;
  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistry.class);

  private final SchemaRegistryConfig config;
  private final LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache;
  // visible for testing
  final KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore;
  private final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer;
  private final SchemaRegistryIdentity myIdentity;
  private final Object masterLock = new Object();
  private final AvroCompatibilityLevel defaultCompatibilityLevel;
  private final Mode defaultMode;
  private final int kafkaStoreTimeoutMs;
  private final int initTimeout;
  private final boolean isEligibleForMasterElector;
  private final boolean allowModeChanges;
  private SchemaRegistryIdentity masterIdentity;
  private RestService masterRestService;
  private SslFactory sslFactory;
  private IdGenerator idGenerator = null;
  private MasterElector masterElector = null;
  private Metrics metrics;
  private Sensor masterNodeSensor;

  public KafkaSchemaRegistry(SchemaRegistryConfig config,
                             Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer)
      throws SchemaRegistryException {
    this.config = config;
    String host = config.getString(SchemaRegistryConfig.HOST_NAME_CONFIG);
    SchemeAndPort schemeAndPort = getSchemeAndPortForIdentity(
        config.getInt(SchemaRegistryConfig.PORT_CONFIG),
        config.getList(RestConfig.LISTENERS_CONFIG),
        config.interInstanceProtocol()
    );
    this.isEligibleForMasterElector = config.getBoolean(SchemaRegistryConfig.MASTER_ELIGIBILITY);
    this.allowModeChanges = config.getBoolean(SchemaRegistryConfig.MODE_MUTABILITY);
    this.myIdentity = new SchemaRegistryIdentity(host, schemeAndPort.port,
        isEligibleForMasterElector, schemeAndPort.scheme);
    this.sslFactory = new SslFactory(config);
    this.kafkaStoreTimeoutMs =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
    this.initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
    this.serializer = serializer;
    this.defaultCompatibilityLevel = config.compatibilityType();
    this.defaultMode = Mode.READWRITE;
    this.lookupCache = lookupCache();
    this.idGenerator = identityGenerator(config);
    this.kafkaStore = kafkaStore(config);
    MetricConfig metricConfig =
        new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                        TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters =
        config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                                      MetricsReporter.class);
    String jmxPrefix = "kafka.schema.registry";
    reporters.add(new JmxReporter(jmxPrefix));
    this.metrics = new Metrics(metricConfig, reporters, new SystemTime());
    this.masterNodeSensor = metrics.sensor("master-slave-role");

    Map<String, String> configuredTags = config.getMap(RestConfig.METRICS_TAGS_CONFIG);
    MetricName
        m = new MetricName("master-slave-role", "master-slave-role",
                           "1.0 indicates the node is the active master in the cluster and is the"
                           + " node where all register schema and config update requests are "
                           + "served.", configuredTags);
    this.masterNodeSensor.add(m, new Gauge());
  }

  protected KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore(
      SchemaRegistryConfig config) throws SchemaRegistryException {
    return new KafkaStore<SchemaRegistryKey, SchemaRegistryValue>(
        config,
        new KafkaStoreMessageHandler(this, lookupCache, idGenerator),
        this.serializer, lookupCache, new NoopKey());
  }

  protected LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache() {
    return new InMemoryCache<SchemaRegistryKey, SchemaRegistryValue>();
  }

  protected IdGenerator identityGenerator(SchemaRegistryConfig config) {
    IdGenerator idGenerator;
    if (config.useKafkaCoordination()) {
      idGenerator = new IncrementalIdGenerator();
    } else {
      idGenerator = new ZookeeperIdGenerator();
    }
    idGenerator.configure(config);
    return idGenerator;
  }

  /**
   * A Schema Registry instance's identity is in part the port it listens on. Currently the port can
   * either be configured via the deprecated `port` configuration, or via the `listeners`
   * configuration.
   *
   * <p>This method uses `Application.parseListeners()` from `rest-utils` to get a list of
   * listeners, and returns the port of the first listener to be used for the instance's identity.
   *
   * <p></p>In theory, any port from any listener would be sufficient. Choosing the first, instead
   * of say the last, is arbitrary.
   */
  // TODO: once RestConfig.PORT_CONFIG is deprecated, remove the port parameter.
  static SchemeAndPort getSchemeAndPortForIdentity(int port, List<String> configuredListeners,
                                                   String requestedScheme)
      throws SchemaRegistryException {
    List<URI> listeners = Application.parseListeners(configuredListeners, port,
                                                     Arrays.asList(
                                                         SchemaRegistryConfig.HTTP,
                                                         SchemaRegistryConfig.HTTPS
                                                     ), SchemaRegistryConfig.HTTP
    );
    if (requestedScheme.isEmpty()) {
      requestedScheme = SchemaRegistryConfig.HTTP;
    }
    for (URI listener: listeners) {
      if (requestedScheme.equalsIgnoreCase(listener.getScheme())) {
        return new SchemeAndPort(listener.getScheme(), listener.getPort());
      }
    }
    throw new SchemaRegistryException(" No listener configured with requested scheme "
                                      + requestedScheme);
  }

  @Override
  public void init() throws SchemaRegistryException {
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      throw new SchemaRegistryInitializationException(
          "Error initializing kafka store while initializing schema registry", e);
    }

    try {
      if (config.useKafkaCoordination()) {
        log.info("Joining schema registry with Kafka-based coordination");
        masterElector = new KafkaGroupMasterElector(config, myIdentity, this);
      } else {
        log.info("Joining schema registry with Zookeeper-based coordination");
        masterElector = new ZookeeperMasterElector(config, myIdentity, this);
      }
      masterElector.init();
    } catch (SchemaRegistryStoreException e) {
      throw new SchemaRegistryInitializationException(
          "Error electing master while initializing schema registry", e);
    } catch (SchemaRegistryTimeoutException e) {
      throw new SchemaRegistryInitializationException(e);
    }
  }

  public boolean isMaster() {
    synchronized (masterLock) {
      if (masterIdentity != null && masterIdentity.equals(myIdentity)) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * 'Inform' this SchemaRegistry instance which SchemaRegistry is the current master.
   * If this instance is set as the new master, ensure it is up-to-date with data in
   * the kafka store, and tell Zookeeper to allocate the next batch of schema IDs.
   *
   * @param newMaster Identity of the current master. null means no master is alive.
   */
  @Override
  public void setMaster(@Nullable SchemaRegistryIdentity newMaster)
      throws SchemaRegistryTimeoutException, SchemaRegistryStoreException, IdGenerationException {
    log.debug("Setting the master to " + newMaster);

    // Only schema registry instances eligible for master can be set to master
    if (newMaster != null && !newMaster.getMasterEligibility()) {
      throw new IllegalStateException(
          "Tried to set an ineligible node to master: " + newMaster);
    }

    synchronized (masterLock) {
      SchemaRegistryIdentity previousMaster = masterIdentity;
      masterIdentity = newMaster;

      if (masterIdentity == null) {
        masterRestService = null;
      } else {
        masterRestService = new RestService(masterIdentity.getUrl());
        if (sslFactory != null && sslFactory.sslContext() != null) {
          masterRestService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
          masterRestService.setHostnameVerifier(getHostnameVerifier());
        }
      }

      if (masterIdentity != null && !masterIdentity.equals(previousMaster) && isMaster()) {
        // The new master may not know the exact last offset in the Kafka log. So, mark the
        // last offset invalid here
        kafkaStore.markLastWrittenOffsetInvalid();
        //ensure the new master catches up with the offsets before it gets nextid and assigns
        // master
        try {
          kafkaStore.waitUntilKafkaReaderReachesLastOffset(initTimeout);
        } catch (StoreException e) {
          throw new SchemaRegistryStoreException("Exception getting latest offset ", e);
        }
        idGenerator.init();
      }
      masterNodeSensor.record(isMaster() ? 1.0 : 0.0);
    }
  }

  /**
   * Return json data encoding basic information about this SchemaRegistry instance, such as
   * host, port, etc.
   */
  public SchemaRegistryIdentity myIdentity() {
    return myIdentity;
  }

  /**
   * Return the identity of the SchemaRegistry that this instance thinks is current master.
   * Any request that requires writing new data gets forwarded to the master.
   */
  public SchemaRegistryIdentity masterIdentity() {
    synchronized (masterLock) {
      return masterIdentity;
    }
  }

  @Override
  public int register(String subject,
                      Schema schema)
      throws SchemaRegistryException {
    try {
      checkRegisterMode(subject, schema);

      // Ensure cache is up-to-date before any potential writes
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(kafkaStoreTimeoutMs);

      // see if the schema to be registered already exists
      int schemaId = schema.getId();
      SchemaIdAndSubjects schemaIdAndSubjects = this.lookupCache.schemaIdAndSubjects(schema);
      if (schemaIdAndSubjects != null) {
        if (schemaId >= 0 && schemaId != schemaIdAndSubjects.getSchemaId()) {
          throw new IdDoesNotMatchException(schemaIdAndSubjects.getSchemaId(), schema.getId());
        }
        if (schemaIdAndSubjects.hasSubject(subject)
            && !isSubjectVersionDeleted(subject, schemaIdAndSubjects.getVersion(subject))) {
          // return only if the schema was previously registered under the input subject
          return schemaIdAndSubjects.getSchemaId();
        } else {
          // need to register schema under the input subject
          schemaId = schemaIdAndSubjects.getSchemaId();
        }
      }

      // determine the latest version of the schema in the subject
      Iterator<Schema> allVersions = getAllVersions(subject, true);
      Iterator<Schema> undeletedVersions = getAllVersions(subject, false);

      List<String> undeletedSchemasList = new ArrayList<>();
      Schema latestSchema = null;
      int newVersion = MIN_VERSION;
      while (allVersions.hasNext()) {
        newVersion = allVersions.next().getVersion() + 1;
      }
      while (undeletedVersions.hasNext()) {
        latestSchema = undeletedVersions.next();
        undeletedSchemasList.add(latestSchema.getSchema());
      }

      AvroSchema avroSchema = canonicalizeSchema(schema);
      // assign a guid and put the schema in the kafka store
      if (latestSchema == null || isCompatible(subject, avroSchema.canonicalString,
                                               undeletedSchemasList)) {
        if (schema.getVersion() <= 0) {
          schema.setVersion(newVersion);
        }

        if (schemaId >= 0) {
          schema.setId(schemaId);
        } else {
          int newId = idGenerator.id(schema);
          if (lookupCache.schemaKeyById(newId) != null) {
            throw new SchemaRegistryStoreException("Error while registering the schema due "
                + "to generating an ID that is already in use.");
          }
          schema.setId(newId);
        }

        SchemaValue schemaValue = new SchemaValue(schema);
        kafkaStore.put(new SchemaKey(subject, schema.getVersion()), schemaValue);
        return schema.getId();
      } else {
        throw new IncompatibleSchemaException(
            "New schema is incompatible with an earlier schema.");
      }
    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while registering the schema in the"
                                             + " backend Kafka store", e);
    }
  }

  private void checkRegisterMode(
      String subject, Schema schema
  ) throws OperationNotPermittedException, SchemaRegistryStoreException {
    if (getModeInScope(subject) == Mode.READONLY) {
      throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
    }

    if (schema.getId() >= 0 || schema.getVersion() > 0) {
      if (getModeInScope(subject) != Mode.IMPORT) {
        throw new OperationNotPermittedException("Subject " + subject + " is not in import mode");
      }
    } else {
      if (getModeInScope(subject) != Mode.READWRITE) {
        throw new OperationNotPermittedException(
            "Subject " + subject + " is not in read-write mode"
        );
      }
    }
  }

  public int registerOrForward(String subject,
                               Schema schema,
                               Map<String, String> headerProperties)
      throws SchemaRegistryException {
    Schema existingSchema = lookUpSchemaUnderSubject(subject, schema, false);
    if (existingSchema != null) {
      if (schema.getId() != null
          && schema.getId() >= 0
          && !schema.getId().equals(existingSchema.getId())
      ) {
        throw new IdDoesNotMatchException(existingSchema.getId(), schema.getId());
      }
      return existingSchema.getId();
    }

    synchronized (masterLock) {
      if (isMaster()) {
        return register(subject, schema);
      } else {
        // forward registering request to the master
        if (masterIdentity != null) {
          return forwardRegisterRequestToMaster(subject, schema, headerProperties);
        } else {
          throw new UnknownMasterException("Register schema request failed since master is "
                                           + "unknown");
        }
      }
    }
  }

  @Override
  public void deleteSchemaVersion(String subject,
                                  Schema schema)
      throws SchemaRegistryException {
    try {
      if (getModeInScope(subject) == Mode.READONLY) {
        throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
      }
      // Ensure cache is up-to-date before any potential writes
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(kafkaStoreTimeoutMs);
      SchemaValue schemaValue = new SchemaValue(schema);
      schemaValue.setDeleted(true);
      kafkaStore.put(new SchemaKey(subject, schema.getVersion()), schemaValue);
      if (!getAllVersions(subject, false).hasNext()) {
        if (getMode(subject) != null) {
          deleteMode(subject);
        }
        if (getCompatibilityLevel(subject) != null) {
          deleteSubjectCompatibility(subject);
        }
      }
    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while deleting the schema in the"
                                             + " backend Kafka store", e);
    }
  }


  public void deleteSchemaVersionOrForward(
      Map<String, String> headerProperties, String subject,
      Schema schema) throws SchemaRegistryException {

    synchronized (masterLock) {
      if (isMaster()) {
        deleteSchemaVersion(subject, schema);
      } else {
        // forward registering request to the master
        if (masterIdentity != null) {
          forwardDeleteSchemaVersionRequestToMaster(headerProperties, subject, schema.getVersion());
        } else {
          throw new UnknownMasterException("Register schema request failed since master is "
                                           + "unknown");
        }
      }
    }
  }

  @Override
  public List<Integer> deleteSubject(String subject) throws SchemaRegistryException {
    // Ensure cache is up-to-date before any potential writes
    try {
      if (getModeInScope(subject) == Mode.READONLY) {
        throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
      }
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(kafkaStoreTimeoutMs);
      List<Integer> deletedVersions = new ArrayList<>();
      int deleteWatermarkVersion = 0;
      Iterator<Schema> schemasToBeDeleted = getAllVersions(subject, false);
      while (schemasToBeDeleted.hasNext()) {
        deleteWatermarkVersion = schemasToBeDeleted.next().getVersion();
        deletedVersions.add(deleteWatermarkVersion);
      }
      DeleteSubjectKey key = new DeleteSubjectKey(subject);
      DeleteSubjectValue value = new DeleteSubjectValue(subject, deleteWatermarkVersion);
      kafkaStore.put(key, value);
      if (getMode(subject) != null) {
        deleteMode(subject);
      }
      if (getCompatibilityLevel(subject) != null) {
        deleteSubjectCompatibility(subject);
      }
      return deletedVersions;

    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while deleting the subject in the"
                                             + " backend Kafka store", e);
    }
  }

  public List<Integer> deleteSubjectOrForward(
      Map<String, String> requestProperties,
      String subject) throws SchemaRegistryException {
    synchronized (masterLock) {
      if (isMaster()) {
        return deleteSubject(subject);
      } else {
        // forward registering request to the master
        if (masterIdentity != null) {
          return forwardDeleteSubjectRequestToMaster(requestProperties, subject);
        } else {
          throw new UnknownMasterException("Register schema request failed since master is "
                                           + "unknown");
        }
      }
    }
  }


  /**
   * Checks if given schema was ever registered under a subject. If found, it returns the version of
   * the schema under the subject. If not, returns -1
   */
  public Schema lookUpSchemaUnderSubject(String subject, Schema schema, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    canonicalizeSchema(schema);
    SchemaIdAndSubjects schemaIdAndSubjects = this.lookupCache.schemaIdAndSubjects(schema);
    if (schemaIdAndSubjects != null) {
      if (schemaIdAndSubjects.hasSubject(subject)
          && (lookupDeletedSchema || !isSubjectVersionDeleted(subject, schemaIdAndSubjects
          .getVersion(subject)))) {
        Schema matchingSchema = new Schema(subject,
                                           schemaIdAndSubjects.getVersion(subject),
                                           schemaIdAndSubjects.getSchemaId(),
                                           schema.getSchema());
        return matchingSchema;
      } else {
        // this schema was never registered under the input subject
        return null;
      }
    } else {
      // this schema was never registered in the registry under any subject
      return null;
    }
  }

  private int forwardRegisterRequestToMaster(String subject, Schema schema,
                                             Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    final UrlList baseUrl = masterRestService.getBaseUrls();

    RegisterSchemaRequest registerSchemaRequest = new RegisterSchemaRequest();
    registerSchemaRequest.setSchema(schema.getSchema());
    registerSchemaRequest.setVersion(schema.getVersion());
    registerSchemaRequest.setId(schema.getId());
    log.debug(String.format("Forwarding registering schema request %s to %s",
                            registerSchemaRequest, baseUrl));
    try {
      int id = masterRestService.registerSchema(headerProperties, registerSchemaRequest, subject);
      return id;
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the registering schema request %s to %s",
                        registerSchemaRequest, baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardUpdateCompatibilityLevelRequestToMaster(
      String subject, AvroCompatibilityLevel compatibilityLevel,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = masterRestService.getBaseUrls();

    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest();
    configUpdateRequest.setCompatibilityLevel(compatibilityLevel.name);
    log.debug(String.format("Forwarding update config request %s to %s",
                            configUpdateRequest, baseUrl));
    try {
      masterRestService.updateConfig(headerProperties, configUpdateRequest, subject);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the update config request %s to %s",
                        configUpdateRequest, baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardDeleteSchemaVersionRequestToMaster(
      Map<String, String> headerProperties,
      String subject,
      Integer version) throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = masterRestService.getBaseUrls();

    log.debug(String.format("Forwarding deleteSchemaVersion schema version request %s-%s to %s",
                            subject, version, baseUrl));
    try {
      masterRestService.deleteSchemaVersion(headerProperties, subject, String.valueOf(version));
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format(
              "Unexpected error while forwarding deleteSchemaVersion schema version "
              + "request %s-%s to %s", subject, version, baseUrl), e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private List<Integer> forwardDeleteSubjectRequestToMaster(
      Map<String, String> requestProperties,
      String subject) throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = masterRestService.getBaseUrls();

    log.debug(String.format("Forwarding delete subject request for  %s to %s",
                            subject, baseUrl));
    try {
      return masterRestService.deleteSubject(requestProperties, subject);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format(
              "Unexpected error while forwarding delete subject "
              + "request %s to %s", subject, baseUrl), e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardSetModeRequestToMaster(
      String subject, Mode mode,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = masterRestService.getBaseUrls();

    ModeUpdateRequest modeUpdateRequest = new ModeUpdateRequest();
    modeUpdateRequest.setMode(mode.name());
    log.debug(String.format("Forwarding update mode request %s to %s",
        modeUpdateRequest, baseUrl));
    try {
      masterRestService.setMode(headerProperties, modeUpdateRequest, subject);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the update mode request %s to %s",
              modeUpdateRequest, baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private AvroSchema canonicalizeSchema(Schema schema) throws InvalidSchemaException {
    AvroSchema avroSchema = AvroUtils.parseSchema(schema.getSchema());
    if (avroSchema == null) {
      throw new InvalidSchemaException("Invalid schema " + schema.toString());
    }
    schema.setSchema(avroSchema.canonicalString);
    return avroSchema;
  }

  public Schema validateAndGetSchema(String subject, VersionId versionId, boolean
      returnDeletedSchema) throws SchemaRegistryException {
    Schema schema = this.get(subject, versionId.getVersionId(), returnDeletedSchema);
    if (schema == null) {
      if (!this.listSubjects().contains(subject)) {
        throw Errors.subjectNotFoundException();
      } else {
        throw Errors.versionNotFoundException();
      }
    }
    return schema;
  }

  @Override
  public Schema get(String subject, int version, boolean returnDeletedSchema)
      throws SchemaRegistryException {
    VersionId versionId = new VersionId(version);
    if (versionId.isLatest()) {
      return getLatestVersion(subject);
    } else {
      SchemaKey key = new SchemaKey(subject, version);
      try {
        SchemaValue schemaValue = (SchemaValue) kafkaStore.get(key);
        Schema schema = null;
        if ((schemaValue != null && !schemaValue.isDeleted()) || returnDeletedSchema) {
          schema = getSchemaEntityFromSchemaValue(schemaValue);
        }
        return schema;
      } catch (StoreException e) {
        throw new SchemaRegistryStoreException(
            "Error while retrieving schema from the backend Kafka"
            + " store", e);
      }
    }
  }

  @Override
  public SchemaString get(int id) throws SchemaRegistryException {
    SchemaValue schema = null;
    try {
      SchemaKey subjectVersionKey = lookupCache.schemaKeyById(id);
      if (subjectVersionKey == null) {
        return null;
      }
      schema = (SchemaValue) kafkaStore.get(subjectVersionKey);
      if (schema == null) {
        return null;
      }
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error while retrieving schema with id "
          + id
          + " from the backend Kafka"
          + " store", e);
    }
    SchemaString schemaString = new SchemaString();
    schemaString.setSchemaString(schema.getSchema());
    return schemaString;
  }

  @Override
  public Set<String> listSubjects() throws SchemaRegistryException {
    try {
      Iterator<SchemaRegistryKey> allKeys = kafkaStore.getAllKeys();
      return extractUniqueSubjects(allKeys);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  private Set<String> extractUniqueSubjects(Iterator<SchemaRegistryKey> allKeys)
      throws StoreException {
    Set<String> subjects = new HashSet<String>();
    while (allKeys.hasNext()) {
      SchemaRegistryKey k = allKeys.next();
      if (k instanceof SchemaKey) {
        SchemaKey key = (SchemaKey) k;
        SchemaValue value = (SchemaValue) kafkaStore.get(key);
        if (value != null && !value.isDeleted()) {
          subjects.add(key.getSubject());
        }
      }
    }
    return subjects;
  }

  public boolean hasSubjects(String subject) throws SchemaRegistryStoreException {
    try {
      return lookupCache.hasSubjects(subject);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public Iterator<Schema> getAllVersions(String subject, boolean returnDeletedSchemas)
      throws SchemaRegistryException {
    try {
      SchemaKey key1 = new SchemaKey(subject, MIN_VERSION);
      SchemaKey key2 = new SchemaKey(subject, MAX_VERSION);
      Iterator<SchemaRegistryValue> allVersions = kafkaStore.getAll(key1, key2);
      return sortSchemasByVersion(allVersions, returnDeletedSchemas).iterator();
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public Schema getLatestVersion(String subject) throws SchemaRegistryException {
    try {
      SchemaKey key1 = new SchemaKey(subject, MIN_VERSION);
      SchemaKey key2 = new SchemaKey(subject, MAX_VERSION);
      Iterator<SchemaRegistryValue> allVersions = kafkaStore.getAll(key1, key2);
      Vector<Schema> sortedVersions = sortSchemasByVersion(allVersions, false);
      Schema latestSchema = null;
      if (sortedVersions.size() > 0) {
        latestSchema = sortedVersions.lastElement();
      }
      return latestSchema;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public void close() {
    log.info("Shutting down schema registry");
    kafkaStore.close();
    if (masterElector != null) {
      masterElector.close();
    }
  }

  public void updateCompatibilityLevel(String subject, AvroCompatibilityLevel newCompatibilityLevel)
      throws SchemaRegistryStoreException, UnknownMasterException, OperationNotPermittedException {
    if (getModeInScope(subject) == Mode.READONLY) {
      throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
    }
    ConfigKey configKey = new ConfigKey(subject);
    try {
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(initTimeout);
      kafkaStore.put(configKey, new ConfigValue(newCompatibilityLevel));
      log.debug("Wrote new compatibility level: " + newCompatibilityLevel.name + " to the"
                + " Kafka data store with key " + configKey.toString());
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store",
                                             e);
    }
  }

  public void updateConfigOrForward(String subject, AvroCompatibilityLevel newCompatibilityLevel,
                                    Map<String, String> headerProperties)
      throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
             UnknownMasterException, OperationNotPermittedException {
    synchronized (masterLock) {
      if (isMaster()) {
        updateCompatibilityLevel(subject, newCompatibilityLevel);
      } else {
        // forward update config request to the master
        if (masterIdentity != null) {
          forwardUpdateCompatibilityLevelRequestToMaster(subject, newCompatibilityLevel,
                                                         headerProperties);
        } else {
          throw new UnknownMasterException("Update config request failed since master is "
                                           + "unknown");
        }
      }
    }
  }

  public AvroCompatibilityLevel getCompatibilityLevel(String subject)
      throws SchemaRegistryStoreException {
    return lookupCache.compatibilityLevel(subject, false, defaultCompatibilityLevel);
  }

  private AvroCompatibilityLevel getCompatibilityLevelInScope(String subject)
      throws SchemaRegistryStoreException {
    return lookupCache.compatibilityLevel(subject, true, defaultCompatibilityLevel);
  }

  @Override
  public boolean isCompatible(String subject,
                              String newSchemaObj,
                              String latestSchema)
      throws SchemaRegistryException {
    if (latestSchema == null) {
      throw new InvalidSchemaException(
          "Latest schema not provided");
    }
    return isCompatible(subject, newSchemaObj, Collections.singletonList(latestSchema));
  }

  /**
   * @param previousSchemas Full schema history in chronological order
   */
  @Override
  public boolean isCompatible(String subject,
                              String newSchemaObj,
                              List<String> previousSchemas)
      throws SchemaRegistryException {

    if (previousSchemas == null || previousSchemas.isEmpty()) {
      throw new InvalidSchemaException(
          "Previous schema not provided");
    }

    List<org.apache.avro.Schema> previousAvroSchemas = new ArrayList<>(previousSchemas.size());
    for (String previousSchema : previousSchemas) {
      if (previousSchema == null) {
        throw new InvalidSchemaException(
            "Existing schema " + previousSchema + " is not a valid Avro schema");
      }
      AvroSchema previousAvroSchema = AvroUtils.parseSchema(previousSchema);
      previousAvroSchemas.add(previousAvroSchema.schemaObj);
    }

    AvroCompatibilityLevel compatibility = getCompatibilityLevelInScope(subject);
    return compatibility.compatibilityChecker
        .isCompatible(AvroUtils.parseSchema(newSchemaObj).schemaObj, previousAvroSchemas);
  }

  private void deleteMode(String subject) throws StoreException {
    ModeKey modeKey = new ModeKey(subject);
    this.kafkaStore.delete(modeKey);
  }

  private void deleteSubjectCompatibility(String subject) throws StoreException {
    ConfigKey configKey = new ConfigKey(subject);
    this.kafkaStore.delete(configKey);
  }

  public Mode getMode(String subject) throws SchemaRegistryStoreException {
    return lookupCache.mode(subject, false, defaultMode);
  }

  private Mode getModeInScope(String subject) throws SchemaRegistryStoreException {
    return lookupCache.mode(subject, true, defaultMode);
  }

  public void setMode(String subject, Mode mode)
      throws SchemaRegistryStoreException, OperationNotPermittedException {
    if (!allowModeChanges) {
      throw new OperationNotPermittedException("Mode changes are not allowed");
    }
    ModeKey modeKey = new ModeKey(subject);
    try {
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(initTimeout);
      if (mode == Mode.IMPORT && getMode(subject) != Mode.IMPORT) {
        // Changing to import mode requires that no schemas exist with matching subjects.
        if (hasSubjects(subject)) {
          throw new OperationNotPermittedException("Cannot import since found existing subjects");
        }
        // At this point no schemas should exist with matching subjects.
        // Write an event to clear deleted schemas from the caches.
        kafkaStore.put(new ClearSubjectKey(subject), new ClearSubjectValue(subject));
      }
      kafkaStore.put(modeKey, new ModeValue(mode));
      log.debug("Wrote new mode: " + mode.name() + " to the"
          + " Kafka data store with key " + modeKey.toString());
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new mode to the store", e);
    }
  }

  public void setModeOrForward(String subject, Mode mode, Map<String, String> headerProperties)
      throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
      OperationNotPermittedException, UnknownMasterException {
    synchronized (masterLock) {
      if (isMaster()) {
        setMode(subject, mode);
      } else {
        // forward update mode request to the master
        if (masterIdentity != null) {
          forwardSetModeRequestToMaster(subject, mode, headerProperties);
        } else {
          throw new UnknownMasterException("Update mode request failed since master is "
              + "unknown");
        }
      }
    }
  }

  KafkaStore<SchemaRegistryKey, SchemaRegistryValue> getKafkaStore() {
    return this.kafkaStore;
  }

  private Vector<Schema> sortSchemasByVersion(Iterator<SchemaRegistryValue> schemas,
                                              boolean returnDeletedSchemas) {
    Vector<Schema> schemaVector = new Vector<Schema>();
    while (schemas.hasNext()) {
      SchemaValue schemaValue = (SchemaValue) schemas.next();
      if (returnDeletedSchemas || !schemaValue.isDeleted()) {
        schemaVector.add(getSchemaEntityFromSchemaValue(schemaValue));
      }
    }
    Collections.sort(schemaVector);
    return schemaVector;
  }

  private Schema getSchemaEntityFromSchemaValue(SchemaValue schemaValue) {
    if (schemaValue == null) {
      return null;
    }
    return new Schema(schemaValue.getSubject(), schemaValue.getVersion(),
                      schemaValue.getId(), schemaValue.getSchema());
  }

  private boolean isSubjectVersionDeleted(String subject, int version)
      throws SchemaRegistryException {
    try {
      SchemaValue schemaValue = (SchemaValue) this.kafkaStore.get(new SchemaKey(subject, version));
      return schemaValue == null || schemaValue.isDeleted();
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error while retrieving schema from the backend Kafka"
          + " store", e);
    }
  }

  public HostnameVerifier getHostnameVerifier() throws SchemaRegistryStoreException {
    String sslEndpointIdentificationAlgo =
        config.getString(RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);

    if (sslEndpointIdentificationAlgo == null
        || sslEndpointIdentificationAlgo.equals("none")
        || sslEndpointIdentificationAlgo.isEmpty()) {
      return (hostname, session) -> true;
    }

    if (sslEndpointIdentificationAlgo.equalsIgnoreCase("https")) {
      return null;
    }

    throw new SchemaRegistryStoreException(
        RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
            + " "
            + sslEndpointIdentificationAlgo
            + " not supported");
  }

  public static class SchemeAndPort {
    public int port;
    public String scheme;

    public SchemeAndPort(String scheme, int port) {
      this.port = port;
      this.scheme = scheme;
    }
  }
}
