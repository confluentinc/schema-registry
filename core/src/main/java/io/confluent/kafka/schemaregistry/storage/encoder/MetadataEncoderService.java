/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage.encoder;

import com.google.common.annotations.VisibleForTesting;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeyTemplate;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.KeysetManager;
import com.google.crypto.tink.Registry;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.proto.AesGcmKey;
import com.google.crypto.tink.subtle.Hkdf;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.MD5;
import io.confluent.kafka.schemaregistry.storage.Metadata;
import io.confluent.kafka.schemaregistry.storage.MetadataEncoderStoreReaderThread;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.kcache.Cache;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.Caches;
import io.kcache.utils.InMemoryCache;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class MetadataEncoderService implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MetadataEncoderService.class);

  private static final String AES_GCM_KEY = "type.googleapis.com/google.crypto.tink.AesGcmKey";
  private static final byte[] EMPTY_AAD = new byte[0];
  private static final String KEY_TEMPLATE_NAME = "AES128_GCM";

  private final KafkaSchemaRegistry schemaRegistry;
  private KeyTemplate keyTemplate;
  // visible for testing
  Cache<String, KeysetWrapper> encoders = null;
  private final AtomicBoolean initialized = new AtomicBoolean();
  private final CountDownLatch initLatch = new CountDownLatch(1);

  // new
  private final String bootstrapBrokers;
  private final String topic;
  private final String groupId;
  private Producer<byte[], byte[]> producer;
  private final String noopKey;
  private SchemaRegistryConfig config;
  private final ResourceManagerStoreUpdateHandler rmStoreUpdateHandler;

  private MetadataEncoderStoreReaderThread<String, KeysetWrapper> metadataEncoderStoreReaderThread;

  static {
    try {
      AeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Inject
  public MetadataEncoderService(SchemaRegistry schemaRegistry) {
    this.producer = null;  // Initialize before try block
    try {
      this.schemaRegistry = (KafkaSchemaRegistry) schemaRegistry;
      this.config = schemaRegistry.config();

      this.rmStoreUpdateHandler = new ResourceManagerStoreUpdateHandler(config);

      log.info("LocalMES running");
      
      // Initialize final fields first
      this.bootstrapBrokers = config.bootstrapBrokers();
      this.topic = config.getString(SchemaRegistryConfig.METADATA_ENCODER_TOPIC_CONFIG);
      this.groupId = config.getString(SchemaRegistryConfig.KAFKASTORE_GROUP_ID_CONFIG);
      this.noopKey = "noop";
      
      String secret = encoderSecret(config);
      if (secret == null) {
        log.warn("No value specified for {}, sensitive metadata will not be encoded",
            SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG);
        return;
      }
      
      // Create producer after initializing fields
      Properties props = new Properties();
      props.putAll(config.originalsWithPrefix("metadata.encoder"));
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapBrokers);
      props.put(ProducerConfig.ACKS_CONFIG, "-1");
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      this.producer = new KafkaProducer<>(props);
      
      // Rest of the initialization
      this.keyTemplate = KeyTemplates.get(KEY_TEMPLATE_NAME);
      this.encoders = createCache(new StringSerde(), new KeysetWrapperSerde(config), this.topic, null);
      // sending dummyRM
      log.info("Test client connection");
      sendDummyRM();

      // Create reader thread
      this.metadataEncoderStoreReaderThread = new MetadataEncoderStoreReaderThread<>(
          this.bootstrapBrokers,
          this.topic,
          this.groupId,
          null, // storeUpdateHandler - we don't need this
          new KeysetWrapperSerde(config),// localStore - we don't need this
          this.producer,
          this.noopKey,
          this.initialized,
          config
      );

      init(); // manual trigger?

    } catch (Exception e) {
      throw new IllegalArgumentException("Could not instantiate MetadataEncoderService", e);
    }
  }

  public static void addSchemaRegistryConfigsToClientProperties(SchemaRegistryConfig config,
                                                                Properties props) {
    props.putAll(config.originalsWithPrefix("metadata.encoder"));
  }

  private void createOrVerifyEncoderTopic() throws StoreInitializationException {

    Properties props = new Properties();
    addSchemaRegistryConfigsToClientProperties(this.config, props);
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

    try (AdminClient admin = AdminClient.create(props)) {
      String temp_topic = "_schema_encoders_v0";
      createEncoderTopic(admin, temp_topic); // try hard coding the creation once

      
      Set<String> allTopics = admin.listTopics().names().get(60000, TimeUnit.MILLISECONDS);
      log.info("All topics: {}", allTopics);
      if (allTopics.contains(topic)) {
        log.info("Verifying encoder topic");
        verifyEncoderTopic(admin);
      } else {
        log.info("Creating encoder topic");
        createEncoderTopic(admin);
      }
    } catch (TimeoutException e) {
      throw new StoreInitializationException(
              "Timed out trying to create or validate encoder topic configuration",
              e
      );
    } catch (InterruptedException | ExecutionException e) {
      throw new StoreInitializationException(
              "Failed trying to create or validate encoder topic configuration",
              e
      );
    }
  }

  private void createEncoderTopic(AdminClient admin, String topic) throws StoreInitializationException,
          InterruptedException,
          ExecutionException,
          TimeoutException {
    log.info("Creating encoders topic {}", topic);

    int numLiveBrokers = admin.describeCluster().nodes()
            .get(60000, TimeUnit.MILLISECONDS).size();
    if (numLiveBrokers <= 0) {
      throw new StoreInitializationException("No live Kafka brokers");
    }

    int encoderTopicReplicationFactor = Math.min(numLiveBrokers, 3);
    if (encoderTopicReplicationFactor < 3) {
      log.warn("Creating the schema topic "
              + topic
              + " using a replication factor of "
              + encoderTopicReplicationFactor
              + ", which is less than the desired one of "
              + 3 + ". If this is a production environment, it's "
              + "crucial to add more brokers and increase the replication factor of the topic.");
    }

    NewTopic encoderTopicRequest = new NewTopic(topic, 1, (short) encoderTopicReplicationFactor);
    Map topicConfigs = new HashMap(config.originalsWithPrefix("metadata.encoder.topic.config."));
    topicConfigs.put(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_COMPACT
    );
    encoderTopicRequest.configs(topicConfigs);
    try {
      admin.createTopics(Collections.singleton(encoderTopicRequest)).all()
              .get(60000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        // If topic already exists, ensure that it is configured correctly.
        verifyEncoderTopic(admin);
      } else {
        throw e;
      }
    }
  }

  private void createEncoderTopic(AdminClient admin) throws StoreInitializationException,
          InterruptedException,
          ExecutionException,
          TimeoutException {
    log.info("Creating encoders topic {}", topic);

    int numLiveBrokers = admin.describeCluster().nodes()
            .get(60000, TimeUnit.MILLISECONDS).size();
    if (numLiveBrokers <= 0) {
      throw new StoreInitializationException("No live Kafka brokers");
    }

    int encoderTopicReplicationFactor = Math.min(numLiveBrokers, 3);
    if (encoderTopicReplicationFactor < 3) {
      log.warn("Creating the schema topic "
              + topic
              + " using a replication factor of "
              + encoderTopicReplicationFactor
              + ", which is less than the desired one of "
              + 3 + ". If this is a production environment, it's "
              + "crucial to add more brokers and increase the replication factor of the topic.");
    }

    NewTopic encoderTopicRequest = new NewTopic(topic, 1, (short) encoderTopicReplicationFactor);
    Map topicConfigs = new HashMap(config.originalsWithPrefix("metadata.encoder.topic.config."));
    topicConfigs.put(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_COMPACT
    );
    encoderTopicRequest.configs(topicConfigs);
    try {
      admin.createTopics(Collections.singleton(encoderTopicRequest)).all()
              .get(60000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        // If topic already exists, ensure that it is configured correctly.
        verifyEncoderTopic(admin);
      } else {
        throw e;
      }
    }
  }

  private void verifyEncoderTopic(AdminClient admin) throws StoreInitializationException,
          InterruptedException,
          ExecutionException,
          TimeoutException {
    log.info("Validating encoders topic {}", topic);

    Set<String> topics = Collections.singleton(topic);
    Map<String, TopicDescription> topicDescription = admin.describeTopics(topics)
            .all().get(60000, TimeUnit.MILLISECONDS);

    TopicDescription description = topicDescription.get(topic);
    final int numPartitions = description.partitions().size();
    if (numPartitions != 1) {
      throw new StoreInitializationException("The encoder topic " + topic + " should have only 1 "
              + "partition but has " + numPartitions);
    }

    if (description.partitions().get(0).replicas().size() < 3) {
      log.warn("The replication factor of the encoder topic "
              + topic
              + " is less than the desired one of "
              + "3"
              + ". If this is a production environment, it's crucial to add more brokers and "
              + "increase the replication factor of the topic.");
    }

    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

    Map<ConfigResource, Config> configs =
            admin.describeConfigs(Collections.singleton(topicResource)).all()
                    .get(60000, TimeUnit.MILLISECONDS);
    Config topicConfigs = configs.get(topicResource);
    String retentionPolicy = topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
    log.info("Retention policy for encoder topic {} is {}", topic, retentionPolicy);
    // REMOVE FOR LOCAL DEVELOPMENT
//    if (retentionPolicy == null || !TopicConfig.CLEANUP_POLICY_COMPACT.equals(retentionPolicy)) {
//      log.error("The retention policy of the encoder topic {} is incorrect. "
//              + "You must configure the topic to 'compact' cleanup policy to avoid Kafka "
//              + "deleting your schemas after a week. "
//              + "Refer to Kafka documentation for more details on cleanup policies", topic);
//
//      throw new StoreInitializationException("The retention policy of the encoder topic " + topic
//              + " is incorrect. Expected cleanup.policy to be "
//              + "'compact' but it is " + retentionPolicy);
//
//    }
  }


  @VisibleForTesting
  protected MetadataEncoderService(
      SchemaRegistry schemaRegistry, Cache<String, KeysetWrapper> encoders) {
    this.producer = null;  // Initialize before try block
    try {
      this.schemaRegistry = (KafkaSchemaRegistry) schemaRegistry;
      SchemaRegistryConfig config = schemaRegistry.config();

      this.rmStoreUpdateHandler = new ResourceManagerStoreUpdateHandler(config);

      // Initialize final fields first
      this.bootstrapBrokers = config.bootstrapBrokers();
      this.topic = config.getString(SchemaRegistryConfig.METADATA_ENCODER_TOPIC_CONFIG);
      this.groupId = config.getString(SchemaRegistryConfig.KAFKASTORE_GROUP_ID_CONFIG);
      this.noopKey = "noop";

      
      String secret = encoderSecret(config);
      if (secret == null) {
        log.warn("No value specified for {}, sensitive metadata will not be encoded",
            SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG);
        return;
      }
      this.keyTemplate = KeyTemplates.get(KEY_TEMPLATE_NAME);
      this.encoders = encoders;

      // Create producer after initializing fields
      Properties props = new Properties();
      props.putAll(config.originalsWithPrefix("metadata.encoder"));
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapBrokers);
      props.put(ProducerConfig.ACKS_CONFIG, "-1");
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      this.producer = new KafkaProducer<>(props);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not instantiate MetadataEncoderService", e);
    }
  }

  public KafkaSchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
  }

  @VisibleForTesting
  public Cache<String, KeysetWrapper> getEncoders() {
    return encoders;
  }

  protected static Aead getPrimitive(String secret) throws GeneralSecurityException {
    if (secret == null) {
      throw new IllegalArgumentException("Secret is null");
    }
    byte[] keyBytes = Hkdf.computeHkdf(
        "HmacSha256", secret.getBytes(StandardCharsets.UTF_8), null, null, 16);
    AesGcmKey key = AesGcmKey.newBuilder()
        .setVersion(0)
        .setKeyValue(ByteString.copyFrom(keyBytes))
        .build();
    return Registry.getPrimitive(AES_GCM_KEY, key.toByteString(), Aead.class);
  }

  protected static String encoderSecret(SchemaRegistryConfig config) {
    String secret;
    Password password = config.getPassword(SchemaRegistryConfig.METADATA_ENCODER_SECRET_CONFIG);
    if (password != null) {
      secret = password.value();
    } else {
      secret = System.getenv("METADATA_ENCODER_SECRET");
    }
    return secret;
  }

  protected static String encoderOldSecret(SchemaRegistryConfig config) {
    String secret;
    Password password = config.getPassword(SchemaRegistryConfig.METADATA_ENCODER_OLD_SECRET_CONFIG);
    if (password != null) {
      secret = password.value();
    } else {
      secret = System.getenv("METADATA_ENCODER_OLD_SECRET");
    }
    return secret;
  }

  protected <K, V> Cache<K, V> createCache(
      Serde<K> keySerde,
      Serde<V> valueSerde,
      String topic,
      CacheUpdateHandler<K, V> cacheUpdateHandler) throws CacheInitializationException {
    Properties props = getKafkaCacheProperties(topic);
    KafkaCacheConfig config = new KafkaCacheConfig(props);
    Cache<K, V> kafkaCache = Caches.concurrentCache(
        new KafkaCache<>(config,
            keySerde,
            valueSerde,
            cacheUpdateHandler,
            new InMemoryCache<>()));
    getSchemaRegistry().addLeaderChangeListener(isLeader -> {
      if (isLeader) {
        // Reset the cache to remove any stale data from previous leadership
        kafkaCache.reset();
        // Ensure the new leader catches up with the offsets
        kafkaCache.sync();
      }
    });
    return kafkaCache;
  }

  private Properties getKafkaCacheProperties(String topic) {
    Properties props = new Properties();
    props.putAll(schemaRegistry.config().originalProperties());
    Set<String> keys = props.stringPropertyNames();
    for (String key : keys) {
      if (key.startsWith("kafkastore.")) {
        String newKey = key.replace("kafkastore", "kafkacache");
        if (!keys.contains(newKey)) {
          props.put(newKey, props.get(key));
        }
      }
    }
    props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
    return props;
  }

  public void init() throws StoreInitializationException {
    log.info("Initializing MetadataEncoderService");
    createOrVerifyEncoderTopic();
    if (encoders != null && !initialized.get()) {
      encoders.init();
      maybeRotateSecrets();
      boolean isInitialized = initialized.compareAndSet(false, true);
      if (!isInitialized) {
        throw new IllegalStateException("Metadata encoder service was already initialized");
      }
      // Start the reader thread after initialization
      log.info("Metadata encoder service initialized");
      if (metadataEncoderStoreReaderThread != null) {
        log.info("Metadata encoder service reader thread starting");
        metadataEncoderStoreReaderThread.start();
      }
      initLatch.countDown();
    }
  }

  private void maybeRotateSecrets() {
    String oldSecret = encoderOldSecret(schemaRegistry.config());
    if (oldSecret != null) {
      log.info("Rotating encoder secret");
      for (String key : encoders.keySet()) {
        KeysetWrapper wrapper = encoders.get(key);
        if (wrapper.isRotationNeeded()) {
          try {
            KeysetHandle handle = wrapper.getKeysetHandle();
            KeysetHandle rotatedHandle = KeysetManager
                .withKeysetHandle(handle)
                .add(keyTemplate)
                .getKeysetHandle();
            int keyId = rotatedHandle.getAt(rotatedHandle.size()).getId();
            rotatedHandle = KeysetManager
                .withKeysetHandle(rotatedHandle)
                .setPrimary(keyId)
                .getKeysetHandle();
            // This will cause the new secret to be used
            encoders.put(key, new KeysetWrapper(rotatedHandle, false));
          } catch (GeneralSecurityException e) {
            log.error("Could not rotate key for {}", key, e);
          }
        }
      }
      log.info("Done rotating encoder secret");
    }

  }

  public void waitForInit() throws InterruptedException {
    initLatch.await();
  }

  @VisibleForTesting
  public KeysetHandle getEncoder(String tenant) {
    if (encoders == null) {
      return null;
    }
    KeysetWrapper wrapper = encoders.get(tenant);
    if (wrapper == null) {
      // Ensure encoders are up to date
      encoders.sync();
      wrapper = encoders.get(tenant);
    }
    return wrapper != null ? wrapper.getKeysetHandle() : null;
  }

  public void encodeMetadata(SchemaValue schema) throws SchemaRegistryStoreException {
    log.info("Encode metadata called");
    if (!initialized.get() || schema == null || isEncoded(schema)) {
      return;
    }
    try {
      transformMetadata(schema, true, (aead, value) -> {
        try {
          byte[] ciphertext = aead.encrypt(value.getBytes(StandardCharsets.UTF_8), EMPTY_AAD);
          return Base64.getEncoder().encodeToString(ciphertext);
        } catch (GeneralSecurityException e) {
          throw new IllegalStateException("Could not encrypt sensitive metadata", e);
        }
      });
    } catch (IllegalStateException e) {
      throw new SchemaRegistryStoreException(
          "Could not encrypt metadata for schema id " + schema.getId(), e);
    }
  }

  private boolean isEncoded(SchemaValue schema) {
    if (schema == null
        || schema.getMetadata() == null
        || schema.getMetadata().getProperties() == null) {
      return false;
    }
    return schema.getMetadata().getProperties().containsKey(SchemaValue.ENCODED_PROPERTY);
  }

  public void decodeMetadata(SchemaValue schema) throws SchemaRegistryStoreException {
    if (!initialized.get() || schema == null || !isEncoded(schema)) {
      return;
    }
    try {
      transformMetadata(schema, false, (aead, value) -> {
        try {
          byte[] plaintext = aead.decrypt(Base64.getDecoder().decode(value), EMPTY_AAD);
          return new String(plaintext, StandardCharsets.UTF_8);
        } catch (GeneralSecurityException e) {
          throw new IllegalStateException("Could not decrypt sensitive metadata", e);
        }
      });
    } catch (IllegalStateException e) {
      throw new SchemaRegistryStoreException(
          "Could not decrypt metadata for schema id " + schema.getId(), e);
    }
  }

  private void transformMetadata(
      SchemaValue schema, boolean isEncode, BiFunction<Aead, String, String> func)
      throws SchemaRegistryStoreException {
    Metadata metadata = schema.getMetadata();
    if (metadata == null
        || metadata.getProperties() == null
        || metadata.getProperties().isEmpty()
        || metadata.getSensitive() == null
        || metadata.getSensitive().isEmpty()) {
      return;
    }

    try {
      String subject = schema.getSubject();
      // We pass in the schema registry tenant, but the QualifiedSubject.create method only cares
      // if it is the default tenant; otherwise it will extract the tenant from the subject.
      QualifiedSubject qualifiedSubject = QualifiedSubject.create(schemaRegistry.tenant(), subject);
      String tenant = qualifiedSubject.getTenant();

      // Only create the encoder if we are encoding during writes and not decoding during reads
      KeysetHandle handle = isEncode ? createTestEncoders(tenant) : getEncoder(tenant); // ALWAYS CREATE SO WE CAN TEST
      if (handle == null) {
        throw new SchemaRegistryStoreException("Could not get encoder for tenant " + tenant);
      }
      Aead aead = handle.getPrimitive(Aead.class);

      SortedMap<String, String> newProperties = metadata.getProperties().entrySet().stream()
          .map(e -> new AbstractMap.SimpleEntry<>(
              e.getKey(),
              metadata.getSensitive().contains(e.getKey())
                  ? func.apply(aead, e.getValue())
                  : e.getValue())
          )
          .collect(Collectors.toMap(
              SimpleEntry::getKey,
              SimpleEntry::getValue,
              (e1, e2) -> e2,
              TreeMap::new)
          );
      if (isEncode) {
        schema.setMd5Bytes(MD5.ofSchema(schema.toSchemaEntity()).bytes());
        newProperties.put(SchemaValue.ENCODED_PROPERTY, "true");
      } else {
        newProperties.remove(SchemaValue.ENCODED_PROPERTY);
      }
      log.info("Setting metadata in transformMetadata");

      // attempt to read from RM here
      log.info("Attempting RM parity read");
      rmStoreUpdateHandler.getKeysets(tenant);

      schema.setMetadata(
          new Metadata(metadata.getTags(), newProperties, metadata.getSensitive()));
    } catch (GeneralSecurityException e) {
      throw new SchemaRegistryStoreException("Could not transform schema id " + schema.getId(), e);
    }
  }

  // function to create multiple encoders for testing
  private KeysetHandle createTestEncoders(String tenant) {
    encoders.sync();
    List<KeysetHandle> encoderList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String currTenant = tenant;
      if (i != 0) {
        currTenant = tenant + "_" + i;
      }
      KeysetWrapper wrapper = encoders.compute(currTenant,
          (k, v) -> {
            try {
              log.info("Encoder wasnt present");
              KeysetHandle handle = KeysetHandle.generateNew(keyTemplate);
              return new KeysetWrapper(handle, false);
            } catch (GeneralSecurityException e) {
              throw new IllegalStateException("Could not create key template");
            }

          });
      encoderList.add(wrapper.getKeysetHandle());
    }
    return encoderList.get(0);
  }

//  private KeysetHandle getOrCreateEncoder(String tenant) {
//    // Ensure encoders are up to date
//    encoders.sync();
//    KeysetWrapper wrapper = encoders.computeIfAbsent(tenant,
//        k -> {
//          try {
//            log.info("Encoder wasnt present");
//            KeysetHandle handle = KeysetHandle.generateNew(keyTemplate);
//            return new KeysetWrapper(handle, false);
//          } catch (GeneralSecurityException e) {
//            throw new IllegalStateException("Could not create key template");
//          }
//
//        });
//    log.info("Was encoder present?");
//    return wrapper.getKeysetHandle();
//  }

  // send a test RM create to ensure client is up
  private void sendDummyRM() {
    rmStoreUpdateHandler.sendDummyTransaction();
  }

  @Override
  public void close() {
    if (metadataEncoderStoreReaderThread != null) {
      metadataEncoderStoreReaderThread.shutdown();
      try {
        metadataEncoderStoreReaderThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (producer != null) {
      producer.close();
    }
    if (encoders != null) {
      try {
        encoders.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }
}
