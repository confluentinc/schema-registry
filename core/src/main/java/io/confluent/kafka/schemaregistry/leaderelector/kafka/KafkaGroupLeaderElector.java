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

package io.confluent.kafka.schemaregistry.leaderelector.kafka;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryInitializationException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.LeaderElector;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaGroupLeaderElector implements LeaderElector, SchemaRegistryRebalanceListener {

  private static final Logger log = LoggerFactory.getLogger(KafkaGroupLeaderElector.class);

  private static final AtomicInteger SR_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
  private static final String JMX_PREFIX = "kafka.schema.registry";

  private final int initTimeout;
  private final String clientId;
  private final ConsumerNetworkClient client;
  private final Metrics metrics;
  private final Metadata metadata;
  private final long retryBackoffMs;
  private final boolean stickyLeaderElection;
  private final SchemaRegistryCoordinator coordinator;
  private final KafkaSchemaRegistry schemaRegistry;

  private AtomicBoolean stopped = new AtomicBoolean(false);
  private ExecutorService executor;
  private CountDownLatch joinedLatch = new CountDownLatch(1);

  public KafkaGroupLeaderElector(SchemaRegistryConfig config,
                                 SchemaRegistryIdentity myIdentity,
                                 KafkaSchemaRegistry schemaRegistry
  ) throws SchemaRegistryInitializationException {
    try {
      this.schemaRegistry = schemaRegistry;

      clientId = "sr-" + SR_CLIENT_ID_SEQUENCE.getAndIncrement();

      Map<String, String> metricsTags = new LinkedHashMap<>();
      metricsTags.put("client-id", clientId);
      this.stickyLeaderElection = config.getBoolean(SchemaRegistryConfig.LEADER_ELECTION_STICKY);
      long sampleWindowMs = config.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG);
      MetricConfig metricConfig = new MetricConfig()
          .samples(config.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
          .timeWindow(sampleWindowMs, TimeUnit.MILLISECONDS)
          .tags(metricsTags);
      List<MetricsReporter>
          reporters = config.getConfiguredInstances(
          CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
          MetricsReporter.class
      );
      reporters.add(new JmxReporter());
      MetricsContext metricsContext = schemaRegistry.getMetricsContainer().getMetricsContext();

      Time time = Time.SYSTEM;

      ClientConfig clientConfig = new ClientConfig(config.originalsWithPrefix("kafkastore."),
          false);

      this.metrics = new Metrics(metricConfig, reporters, time, metricsContext);
      this.retryBackoffMs = clientConfig.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
      String groupId = config.getString(SchemaRegistryConfig.SCHEMAREGISTRY_GROUP_ID_CONFIG);
      LogContext logContext = new LogContext("[Schema registry clientId=" + clientId + ", groupId="
          + groupId + "] ");
      this.metadata = new Metadata(
          retryBackoffMs,
          clientConfig.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
          logContext,
          new ClusterResourceListeners()
      );
      List<String> bootstrapServers
          = config.getList(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG);
      List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(bootstrapServers,
          clientConfig.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG));
      this.metadata.bootstrap(addresses);
      String metricGrpPrefix = "kafka.schema.registry";

      ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(
          clientConfig,
          time,
          logContext);
      long maxIdleMs = clientConfig.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG);

      NetworkClient netClient = new NetworkClient(
          new Selector(maxIdleMs, metrics, time, metricGrpPrefix, channelBuilder, logContext),
          this.metadata,
          clientId,
          100, // a fixed large enough value will suffice
          clientConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
          clientConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
          clientConfig.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
          clientConfig.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
          clientConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
              10000L ,
              127000L ,
          time,
          true,
          new ApiVersions(),
          logContext);

      this.client = new ConsumerNetworkClient(
          logContext,
          netClient,
          metadata,
          time,
          retryBackoffMs,
          clientConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
          Integer.MAX_VALUE
      );
      this.coordinator = new SchemaRegistryCoordinator(
          logContext,
          this.client,
          groupId,
          config.getInt(SchemaRegistryConfig.KAFKAGROUP_REBALANCE_TIMEOUT_MS_CONFIG),
          config.getInt(SchemaRegistryConfig.KAFKAGROUP_SESSION_TIMEOUT_MS_CONFIG),
          config.getInt(SchemaRegistryConfig.KAFKAGROUP_HEARTBEAT_INTERVAL_MS_CONFIG),
          metrics,
          metricGrpPrefix,
          time,
          retryBackoffMs,
          myIdentity,
          this,
          schemaRegistry.getMetricsContainer().getNodeCountMetric(),
          stickyLeaderElection
      );

      AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());

      initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);

      log.debug("Schema registry group member created");
    } catch (Throwable t) {
      // call close methods if internal objects are already constructed
      // this is to prevent resource leak. see KAFKA-2121
      stop(true);
      // now propagate the exception
      throw new SchemaRegistryInitializationException("Failed to construct kafka consumer", t);
    }
  }

  @Override
  public void init() throws SchemaRegistryTimeoutException, SchemaRegistryStoreException {
    log.debug("Initializing schema registry group member");

    executor = Executors.newSingleThreadExecutor();
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          while (!stopped.get()) {
            coordinator.poll(Integer.MAX_VALUE);
          }
        } catch (WakeupException we) {
          // do nothing because the thread is closing -- see stop()
        } catch (Throwable t) {
          log.error("Unexpected exception in schema registry group processing thread", t);
        }
      }
    });

    try {
      if (!joinedLatch.await(initTimeout, TimeUnit.MILLISECONDS)) {
        throw new SchemaRegistryTimeoutException("Timed out waiting for join group to complete");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SchemaRegistryStoreException("Interrupted while waiting for join group to "
                                             + "complete", e);
    }

    log.debug("Schema registry group member initialized and joined group");
  }

  @Override
  public void close() {
    if (stopped.get()) {
      return;
    }
    stop(false);
  }

  @Override
  public void onAssigned(SchemaRegistryProtocol.Assignment assignment, int generation) {
    log.info("Finished rebalance with leader election result: {}", assignment);
    try {
      switch (assignment.error()) {
        case SchemaRegistryProtocol.Assignment.NO_ERROR:
          if (assignment.leaderIdentity() == null) {
            log.error(
                "No leader eligible schema registry instances joined the schema registry group. "
                + "Rebalancing was successful and this instance can serve reads, but no writes "
                + "can be processed."
            );
          }
          schemaRegistry.setLeader(assignment.leaderIdentity());
          joinedLatch.countDown();
          break;
        case SchemaRegistryProtocol.Assignment.DUPLICATE_URLS:
          throw new IllegalStateException(
              "The schema registry group contained multiple members advertising the same URL. "
              + "Verify that each instance has a unique, routable listener by setting the "
              + "'listeners' configuration. This error may happen if executing in containers "
              + "where the default hostname is 'localhost'."
          );
        default:
          throw new IllegalStateException("Unknown error returned from schema registry "
                                          + "coordination protocol");
      }
    } catch (SchemaRegistryException e) {
      // This shouldn't be possible with this implementation. The exceptions from setLeader come
      // from it calling nextRange in this class, but this implementation doesn't require doing
      // any IO, so the errors that can occur in the ZK implementation should not be possible here.
      log.error(
          "Error when updating leader, we will not be able to forward requests to the leader",
          e
      );
    }
  }

  /**
   * This is a no-op during leader election if stickyLeaderElection is enabled as we're not
   * revoking previous leader assignment. The expectation is that the sync group response will
   * notify the group members if there's a leadership change.
   */
  @Override
  public void onRevoked() {
    log.info("Rebalance started");
    if (!stickyLeaderElection) {
      try {
        schemaRegistry.setLeader(null);
      } catch (SchemaRegistryException e) {
        // This shouldn't be possible with this implementation. The exceptions from setLeader come
        // from it calling nextRange in this class, but this implementation doesn't require doing
        // any IO, so the errors that can occur in the ZK implementation should not be possible
        // here.
        log.error(
                "Error when updating leader, we will not be able to forward requests to the leader",
                e
        );
      }
    }
  }

  private void stop(boolean swallowException) {
    log.info("Stopping the schema registry group member.");

    // Interrupt any outstanding poll calls
    if (client != null) {
      client.wakeup();
    }

    // Wait for processing thread to complete
    if (executor != null) {
      executor.shutdown();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Interrupted waiting for schema registry group processing thread to exit",
            e
        );
      }
    }

    // Do final cleanup
    AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
    this.stopped.set(true);
    closeQuietly(coordinator, "coordinator", firstException);
    closeQuietly(metrics, "consumer metrics", firstException);
    closeQuietly(client, "consumer network client", firstException);
    AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
    if (firstException.get() != null && !swallowException) {
      throw new KafkaException(
          "Failed to stop the schema registry group member",
          firstException.get()
      );
    } else {
      log.info("The schema registry group member has stopped.");
    }
  }

  private static void closeQuietly(AutoCloseable closeable,
                                   String name,
                                   AtomicReference<Throwable> firstException
  ) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Throwable t) {
        firstException.compareAndSet(null, t);
        log.error("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
      }
    }
  }
}
