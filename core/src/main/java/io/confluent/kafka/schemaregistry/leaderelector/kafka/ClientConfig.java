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

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * o.a.k AbstractConfig that parses configs that all Kafka clients require.
 */
class ClientConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final long METADATA_MAX_AGE_DEFAULT = 5 * 60 * 1000;
  public static final int SEND_BUFFER_DEFAULT = 128 * 1024;
  public static final int RECEIVE_BUFFER_DEFAULT = 64 * 1024;
  public static final long RECONNECT_BACKOFF_MS_DEFAULT = 50L;
  public static final long RECONNECT_BACKOFF_MAX_MS_DEFAULT = 1000L;
  public static final long RETRY_BACKOFF_MS_DEFAULT = 100L;
  public static final long RETRY_BACKOFF_MAX_MS_DEFAULT = 1000L;
  public static final int REQUEST_TIMEOUT_MS_DEFAULT = 305000;
  public static final long CONNECTIONS_MAX_IDLE_MS_DEFAULT = 9 * 60 * 1000;

  static {
    CONFIG = new ConfigDef()
        .define(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
                ConfigDef.Type.STRING,
                ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                   ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
        .define(CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
                ConfigDef.Type.LONG,
                METADATA_MAX_AGE_DEFAULT,
                atLeast(0),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.METADATA_MAX_AGE_DOC)
        .define(CommonClientConfigs.SEND_BUFFER_CONFIG,
                ConfigDef.Type.INT,
                SEND_BUFFER_DEFAULT,
                atLeast(-1),
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.SEND_BUFFER_DOC)
        .define(CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                ConfigDef.Type.INT,
                RECEIVE_BUFFER_DEFAULT,
                atLeast(-1),
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.RECEIVE_BUFFER_DOC)
        .define(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                ConfigDef.Type.LONG,
                RECONNECT_BACKOFF_MS_DEFAULT,
                atLeast(0L),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
        .define(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                ConfigDef.Type.LONG,
                RECONNECT_BACKOFF_MAX_MS_DEFAULT,
                atLeast(0L),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
        .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
                ConfigDef.Type.LONG,
                RETRY_BACKOFF_MS_DEFAULT,
                atLeast(0L),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
        .define(
                CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG,
                ConfigDef.Type.LONG,
                RETRY_BACKOFF_MAX_MS_DEFAULT,
                atLeast(0L),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC
        )
        .define(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                // chosen to be higher than the default of max.poll.interval.ms
                REQUEST_TIMEOUT_MS_DEFAULT,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
        .define(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                ConfigDef.Type.LONG,
                CONNECTIONS_MAX_IDLE_MS_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
        .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                ConfigDef.Type.STRING,
                CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.SECURITY_PROTOCOL_DOC)
        .withClientSslSupport()
        .withClientSaslSupport();

  }

  ClientConfig(Map<String, ?> props) {
    super(CONFIG, props);
  }

  ClientConfig(Map<String, ?> props, boolean doLog) {
    super(CONFIG, props, doLog);
  }
}
