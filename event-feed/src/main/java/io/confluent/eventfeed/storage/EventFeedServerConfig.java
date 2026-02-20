/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.eventfeed.storage;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.rest.RestConfigException;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Properties;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class EventFeedServerConfig extends SchemaRegistryConfig {
  /**
   * <code>event.feed.topic-events.topic</code>
   */
  public static final String EVENT_FEED_TOPIC_EVENTS_TOPIC_CONFIG = "event.feed.topic-events.topic";
  public static final String EVENT_FEED_TOPIC_EVENTS_TOPIC_DEFAULT = "_sr_event_feed_topic_events";
  protected static final String EVENT_FEED_TOPIC_EVENTS_TOPIC_DOC =
          "The Kafka topic name for storing events about Kafka topics (snapshots, deltas, etc.).";

  public EventFeedServerConfig(String propsFile) throws RestConfigException {
    super(propsFile);
  }

  public EventFeedServerConfig(Properties props) throws RestConfigException {
    super(serverConfig, props);
  }

  private static final ConfigDef serverConfig;

  static {
    serverConfig = baseSchemaRegistryConfigDef()
            .define(EVENT_FEED_TOPIC_EVENTS_TOPIC_CONFIG, STRING,
                    EVENT_FEED_TOPIC_EVENTS_TOPIC_DEFAULT,
                    MEDIUM, EVENT_FEED_TOPIC_EVENTS_TOPIC_DOC);
  }
}
