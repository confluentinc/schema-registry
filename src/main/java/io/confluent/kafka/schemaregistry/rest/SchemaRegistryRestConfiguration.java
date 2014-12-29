/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.schemaregistry.rest;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.rest.Configuration;
import io.confluent.rest.ConfigurationException;

public class SchemaRegistryRestConfiguration extends Configuration {

  public static final String DEFAULT_ID = "";
  public static final String DEFAULT_DEBUG = "false";
  public static final String DEFAULT_PORT = "8080";
  public static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";
  public static final String DEFAULT_KAFKASTORE_TOPIC = "_schemas";
  public static final String DEFAULT_PRODUCER_THREADS = "5";
  public static final String DEFAULT_CONSUMER_ITERATOR_TIMEOUT_MS = "1";
  public static final String DEFAULT_CONSUMER_ITERATOR_BACKOFF_MS = "50";
  public static final String DEFAULT_CONSUMER_REQUEST_TIMEOUT_MS = "1000";
  public static final String DEFAULT_CONSUMER_REQUEST_MAX_MESSAGES = "100";
  public static final String DEFAULT_CONSUMER_THREADS = "1";
  public static final String DEFAULT_CONSUMER_INSTANCE_TIMEOUT_MS = "300000";
  public Time time;
  /**
   * Unique ID for this REST server instance. This is used in generating unique IDs for consumers
   * that do not specify their ID. The ID is empty by default, which makes a single server setup
   * easier to get up and running, but is not safe for multi-server deployments where automatic
   * consumer IDs are used.
   */
  public String id;
  public boolean debug;
  public int port;
  public String zookeeperConnect;
  public String kafkastoreTopic;
  public int producerThreads;
  /**
   * The consumer timeout used to limit consumer iterator operations. This should be very small so
   * we can effectively peek() on the iterator.
   */
  public int consumerIteratorTimeoutMs;
  /**
   * Amount of time to backoff when an iterator runs out of data. When a consumer has a dedicated
   * worker thread, this is effectively the maximum error for the entire request timeout. It should
   * be small enough to get reasonably close to the timeout, but large enough to avoid busy
   * waiting.
   */
  public int consumerIteratorBackoffMs;
  /**
   * The maximum total time to wait for messages for a request if the maximum number of messages has
   * not yet been reached.
   */
  public int consumerRequestTimeoutMs;
  /**
   * The maximum number of messages returned in a single request.
   */
  public int consumerRequestMaxMessages;
  public int consumerThreads;
  /**
   * Amount of idle time before a consumer instance is automatically destroyed.
   */
  public int consumerInstanceTimeoutMs;

  public SchemaRegistryRestConfiguration() throws ConfigurationException {
    this(new Properties());
  }

  public SchemaRegistryRestConfiguration(String propsFile) throws ConfigurationException {
    this(getPropsFromFile(propsFile));
  }

  public SchemaRegistryRestConfiguration(Properties props) throws ConfigurationException {
    time = new SystemTime();

    id = props.getProperty("id", DEFAULT_ID);

    debug = Boolean.parseBoolean(props.getProperty("debug", DEFAULT_DEBUG));

    port = Integer.parseInt(props.getProperty("port", DEFAULT_PORT));
    zookeeperConnect = props.getProperty("zookeeper.connect", DEFAULT_ZOOKEEPER_CONNECT);
    kafkastoreTopic = props.getProperty("kafkastore.topic", DEFAULT_KAFKASTORE_TOPIC);

    producerThreads = Integer.parseInt(props.getProperty("producer.threads",
                                                         DEFAULT_PRODUCER_THREADS));
    consumerIteratorTimeoutMs = Integer.parseInt(props.getProperty(
        "consumer.iterator.timeout.ms", DEFAULT_CONSUMER_ITERATOR_TIMEOUT_MS));
    consumerIteratorBackoffMs = Integer.parseInt(props.getProperty(
        "consumer.iterator.backoff.ms", DEFAULT_CONSUMER_ITERATOR_BACKOFF_MS));
    consumerRequestTimeoutMs = Integer.parseInt(props.getProperty("consumer.request.timeout.ms",
                                                                  DEFAULT_CONSUMER_REQUEST_TIMEOUT_MS));
    consumerRequestMaxMessages = Integer.parseInt(props.getProperty(
        "consumer.request.max.messages", DEFAULT_CONSUMER_REQUEST_MAX_MESSAGES));
    consumerThreads = Integer.parseInt(props.getProperty("consumer.threads",
                                                         DEFAULT_CONSUMER_THREADS));
    consumerInstanceTimeoutMs = Integer.parseInt(props.getProperty(
        "consumer.instance.timeout.ms", DEFAULT_CONSUMER_INSTANCE_TIMEOUT_MS));
  }

  private static Properties getPropsFromFile(String propsFile) throws ConfigurationException {
    Properties props = new Properties();
    if (propsFile == null) {
      return props;
    }
    try {
      FileInputStream propStream = new FileInputStream(propsFile);
      props.load(propStream);
    } catch (IOException e) {
      throw new ConfigurationException("Couldn't load properties from " + propsFile, e);
    }
    return props;
  }
}
