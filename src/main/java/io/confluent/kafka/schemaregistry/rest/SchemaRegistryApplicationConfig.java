package io.confluent.kafka.schemaregistry.rest;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.util.Properties;

import io.confluent.rest.Configuration;
import io.confluent.rest.ConfigurationException;

public class SchemaRegistryApplicationConfig extends Configuration {

  public static final String DEFAULT_DEBUG = "false";
  public static final String DEFAULT_PORT = "8080";
  public static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";
  public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
  public static final String DEFAULT_PRODUCER_THREADS = "5";
  public static final String DEFAULT_CONSUMER_ITERATOR_TIMEOUT_MS = "50";
  public static final String DEFAULT_CONSUMER_REQUEST_TIMEOUT_MS = "1000";
  public static final String DEFAULT_CONSUMER_REQUEST_MAX_MESSAGES = "100";
  public static final String DEFAULT_CONSUMER_THREADS = "1";
  public static final String DEFAULT_CONSUMER_INSTANCE_TIMEOUT_MS = "300000";
  public Time time;
  public boolean debug;
  public int port;
  public String zookeeperConnect;
  public String bootstrapServers;
  public int producerThreads;
  /**
   * The consumer timeout used to limit consumer iterator operations. This is effectively the
   * maximum error for the entire request timeout. It should be small enough to get reasonably close
   * to the timeout, but large enough to not result in busy waiting.
   */
  public int consumerIteratorTimeoutMs;
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

  public SchemaRegistryApplicationConfig() throws ConfigurationException {
    this(new Properties());
  }

  public SchemaRegistryApplicationConfig(Properties props) throws ConfigurationException {
    time = new SystemTime();

    debug = Boolean.parseBoolean(props.getProperty("debug", DEFAULT_DEBUG));

    port = Integer.parseInt(props.getProperty("port", DEFAULT_PORT));
    zookeeperConnect = props.getProperty("zookeeper.connect", DEFAULT_ZOOKEEPER_CONNECT);
    bootstrapServers = props.getProperty("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
    producerThreads = Integer.parseInt(props.getProperty("producer.threads",
                                                         DEFAULT_PRODUCER_THREADS));
    consumerIteratorTimeoutMs = Integer.parseInt(props.getProperty(
        "consumer.iterator.timeout.ms", DEFAULT_CONSUMER_ITERATOR_TIMEOUT_MS));
    consumerRequestTimeoutMs = Integer.parseInt(props.getProperty(
        "consumer.request.timeout.ms", DEFAULT_CONSUMER_REQUEST_TIMEOUT_MS));
    consumerRequestMaxMessages = Integer.parseInt(props.getProperty(
        "consumer.request.max.messages", DEFAULT_CONSUMER_REQUEST_MAX_MESSAGES));
    consumerThreads = Integer.parseInt(props.getProperty("consumer.threads",
                                                         DEFAULT_CONSUMER_THREADS));
    consumerInstanceTimeoutMs = Integer.parseInt(props.getProperty(
        "consumer.instance.timeout.ms", DEFAULT_CONSUMER_INSTANCE_TIMEOUT_MS));
  }

}
