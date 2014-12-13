package io.confluent.kafka.schemaregistry.rest;

import io.confluent.rest.Configuration;
import io.confluent.rest.ConfigurationException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.util.Properties;

/**
 * Created by nnarkhed
 */
public class SchemaRegistryConfig extends Configuration {
    public Time time;

    public boolean debug;
    public static final String DEFAULT_DEBUG = "false";

    public int port;
    public static final String DEFAULT_PORT = "8080";

    public String zookeeperConnect;
    public static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";

    public String bootstrapServers;
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    public int producerThreads;
    public static final String DEFAULT_PRODUCER_THREADS = "5";

    /**
     * The consumer timeout used to limit consumer iterator operations. This is effectively the maximum error for the
     * entire request timeout. It should be small enough to get reasonably close to the timeout, but large enough to
     * not result in busy waiting.
     */
    public int consumerIteratorTimeoutMs;
    public static final String DEFAULT_CONSUMER_ITERATOR_TIMEOUT_MS = "50";

    /** The maximum total time to wait for messages for a request if the maximum number of messages has not yet been reached. */
    public int consumerRequestTimeoutMs;
    public static final String DEFAULT_CONSUMER_REQUEST_TIMEOUT_MS = "1000";

    /** The maximum number of messages returned in a single request. */
    public int consumerRequestMaxMessages;
    public static final String DEFAULT_CONSUMER_REQUEST_MAX_MESSAGES = "100";

    public int consumerThreads;
    public static final String DEFAULT_CONSUMER_THREADS = "1";

    /** Amount of idle time before a consumer instance is automatically destroyed. */
    public int consumerInstanceTimeoutMs;
    public static final String DEFAULT_CONSUMER_INSTANCE_TIMEOUT_MS = "300000";

    public SchemaRegistryConfig() throws ConfigurationException {
        this(new Properties());
    }

    public SchemaRegistryConfig(Properties props) throws ConfigurationException {
        time = new SystemTime();

        debug = Boolean.parseBoolean(props.getProperty("debug", DEFAULT_DEBUG));

        port = Integer.parseInt(props.getProperty("port", DEFAULT_PORT));
        zookeeperConnect = props.getProperty("zookeeper.connect", DEFAULT_ZOOKEEPER_CONNECT);
        bootstrapServers = props.getProperty("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
        producerThreads = Integer.parseInt(props.getProperty("producer.threads", DEFAULT_PRODUCER_THREADS));
        consumerIteratorTimeoutMs = Integer.parseInt(props.getProperty("consumer.iterator.timeout.ms", DEFAULT_CONSUMER_ITERATOR_TIMEOUT_MS));
        consumerRequestTimeoutMs = Integer.parseInt(props.getProperty("consumer.request.timeout.ms", DEFAULT_CONSUMER_REQUEST_TIMEOUT_MS));
        consumerRequestMaxMessages = Integer.parseInt(props.getProperty("consumer.request.max.messages", DEFAULT_CONSUMER_REQUEST_MAX_MESSAGES));
        consumerThreads = Integer.parseInt(props.getProperty("consumer.threads", DEFAULT_CONSUMER_THREADS));
        consumerInstanceTimeoutMs = Integer.parseInt(props.getProperty("consumer.instance.timeout.ms", DEFAULT_CONSUMER_INSTANCE_TIMEOUT_MS));
    }

}
