package io.confluent.kafka.schemaregistry.rest;

import io.confluent.rest.Configuration;
import io.confluent.rest.ConfigurationException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SchemaRegistryRestConfiguration extends Configuration {
    public Time time;

    /**
     * Unique ID for this REST server instance. This is used in generating unique IDs for consumers that do not specify
     * their ID. The ID is empty by default, which makes a single server setup easier to get up and running, but is not
     * safe for multi-server deployments where automatic consumer IDs are used.
     */
    public String id;
    public static final String DEFAULT_ID = "";

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
     * The consumer timeout used to limit consumer iterator operations. This should be very small so we can effectively
     * peek() on the iterator.
     */
    public int consumerIteratorTimeoutMs;
    public static final String DEFAULT_CONSUMER_ITERATOR_TIMEOUT_MS = "1";

    /**
     * Amount of time to backoff when an iterator runs out of data. When a consumer has a dedicated worker thread, this
     * is effectively the maximum error for the entire request timeout. It should be small enough to get reasonably close
     * to the timeout, but large enough to avoid busy waiting.
     */
    public int consumerIteratorBackoffMs;
    public static final String DEFAULT_CONSUMER_ITERATOR_BACKOFF_MS = "50";

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
        bootstrapServers = props.getProperty("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
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

    @Override
    public boolean getDebug() {
        return debug;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public Iterable<String> getPreferredResponseMediaTypes() {
        return Versions.PREFERRED_RESPONSE_TYPES;
    }

    @Override
    public String getDefaultResponseMediaType() {
        return Versions.SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT;
    }

    private static Properties getPropsFromFile(String propsFile) throws ConfigurationException {
        Properties props = new Properties();
        if (propsFile == null) return props;
        try {
            FileInputStream propStream = new FileInputStream(propsFile);
            props.load(propStream);
        } catch (IOException e) {
            throw new ConfigurationException("Couldn't load properties from " + propsFile, e);
        }
        return props;
    }}
