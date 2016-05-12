.. _schemaregistry_config:

Configuration Options
=====================
``kafkastore.connection.url``
  Zookeeper url for the Kafka cluster

  * Type: string
  * Default: ""
  * Importance: high

``avro.compatibility.level``
  The Avro compatibility type. Valid values are: none (new schema can be any valid Avro schema), backward (new schema can read data produced by latest registered schema), forward (latest registered schema can read data produced by the new schema), full (new schema is backward and forward compatible with latest registered schema)

  * Type: string
  * Default: "backward"
  * Importance: high

``kafkastore.topic``
  The durable single partition topic that acts as the durable log for the data

  * Type: string
  * Default: "_schemas"
  * Importance: high

``kafkastore.topic.replication.factor``
  The desired replication factor of the schema topic. The actual replication factor will be the smaller of this value and the number of live Kafka brokers.

  * Type: int
  * Default: 3
  * Importance: high

``response.mediatype.default``
  The default response media type that should be used if no specify types are requested in an Accept header.

  * Type: string
  * Default: "application/vnd.schemaregistry.v1+json"
  * Importance: high

``response.mediatype.preferred``
  An ordered list of the server's preferred media types used for responses, from most preferred to least.

  * Type: list
  * Default: [application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json]
  * Importance: high

``host.name``
  The host name advertised in Zookeeper. Make sure to set this if running SchemaRegistry with multiple nodes.

  * Type: string
  * Default: "localhost"
  * Importance: high

``kafkastore.ssl.truststore.location``
  The location of the SSL trust store file.

  * Type: string
  * Default: ""
  * Importance: high

``kafkastore.ssl.truststore.password``
  The password to access the trust store.

  * Type: password
  * Default: ""
  * Importance: high

``kafkastore.ssl.keystore.location``
  The location of the SSL keystore file.

  * Type: string
  * Default: ""
  * Importance: high

``kafkastore.ssl.keystore.password``
  The password to access the keystore.

  * Type: password
  * Default: ""
  * Importance: high

``kafkastore.ssl.key.password``
  The password of the key contained in the keystore.

  * Type: password
  * Default: ""
  * Importance: high

``kafkastore.security.protocol``
  The security protocol to use when connecting with Kafka, the underlying persistent storage. Values can be `PLAINTEXT` or `SSL`.

  * Type: string
  * Default: "PLAINTEXT"
  * Importance: medium

``kafkastore.init.timeout.ms``
  The timeout for initialization of the Kafka store, including creation of the Kafka topic that stores schema data.

  * Type: int
  * Default: 60000
  * Importance: medium

``kafkastore.timeout.ms``
  The timeout for an operation on the Kafka store

  * Type: int
  * Default: 500
  * Importance: medium

``master.eligibility``
  If true, this node can participate in master election. In a multi-colo setup, turn this offfor clusters in the slave data center.

  * Type: boolean
  * Default: true
  * Importance: medium

``kafkastore.ssl.enabled.protocols``
  The list of protocols enabled for SSL.

  * Type: string
  * Default: "TLSv1.2,TLSv1.1,TLSv1"
  * Importance: medium

``kafkastore.ssl.keystore.type``
  The file format of the keystore file.

  * Type: string
  * Default: "JKS"
  * Importance: medium

``kafkastore.ssl.protocol``
  The SSL protocol used to generate the SSLContext.

  * Type: string
  * Default: "TLS"
  * Importance: medium

``kafkastore.ssl.provider``
  The name of the securitiy provider used for SSL.

  * Type: string
  * Default: ""
  * Importance: medium

``kafkastore.ssl.truststore.type``
  The file format of the trust store file.

  * Type: string
  * Default: "JKS"
  * Importance: medium

``access.control.allow.origin``
  Set value for Jetty Access-Control-Allow-Origin header

  * Type: string
  * Default: ""
  * Importance: low

``debug``
  Boolean indicating whether extra debugging information is generated in some error response entities.

  * Type: boolean
  * Default: false
  * Importance: low

``kafkastore.zk.session.timeout.ms``
  Zookeeper session timeout

  * Type: int
  * Default: 30000
  * Importance: low

``metric.reporters``
  A list of classes to use as metrics reporters. Implementing the <code>MetricReporter</code> interface allows plugging in classes that will be notified of new metric creation. The JmxReporter is always included to register JMX statistics.

  * Type: list
  * Default: []
  * Importance: low

``metrics.jmx.prefix``
  Prefix to apply to metric names for the default JMX reporter.

  * Type: string
  * Default: "kafka.schema.registry"
  * Importance: low

``metrics.num.samples``
  The number of samples maintained to compute metrics.

  * Type: int
  * Default: 2
  * Importance: low

``metrics.sample.window.ms``
  The metrics system maintains a configurable number of samples over a fixed window size. This configuration controls the size of the window. For example we might maintain two samples each measured over a 30 second period. When a window expires we erase and overwrite the oldest window.

  * Type: long
  * Default: 30000
  * Importance: low

``port``
  Port to listen on for new connections.

  * Type: int
  * Default: 8081
  * Importance: low

``request.logger.name``
  Name of the SLF4J logger to write the NCSA Common Log Format request log.

  * Type: string
  * Default: "io.confluent.rest-utils.requests"
  * Importance: low

``schema.registry.zk.namespace``
  The string that is used as the zookeeper namespace for storing schema registry metadata. SchemaRegistry instances which are part of the same schema registry service should have the same ZooKeeper namespace.

  * Type: string
  * Default: "schema_registry"
  * Importance: low

``shutdown.graceful.ms``
  Amount of time to wait after a shutdown request for outstanding requests to complete.

  * Type: int
  * Default: 1000
  * Importance: low

``kafkastore.ssl.cipher.suites``
  A list of cipher suites used for SSL.

  * Type: string
  * Default: ""
  * Importance: low

``kafkastore.ssl.endpoint.identification.algorithm``
  The endpoint identification algorithm to validate the server hostname using the server certificate.

  * Type: string
  * Default: ""
  * Importance: low

``kafkastore.ssl.keymanager.algorithm``
  The algorithm used by key manager factory for SSL connections.

  * Type: string
  * Default: "SunX509"
  * Importance: low

``kafkastore.ssl.trustmanager.algorithm``
  The algorithm used by the trust manager factory for SSL connections.

  * Type: string
  * Default: "PKIX"
  * Importance: low
