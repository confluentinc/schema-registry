Configuration Options
=====================

``kafkastore.connection.url``
  Zookeeper url for the Kafka cluster

  * Type: string
  * Default: 
  * Importance: high

``avro.compatibility.level``
  The avro compatibility type. Valid values are: none (new schema can be any valid avro schema), backward (new schema can read data produced by latest registered schema), forward (latest registered schema can read data produced by the new schema), full (new schema is backward and forward compatible with latest registered schema)

  * Type: string
  * Default: backward
  * Importance: high

``debug``
  Boolean indicating whether extra debugging information is generated in some error response entities.

  * Type: boolean
  * Default: true
  * Importance: high

``kafkastore.topic``
  The durable single partition topic that actsas the durable log for the data

  * Type: string
  * Default: _schemas
  * Importance: high

``port``
  Port to listen on for new connections.

  * Type: int
  * Default: 8080
  * Importance: high

``response.mediatype.default``
  The default response media type that should be used if no specify types are requested in an Accept header.

  * Type: string
  * Default: application/vnd.schemaregistry.v1+json
  * Importance: high

``response.mediatype.preferred``
  An ordered list of the server's preferred media types used for responses, from most preferred to least.

  * Type: list
  * Default: [application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json]
  * Importance: high

``kafkastore.commit.interval.ms``
  The interval to commit offsets while consuming the Kafka topic

  * Type: int
  * Default: -1
  * Importance: medium

``kafkastore.timeout.ms``
  The timeout for an operation on the Kafka store

  * Type: int
  * Default: 500
  * Importance: medium

``advertised.host``
  The host name advertised in Zookeeper

  * Type: string
  * Default: localhost
  * Importance: low

``kafkastore.zk.session.timeout.ms``
  Zookeeper session timeout

  * Type: int
  * Default: 10000
  * Importance: low

``request.logger.name``
  Name of the SLF4J logger to write the NCSA Common Log Format request log.

  * Type: string
  * Default: io.confluent.rest-utils.requests
  * Importance: low

``shutdown.graceful.ms``
  Amount of time to wait after a shutdown request for outstanding requests to complete.

  * Type: int
  * Default: 1000
  * Importance: low

