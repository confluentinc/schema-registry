.. shared configuration parameters

kafkastore.connection.url
-------------------------
|zk| URL for the Kafka cluster

* Type: string
* Default: ""
* Importance: high

kafkastore.bootstrap.servers
----------------------------
A list of Kafka brokers to connect to. For example, `PLAINTEXT://hostname:9092,SSL://hostname2:9092`

The effect of this setting depends on whether you specify `kafkastore.connection.url`.

If `kafkastore.connection.url` is not specified, then the Kafka cluster containing these bootstrap servers will be used both to coordinate |sr| instances (master election) and store schema data.

If `kafkastore.connection.url` is specified, then this setting is used to control how |sr| connects to Kafka to store schema data and is particularly important when Kafka security is enabled. When this configuration is not specified, |sr|'s internal Kafka clients will get their Kafka bootstrap server list from |zk| (configured with `kafkastore.connection.url`). In that case, all available listeners matching the `kafkastore.security.protocol` setting will be used.

By specifying this configuration, you can control which endpoints are used to connect to Kafka. Kafka may expose multiple endpoints that all will be stored in |zk|, but |sr| may need to be configured with just one of those endpoints, for example to control which security protocol it uses.

* Type: list
* Default: []
* Importance: medium

.. _sr-listeners:

listeners
---------
Comma-separated list of listeners that listen for API requests over either HTTP or HTTPS. If a listener uses HTTPS, the appropriate SSL configuration parameters need to be set as well.

|sr| identities are stored in |zk| and are made up of a hostname and port. If multiple listeners are configured, the first listener's port is used for its identity.

* Type: list
* Default: "http://0.0.0.0:8081"
* Importance: high

avro.compatibility.level
------------------------
The Avro compatibility type. Valid values are: none (new schema can be any valid Avro schema), backward (new schema can read data produced by latest registered schema), backward_transitive (new schema can read data produced by all previously registered schemas), forward (latest registered schema can read data produced by the new schema), forward_transitive (all previously registered schemas can read data produced by the new schema), full (new schema is backward and forward compatible with latest registered schema), full_transitive (new schema is backward and forward compatible with all previously registered schemas)

* Type: string
* Default: "backward"
* Importance: high

host.name
---------
The host name advertised in |zk|. Make sure to set this if running |sr| with multiple nodes.

* Type: string
* Default: "192.168.50.1"
* Importance: high

kafkastore.ssl.key.password
---------------------------
The password of the key contained in the keystore.

* Type: string
* Default: ""
* Importance: high

kafkastore.ssl.keystore.location
--------------------------------
The location of the SSL keystore file.

* Type: string
* Default: ""
* Importance: high

kafkastore.ssl.keystore.password
--------------------------------
The password to access the keystore.

* Type: string
* Default: ""
* Importance: high

kafkastore.ssl.truststore.location
----------------------------------
The location of the SSL trust store file.

* Type: string
* Default: ""
* Importance: high

kafkastore.ssl.truststore.password
----------------------------------
The password to access the trust store.

* Type: string
* Default: ""
* Importance: high

kafkastore.topic
----------------
The durable single partition topic that acts as the durable log for the data. This topic must be compacted to avoid losing data due to retention policy.

* Type: string
* Default: "_schemas"
* Importance: high

kafkastore.topic.replication.factor
-----------------------------------
The desired replication factor of the schema topic. The actual replication factor will be the smaller of this value and the number of live Kafka brokers.

* Type: int
* Default: 3
* Importance: high

response.mediatype.default
--------------------------
The default response media type that should be used if no specify types are requested in an Accept header.

* Type: string
* Default: "application/vnd.schemaregistry.v1+json"
* Importance: high

ssl.keystore.location
---------------------
Used for HTTPS. Location of the keystore file to use for SSL. IMPORTANT: Jetty requires that the key's CN, stored in the keystore, must match the FQDN.

* Type: string
* Default: ""
* Importance: high

ssl.keystore.password
---------------------
Used for HTTPS. The store password for the keystore file.

* Type: password
* Default: ""
* Importance: high

ssl.key.password
----------------
Used for HTTPS. The password of the private key in the keystore file.

* Type: password
* Default: ""
* Importance: high

ssl.truststore.location
-----------------------
Used for HTTPS. Location of the trust store. Required only to authenticate HTTPS clients.

* Type: string
* Default: ""
* Importance: high

ssl.truststore.password
-----------------------
Used for HTTPS. The store password for the trust store file.

* Type: password
* Default: ""
* Importance: high

response.mediatype.preferred
----------------------------
An ordered list of the server's preferred media types used for responses, from most preferred to least.

* Type: list
* Default: [application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json]
* Importance: high

zookeeper.set.acl
-----------------
Whether or not to set an ACL in |zk| when znodes are created and |zk| SASL authentication is configured. IMPORTANT: if set to `true`, the |zk| SASL principal must be the same as the Kafka brokers.

* Type: boolean
* Default: false
* Importance: high

kafkastore.init.timeout.ms
--------------------------
The timeout for initialization of the Kafka store, including creation of the Kafka topic that stores schema data.

* Type: int
* Default: 60000
* Importance: medium

kafkastore.security.protocol
----------------------------
The security protocol to use when connecting with Kafka, the underlying persistent storage. Values can be `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL` or `SASL_SSL`.

* Type: string
* Default: "PLAINTEXT"
* Importance: medium

kafkastore.ssl.enabled.protocols
--------------------------------
Protocols enabled for SSL connections.

* Type: string
* Default: "TLSv1.2,TLSv1.1,TLSv1"
* Importance: medium

kafkastore.ssl.keystore.type
----------------------------
The file format of the keystore.

* Type: string
* Default: "JKS"
* Importance: medium

kafkastore.ssl.protocol
-----------------------
The SSL protocol used.

* Type: string
* Default: "TLS"
* Importance: medium

kafkastore.ssl.provider
-----------------------
The name of the security provider used for SSL.

* Type: string
* Default: ""
* Importance: medium

kafkastore.ssl.truststore.type
------------------------------
The file format of the trust store.

* Type: string
* Default: "JKS"
* Importance: medium

kafkastore.timeout.ms
---------------------
The timeout for an operation on the Kafka store

* Type: int
* Default: 500
* Importance: medium

master.eligibility
------------------
If true, this node can participate in master election. In a multi-colo setup, turn this off for clusters in the slave data center.

* Type: boolean
* Default: true
* Importance: medium

kafkastore.sasl.kerberos.service.name
-------------------------------------
The Kerberos principal name that the Kafka client runs as. This can be defined either in the JAAS config file or here.

* Type: string
* Default: ""
* Importance: medium

kafkastore.sasl.mechanism
-------------------------
The SASL mechanism used for Kafka connections. GSSAPI is the default.

* Type: string
* Default: "GSSAPI"
* Importance: medium

access.control.allow.methods
----------------------------
Set value to Jetty Access-Control-Allow-Origin header for specified methods

* Type: string
* Default: ""
* Importance: low

ssl.keystore.type
-----------------
Used for HTTPS. The type of keystore file.

* Type: string
* Default: "JKS"
* Importance: medium

ssl.truststore.type
-------------------
Used for HTTPS. The type of trust store file.

* Type: string
* Default: "JKS"
* Importance: medium

ssl.protocol
------------
Used for HTTPS. The SSL protocol used to generate the SslContextFactory.

* Type: string
* Default: "TLS"
* Importance: medium

ssl.provider
------------
Used for HTTPS. The SSL security provider name. Leave blank to use Jetty's default.

* Type: string
* Default: "" (Jetty's default)
* Importance: medium

ssl.client.auth
---------------
Used for HTTPS. Whether or not to require the HTTPS client to authenticate via the server's trust store.

* Type: boolean
* Default: false
* Importance: medium

ssl.enabled.protocols
---------------------
Used for HTTPS. The list of protocols enabled for SSL connections. Comma-separated list. Leave blank to use Jetty's defaults.

* Type: list
* Default: "" (Jetty's default)
* Importance: medium

access.control.allow.origin
---------------------------
Set value for Jetty Access-Control-Allow-Origin header

* Type: string
* Default: ""
* Importance: low

debug
-----
Boolean indicating whether extra debugging information is generated in some error response entities.

* Type: boolean
* Default: false
* Importance: low

kafkastore.ssl.cipher.suites
----------------------------
A list of cipher suites used for SSL.

* Type: string
* Default: ""
* Importance: low

kafkastore.ssl.endpoint.identification.algorithm
------------------------------------------------
The endpoint identification algorithm to validate the server hostname using the server certificate.

* Type: string
* Default: ""
* Importance: low

kafkastore.ssl.keymanager.algorithm
-----------------------------------
The algorithm used by key manager factory for SSL connections.

* Type: string
* Default: "SunX509"
* Importance: low

kafkastore.ssl.trustmanager.algorithm
-------------------------------------
The algorithm used by the trust manager factory for SSL connections.

* Type: string
* Default: "PKIX"
* Importance: low

kafkastore.zk.session.timeout.ms
--------------------------------
|zk| session timeout

* Type: int
* Default: 30000
* Importance: low

metric.reporters
----------------
A list of classes to use as metrics reporters. Implementing the <code>MetricReporter</code> interface allows plugging in classes that will be notified of new metric creation. The JmxReporter is always included to register JMX statistics.

* Type: list
* Default: []
* Importance: low

metrics.jmx.prefix
------------------
Prefix to apply to metric names for the default JMX reporter.

* Type: string
* Default: "kafka.schema.registry"
* Importance: low

metrics.num.samples
-------------------
The number of samples maintained to compute metrics.

* Type: int
* Default: 2
* Importance: low

metrics.sample.window.ms
------------------------
The metrics system maintains a configurable number of samples over a fixed window size. This configuration controls the size of the window. For example we might maintain two samples each measured over a 30 second period. When a window expires we erase and overwrite the oldest window.

* Type: long
* Default: 30000
* Importance: low

port
----
DEPRECATED: port to listen on for new connections. Use :ref:`sr-listeners` instead.

* Type: int
* Default: 8081
* Importance: low

request.logger.name
-------------------
Name of the SLF4J logger to write the NCSA Common Log Format request log.

* Type: string
* Default: "io.confluent.rest-utils.requests"
* Importance: low

inter.instance.protocol
-----------------------
The protocol used while making calls between the instances of |sr|. The slave to master node calls for writes and deletes will use the specified protocol. The default value would be `http`. When `https` is set, `ssl.keystore.` and `ssl.truststore.` configs are used while making the call. The schema.registry.inter.instance.protocol name is deprecated; prefer using inter.instance.protocol instead.

* Type: string
* Default: "http"
* Importance: low

schema.registry.inter.instance.protocol
---------------------------------------
The protocol used while making calls between the instances of |sr|. The slave to master node calls for writes and deletes will use the specified protocol. The default value would be `http`. When `https` is set, `ssl.keystore.` and `ssl.truststore.` configs are used while making the call. The schema.registry.inter.instance.protocol name is deprecated; prefer using inter.instance.protocol instead.

* Type: string
* Default: ""
* Importance: low

resource.extension.class
------------------------
Fully qualified class name of a valid implementation of the interface SchemaRegistryResourceExtension. This can be used to inject user defined resources like filters. Typically used to add custom capability like logging, security, etc. The schema.registry.resource.extension.class name is deprecated; prefer using resource.extension.class instead.

* Type: list
* Default: []
* Importance: low

schema.registry.resource.extension.class
----------------------------------------
Fully qualified class name of a valid implementation of the interface SchemaRegistryResourceExtension. This can be used to inject user defined resources like filters. Typically used to add custom capability like logging, security, etc. The schema.registry.resource.extension.class name is deprecated; prefer using resource.extension.class instead.

* Type: string
* Default: ""
* Importance: low

schema.registry.zk.namespace
----------------------------
The string that is used as the |zk| namespace for storing |sr| metadata. |sr| instances which are part of the same |sr| service should have the same |zk| namespace.

* Type: string
* Default: "schema_registry"
* Importance: low

shutdown.graceful.ms
--------------------
Amount of time to wait after a shutdown request for outstanding requests to complete.

* Type: int
* Default: 1000
* Importance: low

ssl.keymanager.algorithm
------------------------
Used for HTTPS. The algorithm used by the key manager factory for SSL connections. Leave blank to use Jetty's default.

* Type: string
* Default: "" (Jetty's default)
* Importance: low

ssl.trustmanager.algorithm
--------------------------
Used for HTTPS. The algorithm used by the trust manager factory for SSL connections. Leave blank to use Jetty's default.

* Type: string
* Default: "" (Jetty's default)
* Importance: low

ssl.cipher.suites
-----------------
Used for HTTPS. A list of SSL cipher suites. Comma-separated list. Leave blank to use Jetty's defaults.

* Type: list
* Default: "" (Jetty's default)
* Importance: low

ssl.endpoint.identification.algorithm
-------------------------------------
Used for HTTPS. The endpoint identification algorithm to validate the server hostname using the server certificate. Leave blank to use Jetty's default.

* Type: string
* Default: "" (Jetty's default)
* Importance: low

kafkastore.sasl.kerberos.kinit.cmd
----------------------------------
The Kerberos kinit command path.

* Type: string
* Default: "/usr/bin/kinit"
* Importance: low

kafkastore.sasl.kerberos.min.time.before.relogin
------------------------------------------------
The login time between refresh attempts.

* Type: long
* Default: 60000
* Importance: low

kafkastore.sasl.kerberos.ticket.renew.jitter
--------------------------------------------
The percentage of random jitter added to the renewal time.

* Type: double
* Default: 0.05
* Importance: low

kafkastore.sasl.kerberos.ticket.renew.window.factor
---------------------------------------------------
Login thread will sleep until the specified window factor of time from last refresh to ticket's expiry has been reached, at which time it will try to renew the ticket.

* Type: double
* Default: 0.8
* Importance: low

kafkastore.group.id
-------------------
Use this setting to override the group.id for the KafkaStore consumer.
This setting can become important when security is enabled, to ensure stability over |sr| consumer's group.id

Without this configuration, group.id will be "schema-registry-<host>-<port>"

* Type: string
* Default: ""
* Importance: low