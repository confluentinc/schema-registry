Production Deployment
=====================

This section is not meant to be an exhaustive guide to running your Schema Registry in production, but it
covers the key things to consider before putting your cluster live. Three main areas are covered:

* Logistical considerations, such as hardware recommendations and deployment strategies
* Configuration more suited to a production environment
* Post-deployment considerations, such multi-data center setup

.. toctree::
   :maxdepth: 3

Hardware
--------

If you’ve been following the normal development path, you’ve probably been playing with Schema Registry
on your laptop or on a small cluster of machines laying around. But when it comes time to deploying 
Schema Registry to production, there are a few recommendations that you should consider. Nothing is a hard-and-fast rule.

Memory
------

Schema Registry uses Kafka as a commit log to store all registered schemas durably, and maintains a few in-memory indices to make schema lookups faster. A conservative upper bound on the number of unique schemas registered in a large data-oriented company like LinkedIn is around 10,000. Assuming roughly 1000 bytes heap overhead per schema on average, heap size of 1GB would be more than sufficient.

CPUs
----

CPU usage in Schema Registry is light. The most computationally intensive task is checking compatibility of two schemas, an infrequent operation which occurs primarily when new schemas versions are registered under a subject.

If you need to choose between faster CPUs or more cores, choose more cores. The extra concurrency that multiple
cores offers will far outweigh a slightly faster clock speed.

Disks
-----

Schema Registry does not have any disk resident data. It currently uses Kafka as a commit log to store all schemas durably and holds in-memory indices of all schemas. Therefore, the only disk usage comes from storing the log4j logs.

Network
-------

A fast and reliable network is obviously important to performance in a distributed system. Low latency helps ensure that nodes can communicate easily, while high bandwidth helps shard movement and recovery. Modern data-center networking (1 GbE, 10 GbE) is sufficient for the vast majority of clusters.

Avoid clusters that span multiple data centers, even if the data centers are colocated in close proximity. Definitely avoid clusters that span large geographic distances.

Larger latencies tend to exacerbate problems in distributed systems and make debugging and resolution more difficult.

Often, people might assume the pipe between multiple data centers is robust or low latency. But this is usually not true and network failures might happen at some point. Please refer to our recommended :ref:`schemaregistry_mirroring`.

JVM
---

We recommend running the latest version of JDK 1.8 with the G1 collector (older freely available versions have disclosed security vulnerabilities).

If you are still on JDK 1.7 (which is also supported) and you are planning to use G1 (the current default), make
sure you're on u51. We tried out u21 in testing, but we had a number of problems with the GC implementation in
that version.

Our recommended GC tuning looks like this:

.. sourcecode:: bash

   -Xms1g -Xmx1g -XX:MetaspaceSize=96m -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
          -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M \
          -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80

Important Configuration Options
-------------------------------

The full set of configuration options are documented in :ref:`schemaregistry_config`.

However, there are some logistical configurations that should be changed for production. These changes are necessary because there is no way to set a good default (because it depends on your cluster layout).

First, there are two ways to deploy the Schema Registry depending on how the Schema Registry
instances coordinate with each other to choose the :ref:`master<schemaregistry_single_master>`:
with ZooKeeper (which may be shared with Kafka) or via Kafka itself. ZooKeeper-based master
election is available in all versions of Schema Registry and you should continue to use it for
compatibility if you already have a Schema Registry deployment. Kafka-based master election can
be used in cases where ZooKeeper is not available, for example for hosted or cloud Kafka
environments, or if access to ZooKeeper has been locked down.

To configure the Schema Registry to use ZooKeeper for master election, configure the
``kafkastore.connection.url`` setting.

``kafkastore.connection.url``
Comma separated list for Zookeeper urls for the Kafka cluster

* Type: list
* Importance: high

To configure the Schema Registry to use Kafka for master election, configure the
``kafkastore.bootstrap.servers`` setting.

``kafkastore.bootstrap.servers``
  A list of Kafka brokers to connect to. For example, `PLAINTEXT://hostname:9092,SSL://hostname2:9092`

  The effect of this setting depends on whether you specify `kafkastore.connection.url`.

  If `kafkastore.connection.url` is not specified, then the Kafka cluster containing these bootstrap servers will be used both to coordinate schema registry instances (master election) and store schema data.

  If `kafkastore.connection.url` is specified, then this setting is used to control how the schema registry connects to Kafka to store schema data and is particularly important when Kafka security is enabled. When this configuration is not specified, the Schema Registry's internal Kafka clients will get their Kafka bootstrap server list from ZooKeeper (configured with `kafkastore.connection.url`). In that case, all available listeners matching the `kafkastore.security.protocol` setting will be used.

  By specifiying this configuration, you can control which endpoints are used to connect to Kafka. Kafka may expose multiple endpoints that all will be stored in ZooKeeper, but the Schema Registry may need to be configured with just one of those endpoints, for example to control which security protocol it uses.

  * Type: list
  * Default: []
  * Importance: medium

Additionally, there are some configurations that may commonly need to be set in either type of
deployment.

``port``
Port to listen on for new connections.

* Type: int
* Default: 8081
* Importance: high

``host.name``
Hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may need to be different from the interface to which the broker binds. If this is not set, it will use the value returned from ``java.net.InetAddress.getCanonicalHostName()``.

* Type: string
* Default: ``host.name``
* Importance: high

.. note::

     Configure ``min.insync.replicas`` on the Kafka server for the schemas topic that stores all registered
     schemas to be higher than 1. For example, if the ``kafkastore.topic.replication.factor`` is 3, then set
     ``min.insync.replicas`` on the Kafka server for the ``kafkastore.topic`` to 2. This ensures that the
     register schema write is considered durable if it gets committed on at least 2 replicas out of 3. Furthermore, it is best to set ``unclean.leader.election.enable`` to false so that a replica outside of the isr is never elected leader (potentially resulting in data loss).

Don't Touch These Settings!
---------------------------

Storage settings
^^^^^^^^^^^^^^^^

Schema Registry stores all schemas in a Kafka topic defined by ``kafkastore.topic``. Since this Kafka topic acts as the commit log for the Schema Registry database and is the source of truth, writes to this store need to be durable. Schema Registry ships with very good defaults for all settings that affect the durability of writes to the Kafka based commit log. Finally, ``kafkastore.topic`` must be a compacted topic to avoid data loss. Whenever in doubt, leave these settings alone. If you must create the topic manually, this is an example of proper configuration:

.. sourcecode:: bash

  # kafkastore.topic=_schemas
  $ bin/kafka-topics --create --zookeeper localhost:2181 --topic connect-configs --replication-factor 3 --partitions 1 --config cleanup.policy=compact

``kafkastore.topic``
The single partition topic that acts as the durable log for the data. This must be a compacted topic to avoid data loss due to retention policy.

* Type: string
* Default: "_schemas"
* Importance: high

``kafkastore.topic.replication.factor``
The desired replication factor of the schema topic. The actual replication factor will be the smaller of this value and the number of live Kafka brokers.

* Type: int
* Default: 3
* Importance: high

``kafkastore.timeout.ms``
The timeout for an operation on the Kafka store. This is the maximum time that a register call blocks.

* Type: int
* Default: 500
* Importance: medium

Kafka & ZooKeeper
-----------------

Please refer to :ref:`schemaregistry_operations` for recommendations on operationalizing Kafka and ZooKeeper.



Backup and Restore
~~~~~~~~~~~~~~~~~~

As discussed in :ref: `_schemaregistry_design`, all schemas, subject/version and id metadata, and compatibility settings are appended as messages to a special Kafka topic ``<kafkastore.topic>`` (default ``_schemas``). This topic is a common source of truth for schema IDs, and you should back it up. In case of some unexpected event that makes the topic inaccessible, you can restore this schemas topic from the backup, enabling consumers to continue to read Kafka messages that were sent in the Avro format.

As a best practice, we recommend backing up the ``<kafkastore.topic>``. If you already have a multi-datacenter Kafka deployment, you can backup this topic to another Kafka cluster using `Confluent Replicator <https://docs.confluent.io/current/multi-dc/index.html>`_. Otherwise, you can use a `Kafka sink connector <https://docs.confluent.io/current/connect/index.html>`_ to copy the topic data from Kafka to a separate storage (e.g. AWS S3). These will continuously update as the schema topic updates.

In lieu of either of those options, you can also use Kafka command line tools to periodically save the contents of the topic to a file. For the following examples, we assume that ``<kafkastore.topic>`` has its default value "_schemas".

To backup the topic, use the ``kafka-console-consumer`` to capture messages from the schemas topic to a file called "schemas.log". Save this file off the Kafka cluster.

.. sourcecode:: bash

   bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic _schemas --from-beginning --property print.key=true --timeout-ms 1000 1> schemas.log

To restore the topic, use the ``kafka-console-producer`` to write the contents of file "schemas.log" to a new schemas topic. This examples uses a new schemas topic name "_schemas_restore". If you use a new topic name or use the old one (i.e. "_schemas"), make sure to set ``<kafkastore.topic>`` accordingly.

.. sourcecode:: bash

   bin/kafka-console-producer --broker-list localhost:9092 --topic _schemas_restore --property parse.key=true < schemas.log


Migration from Zookeeper master election to Kafka master election
-----------------------------------------------------------------

It is not required to migrate from Zookeeper based election to Kafka based master election. If
you choose to do so, you need to make the below outlined config changes as the first step in all
Schema Registry nodes

- Remove ``kafkastore.connection.url``
- Remove ``schema.registry.zk.namespace`` if its configured
- Configure ``kafkastore.bootstrap.servers``
- Configure ``schema.registry.group.id`` if you originally had ``schema.registry.zk.namespace`` for multiple Schema Registry clusters

Downtime for Writes
^^^^^^^^^^^^^^^^^^^^

You can migrate from Zookeeper based master election to Kafka based master election by following
below outlined steps. These steps would lead to Schema Registry not being available for writes
for a brief amount of time.

- Make above outlined config changes on that node and also ensure ``master.eligibility`` is set to false in all the nodes
- Do a rolling bounce of all the nodes.
- Configure ``master.eligibility`` to true on the nodes that can be master eligible and bounce them

Complete downtime
^^^^^^^^^^^^^^^^^^

If you want to keep things simple, you can take a temporary downtime for Schema Registry and do
the migration. To do so, simply shutdown all the nodes and start them again with the new configs.

Backup and Restore
------------------

As discussed in :ref: `_schemaregistry_design`, all schemas, subject/version and id metadata, and compatibility settings are appended as messages to a special Kafka topic ``<kafkastore.topic>`` (default ``_schemas``). This topic is a common source of truth for schema IDs, and you should back it up. In case of some unexpected event that makes the topic inaccessible, you can restore this schemas topic from the backup, enabling consumers to continue to read Kafka messages that were sent in the Avro format.

As a best practice, we recommend backing up the ``<kafkastore.topic>``. If you already have a multi-datacenter Kafka deployment, you can backup this topic to another Kafka cluster using `Confluent Replicator <https://docs.confluent.io/current/multi-dc/index.html>`_. Otherwise, you can use a `Kafka sink connector <https://docs.confluent.io/current/connect/index.html>`_ to copy the topic data from Kafka to a separate storage (e.g. AWS S3). These will continuously update as the schema topic updates.

In lieu of either of those options, you can also use Kafka command line tools to periodically save the contents of the topic to a file. For the following examples, we assume that ``<kafkastore.topic>`` has its default value "_schemas".

To backup the topic, use the ``kafka-console-consumer`` to capture messages from the schemas topic to a file called "schemas.log". Save this file off the Kafka cluster.

.. sourcecode:: bash

   bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic _schemas --from-beginning --property print.key=true --timeout-ms 1000 1> schemas.log

To restore the topic, use the ``kafka-console-producer`` to write the contents of file "schemas.log" to a new schemas topic. This examples uses a new schemas topic name "_schemas_restore". If you use a new topic name or use the old one (i.e. "_schemas"), make sure to set ``<kafkastore.topic>`` accordingly.

.. sourcecode:: bash

   bin/kafka-console-producer --broker-list localhost:9092 --topic _schemas_restore --property parse.key=true < schemas.log
