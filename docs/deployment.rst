.. _schema-registry-prod:

|sr| System Requirements
========================

This section describes the key considerations before going to production with your cluster. However, it is not an
exhaustive guide to running your |sr| in production.

.. contents::
   :local:
   :depth: 1

Hardware
--------

If you’ve been following the normal development path, you’ve probably been playing with |sr|
on your laptop or on a small cluster of machines laying around. But when it comes time to deploying
|sr| to production, there are a few recommendations that you should consider. Nothing is a hard-and-fast rule.

Memory
------

|sr| uses Kafka as a commit log to store all registered schemas durably, and maintains a few in-memory indices to make schema lookups faster. A conservative upper bound on the number of unique schemas registered in a large data-oriented company like LinkedIn is around 10,000. Assuming roughly 1000 bytes heap overhead per schema on average, heap size of 1GB would be more than sufficient.

CPUs
----

CPU usage in |sr| is light. The most computationally intensive task is checking compatibility of two schemas, an infrequent operation which occurs primarily when new schemas versions are registered under a subject.

If you need to choose between faster CPUs or more cores, choose more cores. The extra concurrency that multiple
cores offers will far outweigh a slightly faster clock speed.

Disks
-----

|sr| does not have any disk resident data. It currently uses Kafka as a commit log to store all schemas durably and holds in-memory indices of all schemas. Therefore, the only disk usage comes from storing the log4j logs.

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

.. _schema-reg-config:

Important Configuration Options
-------------------------------

The following configurations should be changed for production environments. These options depend on your cluster layout.

Depending on how |sr| instances coordinate to choose the :ref:`master<schemaregistry_single_master>`, you can deploy |sr| with |zk| (which can be shared with Kafka) or with Kafka itself. You should configure |sr| to use either Kafka-based or |zk|-based master election:

* Kafka-based master election is available since version 4.0. You can use it in cases where |zk| is not available, for example on hosted or cloud environments, or if access to |zk| has been locked down. To configure |sr| to use Kafka for master election, configure the ``kafkastore.bootstrap.servers`` setting.

  .. include:: includes/shared-config.rst
    :start-line: 10
    :end-line: 25

* |zk|-based master election is available in all versions of |sr|, and if you have an existing |sr| deployment you may continue to use it for compatibility. To configure |sr| to use |zk| for master election, configure the ``kafkastore.connection.url`` setting.

  .. include:: includes/shared-config.rst
    :start-line: 2
    :end-line: 9

If you configure both ``kafkastore.bootstrap.servers`` and ``kafkastore.connection.url``, |zk| will be used for master election. To migrate from |zk|-based to Kafka-based master election, see the :ref:`migration <schemaregistry_zk_migration>` details.

Additionally, there are some configurations that may commonly need to be set in either type of deployment.

.. include:: includes/shared-config.rst
    :start-line: 28
    :end-line: 37

.. include:: includes/shared-config.rst
    :start-line: 46
    :end-line: 53

.. note::

     Configure ``min.insync.replicas`` on the Kafka server for the schemas topic that stores all registered
     schemas to be higher than 1. For example, if the ``kafkastore.topic.replication.factor`` is 3, then set
     ``min.insync.replicas`` on the Kafka server for the ``kafkastore.topic`` to 2. This ensures that the
     register schema write is considered durable if it gets committed on at least 2 replicas out of 3. Furthermore, it
     is best to set ``unclean.leader.election.enable`` to false so that a replica outside of the isr is never elected
     leader (potentially resulting in data loss).

The full set of configuration options are documented in :ref:`schemaregistry_config`.

-----------------------------------
Don't Modify These Storage Settings
-----------------------------------

|sr| stores all schemas in a Kafka topic defined by ``kafkastore.topic``. Since this Kafka topic acts as the commit log for |sr| database and is the source of truth, writes to this store need to be durable. |sr| ships with very good defaults for all settings that affect the durability of writes to the Kafka based commit log. Finally, ``kafkastore.topic`` must be a compacted topic to avoid data loss. Whenever in doubt, leave these settings alone. If you must create the topic manually, this is an example of proper configuration:

.. sourcecode:: bash

  # kafkastore.topic=_schemas
    bin/kafka-topics --create --zookeeper localhost:2181 --topic connect-configs --replication-factor 3 --partitions 1 --config cleanup.policy=compact

.. kafkastore.topic include

.. include:: includes/shared-config.rst
    :start-line: 94
    :end-line: 101

.. kafkastore.topic.replication.factor include

.. include:: includes/shared-config.rst
    :start-line: 102
    :end-line: 109

.. kafkastore.timeout.ms include

.. include:: includes/shared-config.rst
    :start-line: 230
    :end-line: 237

Kafka & ZooKeeper
-----------------

For recommendations on operationalizing Kafka and |zk|, see :ref:`schemaregistry_operations`.

.. _schemaregistry_zk_migration:

Migration from ZooKeeper master election to Kafka master election
-----------------------------------------------------------------

It is not required to migrate from |zk|-based election to Kafka-based master election.

If you choose to migrate from |zk|-based to Kafka-based master election, make the following configuration changes in all |sr| nodes:

- Remove ``kafkastore.connection.url``
- Remove ``schema.registry.zk.namespace`` if its configured
- Configure ``kafkastore.bootstrap.servers``
- Configure ``schema.registry.group.id`` if you originally had ``schema.registry.zk.namespace`` for multiple |sr| clusters

If you configure both ``kafkastore.connection.url`` and ``kafkastore.bootstrap.servers``, |zk| will be used for master election.


Downtime for Writes
^^^^^^^^^^^^^^^^^^^^

You can migrate from |zk| based master election to Kafka based master election by following
below outlined steps. These steps would lead to |sr| not being available for writes
for a brief amount of time.

- Make above outlined config changes on that node and also ensure ``master.eligibility`` is set to false in all the nodes
- Do a rolling bounce of all the nodes.
- Configure ``master.eligibility`` to true on the nodes that can be master eligible and bounce them

Complete downtime
^^^^^^^^^^^^^^^^^^

If you want to keep things simple, you can take a temporary downtime for |sr| and do
the migration. To do so, simply shutdown all the nodes and start them again with the new configs.

Backup and Restore
------------------

As discussed in :ref:`schemaregistry_design`, all schemas, subject/version and ID metadata, and compatibility settings are appended as messages to a special Kafka topic ``<kafkastore.topic>`` (default ``_schemas``). This topic is a common source of truth for schema IDs, and you should back it up. In case of some unexpected event that makes the topic inaccessible, you can restore this schemas topic from the backup, enabling consumers to continue to read Kafka messages that were sent in the Avro format.

As a best practice, we recommend backing up the ``<kafkastore.topic>``. If you already have a multi-datacenter Kafka deployment, you can backup this topic to another Kafka cluster using :ref:`Confluent Replicator <multi_dc>`. Otherwise, you can use a :ref:`Kafka sink connector <kafka_connect>` to copy the topic data from Kafka to a separate storage (e.g. AWS S3). These will continuously update as the schema topic updates.

In lieu of either of those options, you can also use Kafka command line tools to periodically save the contents of the topic to a file. For the following examples, we assume that ``<kafkastore.topic>`` has its default value "_schemas".

To backup the topic, use the ``kafka-console-consumer`` to capture messages from the schemas topic to a file called "schemas.log". Save this file off the Kafka cluster.

.. sourcecode:: bash

   bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic _schemas --from-beginning --property print.key=true --timeout-ms 1000 1> schemas.log

To restore the topic, use the ``kafka-console-producer`` to write the contents of file "schemas.log" to a new schemas topic. This examples uses a new schemas topic name "_schemas_restore". If you use a new topic name or use the old one (i.e. "_schemas"), make sure to set ``<kafkastore.topic>`` accordingly.

.. sourcecode:: bash

   bin/kafka-console-producer --broker-list localhost:9092 --topic _schemas_restore --property parse.key=true < schemas.log
