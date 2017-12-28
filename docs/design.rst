.. _schemaregistry_design:

Design Overview
===============
The Schema Registry is a distributed storage layer for Avro Schemas which uses Kafka as its underlying storage mechanism. Some key design decisions:

* Assigns globally unique id to each registered schema. Allocated ids are guaranteed to be monotonically increasing but not necessarily consecutive.
* Kafka provides the durable backend, and functions as a write-ahead changelog for the state of the Schema Registry and the schemas it contains.
* The Schema Registry is designed to be distributed, with single-master architecture, and ZooKeeper/Kafka coordinates master election (based on the configuration).

Schema ID Allocation
--------------------

Schema ID allocation always happen in the master node and they ensure that the Schema ISs are
monotonically increasing.

If you are using Kafka master election, the Schema ID is always based off the last ID that was
written to Kafka store. During a master re-election, batch allocation happens only after the new
master has caught up with all the records in the store ``<kafkastore.topic>``.

If you are using Zookeeper master election, ``/<schema.registry.zk.namespace>/schema_id_counter``
path stores the upper bound on the current id batch, and new batch allocation is triggered by both master election and exhaustion of the current batch. This batch allocation helps guard against potential zombie-master scenarios, (for example, if the previous master had a GC pause that lasted longer than the ZooKeeper timeout, triggering master reelection).


Kafka Backend
-------------
Kafka is used as the Schema Registry storage backend. The special Kafka topic ``<kafkastore.topic>`` (default ``_schemas``), with a single partition, is used as a highly available write ahead log. All schemas, subject/version and id metadata, and compatibility settings are appended as messages to this log. A Schema Registry instance therefore both produces and consumes messages under the ``_schemas`` topic. It produces messages to the log when, for example, new schemas are registered under a subject, or when updates to compatibility settings are registered. The Schema Registry consumes from the ``_schemas`` log in a background thread, and updates its local caches on consumption of each new ``_schemas`` message to reflect the newly added schema or compatibility setting. Updating local state from the Kafka log in this manner ensures durability, ordering, and easy recoverability.

.. _schemaregistry_single_master:

Single Master Architecture
--------------------------
The Schema Registry is designed to work as a distributed service using single master architecture. In this configuration, at most one Schema Registry instance is master at any given moment (ignoring pathological 'zombie masters'). Only the master is capable of publishing writes to the underlying Kafka log, but all nodes are capable of directly serving read requests. Slave nodes serve registration requests indirectly by simply forwarding them to the current master, and returning the response supplied by the master.
Prior to Schema Registry version 4.0, master election was always coordinated through Zookeeper.
Master election can now optionally happen via Kafka group protocol as well.

.. note::

         Please make sure not to mix up the election modes amongst the nodes in same cluster.
         This will lead to multiple masters and issues with your operations.

Kafka Coordinator Master Election
+++++++++++++++++++++++++++++++++

.. figure:: schema-registry-design-kafka.png
   :align: center

   Kafka based Schema Registry

Kafka based master election is chosen when ``<kafkastore.connection.url>`` is not configured and
has the Kafka bootstrap brokers ``<kafkastore.bootstrap.servers>`` specified. The kafka group
protocol, chooses one amongst the master eligible nodes ``master.eligibility=true`` as the master. Kafka-based master
election can be used in cases where ZooKeeper is not available, for example for hosted or cloud
Kafka environments, or if access to ZooKeeper has been locked down.

Zookeeper Master Election
+++++++++++++++++++++++++

.. figure:: schema-registry-design.png
   :align: center

   Zookeeper based Schema Registry

Zookeeper maser election is chosen when Zookeeper URL is specified in the Schema Registry config
``<kafkastore.connection.url>``.
The current master is maintained as data in the ephemeral node on the``/<schema.registry.zk.namespace>/schema_registry_master`` path in ZooKeeper. Schema Registry nodes listen to data change and deletion events on this path, and shutdown or failure of the master process triggers each node with ``master.eligibility=true`` to participate in a new round of election. Master election is a simple 'first writer wins' policy: the first node to successfully write its own data to ``/<schema.registry.zk.namespace>/schema_registry_master`` is the new master.


The Schema Registry is also designed for multi-colo configuration. See :ref:`schemaregistry_mirroring` for more details.

