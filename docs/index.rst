.. _schemaregistry_intro:

|sr|
====

|sr| provides a serving layer for your metadata. It provides a RESTful interface for storing and retrieving Avro schemas. It stores a versioned history of all schemas, provides multiple compatibility settings and allows evolution of schemas according to the configured compatibility settings and expanded Avro support. It provides serializers that plug into Kafka clients that handle schema storage and retrieval for Kafka messages that are sent in the Avro format.

|sr| is a distributed storage layer for Avro Schemas which uses Kafka as its underlying storage mechanism. Some key design decisions:

* Assigns globally unique ID to each registered schema. Allocated IDs are guaranteed to be monotonically increasing but not necessarily consecutive.
* Kafka provides the durable backend, and functions as a write-ahead changelog for the state of |sr| and the schemas it contains.
* |sr| is designed to be distributed, with single-master architecture, and |zk|/Kafka coordinates master election (based on the configuration).

.. tip:: To see a working example of |sr|, check out :ref:`Confluent Platform demo <cp-demo>`. The demo shows you how to deploy a
         Kafka streaming ETL, including |sr|, using KSQL for stream processing.


Avro Background
---------------

When sending data over the network or storing it in a file, we need a
way to encode the data into bytes. The area of data serialization has
a long history, but has evolved quite a bit over the last few
years. People started with programming language specific serialization
such as Java serialization, which makes consuming the data in other
languages inconvenient. People then moved to language agnostic formats
such as JSON.

However, formats like JSON lack a strictly defined format, which has two significant drawbacks:

1. **Data consumers may not understand data producers:** The lack of structure makes consuming data in these formats
   more challenging because fields can be arbitrarily added or removed, and data can even be corrupted.  This drawback
   becomes more severe the more applications or teams across an organization begin consuming a data feed: if an
   upstream team can make arbitrary changes to the data format at their discretion, then it becomes very difficult to
   ensure that all downstream consumers will (continue to) be able to interpret the data.  What's missing is a
   "contract" (cf. schema below) for data between the producers and the consumers, similar to the contract of an API.
2. **Overhead and verbosity:** They are verbose because field names and type information have to be explicitly
   represented in the serialized format, despite the fact that are identical across all messages.

A few cross-language serialization libraries have emerged that require the data structure to be formally defined by
some sort of schemas. These libraries include `Avro <http://avro.apache.org>`_,
`Thrift <http://thrift.apache.org>`_, and `Protocol Buffers <https://github.com/google/protobuf>`_.  The advantage of
having a schema is that it clearly specifies the structure, the type and the meaning (through documentation) of the
data.  With a schema, data can also be encoded more efficiently.
In particular, we recommend Avro which is supported in |cp|.

An Avro schema defines the data structure in a JSON format.

The following is an example Avro schema that specifies a user record with two fields: ``name`` and ``favorite_number``
of type ``string`` and ``int``, respectively.

.. sourcecode:: json

    {"namespace": "example.avro",
     "type": "record",
     "name": "user",
     "fields": [
         {"name": "name", "type": "string"},
         {"name": "favorite_number",  "type": "int"}
     ]
    }

You can then use this Avro schema, for example, to serialize a Java object (POJO) into bytes, and deserialize these
bytes back into the Java object.

One of the interesting things about Avro is that it not only requires
a schema during data serialization, but also during data
deserialization. Because the schema is provided at decoding time,
metadata such as the field names don't have to be explicitly encoded
in the data. This makes the binary encoding of Avro data very compact.


Schema ID Allocation
--------------------

Schema ID allocation always happen in the master node and they ensure that the Schema IDs are
monotonically increasing.

If you are using Kafka master election, the Schema ID is always based off the last ID that was
written to Kafka store. During a master re-election, batch allocation happens only after the new
master has caught up with all the records in the store ``<kafkastore.topic>``.

If you are using |zk| master election, ``/<schema.registry.zk.namespace>/schema_id_counter``
path stores the upper bound on the current ID batch, and new batch allocation is triggered by both master election and exhaustion of the current batch. This batch allocation helps guard against potential zombie-master scenarios, (for example, if the previous master had a GC pause that lasted longer than the |zk| timeout, triggering master reelection).


Kafka Backend
-------------

.. include:: includes/backend.rst


.. _schemaregistry_single_master:

Single Master Architecture
--------------------------
|sr| is designed to work as a distributed service using single master architecture. In this configuration, at most one |sr| instance is master at any given moment (ignoring pathological 'zombie masters'). Only the master is capable of publishing writes to the underlying Kafka log, but all nodes are capable of directly serving read requests. Slave nodes serve registration requests indirectly by simply forwarding them to the current master, and returning the response supplied by the master.
Prior to |sr| version 4.0, master election was always coordinated through |zk|.
Master election can now optionally happen via Kafka group protocol as well.

.. note::

         Please make sure not to mix up the election modes amongst the nodes in same cluster.
         This will lead to multiple masters and issues with your operations.

---------------------------------
Kafka Coordinator Master Election
---------------------------------

.. figure:: schema-registry-design-kafka.png
   :align: center

   Kafka based Schema Registry

Kafka based master election is chosen when ``<kafkastore.connection.url>`` is not configured and
has the Kafka bootstrap brokers ``<kafkastore.bootstrap.servers>`` specified. The kafka group
protocol, chooses one amongst the master eligible nodes ``master.eligibility=true`` as the master. Kafka-based master
election can be used in cases where |zk| is not available, for example for hosted or cloud
Kafka environments, or if access to |zk| has been locked down.

--------------------
|zk| Master Election
--------------------

.. figure:: schema-registry-design.png
   :align: center

   ZooKeeper based Schema Registry

|zk| master election is chosen when |zk| URL is specified in |sr| config
``<kafkastore.connection.url>``.
The current master is maintained as data in the ephemeral node on the ``/<schema.registry.zk.namespace>/schema_registry_master`` path in |zk|. |sr| nodes listen to data change and deletion events on this path, and shutdown or failure of the master process triggers each node with ``master.eligibility=true`` to participate in a new round of election. Master election is a simple 'first writer wins' policy: the first node to successfully write its own data to ``/<schema.registry.zk.namespace>/schema_registry_master`` is the new master.


|sr| is also designed for multi-colo configuration. See :ref:`schemaregistry_mirroring` for more details.


Documentation
-------------

.. toctree::
   :maxdepth: 1

   installation
   config
   schema_registry_tutorial
   avro
   ../../cloud/connect/schema-reg-cloud-config
   using
   monitoring
   operations
   multidc
   security
   ../../confluent-security-plugins/schema-registry/introduction
   serializer-formatter
   schema-deletion-guidelines
   maven-plugin
   connect
   api
   changelog

