Post Deployment
---------------

Once you have deployed your cluster in production, there are some tools and best practices to keep your cluster running in good shape. This section talks about configuring settings dynamically, tweaking logging levels, partition reassignment and deleting topics.

Tweaking configs dynamically
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many config settings in Kafka are static and are wired through the properties file. However, there are several settings that you can tweak per topic. These settings can be changed dynamically using the kafka-topics.sh tool without having to restart the brokers. 

When changed using this tool, each change is persistent and lives through broker restarts.

``unclean.leader.election.enable``
Indicates whether unclean leader election is enabled. If it is, then a leader may be moved to a replica that is not insync with the leader when all insync replicas are not available, leading to possible data loss.

* Type: boolean
* Default: true
* Importance: high

``min.insync.replicas``
If number of insync replicas drops below this number, we stop accepting writes with -1 (or all) ``request.required.acks``

* Type: int
* Default: 1
* Importance: high

``max.message.bytes``
The maximum size of a message

* Type: int
* Default: Integer.MAX_VALUE
* Importance: medium

``cleanup.policy``
Should old segments in this log be deleted or deduplicated?

* Type: string
* Default: delete
* Importance: medium

``flush.messages``
The number of messages that can be written to the log before a flush is forced

* Type: long
* Default: Long.MAX_VALUE
* Importance: medium

``flush.ms``
The amount of time the log can have dirty data before a flush is forced

* Type: long
* Default: Long.MAX_VALUE
* Importance: medium

``segment.bytes``
The hard maximum for the size of a segment file in the log

* Type: int
* Default: 1048576
* Importance: low

``segment.ms``
The soft maximum on the amount of time before a new log segment is rolled

* Type: long
* Default: Long.MAX_VALUE
* Importance: low

``retention.bytes``
The approximate total number of bytes this log can use

* Type: long
* Default: Long.MAX_VALUE
* Importance: low

``retention.ms``
The approximate total number of bytes this log can use

* Type: long
* Default: Long.MAX_VALUE
* Importance: low

``segment.jitter.ms``
The maximum random jitter subtracted from segmentMs to avoid thundering herds of segment rolling

* Type: long
* Default: 0L
* Importance: low

``segment.bytes``
The hard maximum for the size of a segment file in the log

* Type: long
* Default: 1048576
* Importance: low

Logging
~~~~~~~

Kafka emits a number of logs, which are placed in ``KAFKA_HOME/logs``. The default logging level is INFO. It provides a moderate amount of information, but is designed to be rather light so that your logs are not enormous.

When debugging problems, particularly problems with replicas falling out of ISR, it can be helpful to bump up the logging level to DEBUG.

The logs from the server go to logs/server.log.

You could modify the logging.yml file and restart your nodes—but that is both tedious and leads to unnecessary downtime.

Controller
^^^^^^^^^^

Kafka elects one broker in the cluster to be the controller. The controller is responsible for cluster management and handles events like broker failures, leader election, topic deletion and more.

Since the controller is embedded in the broker, the logs from the controller are separated from the server logs in logs/controller.log. Any ERROR, FATAL or WARN in this log indicates an important event that should be looked at by the administrator.

State Change Log
^^^^^^^^^^^^^^^^

The controller does state management for all resources in the Kafka cluster. This includes topics, partitions, brokers and replicas. As part of state management, when the state of any resource is changed by the controller, it logs the action to a special state change log stored under logs/state-change.log. This is useful for troubleshooting purposes. For example, if some partition is offline for a while, this log can provide useful information as to whether the partition is offline due to a failed leader election operation. 

Note that the default log level for this log is TRACE.

Request logging
^^^^^^^^^^^^^^^

Kafka has the facility to log every request served by the broker. This includes not only produce and consume requests, but also requests sent by the controller to brokers and metadata requests. 

If this log is enabled at the DEBUG level, it contains latency information for every request along with the latency breakdown by component, so you can see where the bottleneck is. If this log is enabled at TRACE, it further logs the contents of the request. 

We do not recommend you set this log to TRACE for a long period of time as the amount of logging can affect the performance of the cluster.

Admin operations
~~~~~~~~~~~~~~~~

This section covers the various admin tools that you can use to administer a Kafka cluster in production. As of Kafka 0.8.2, there are still a number of useful operations that are not automated and have to be triggered using one of the tools that ship with Kafka under ``bin/``

Adding and Removing topics
^^^^^^^^^^^^^^^^^^^^^^^^^^

You have the option of either adding topics manually or having them be created automatically when data is first published to a non-existent topic. If topics are auto-created then you may want to tune the default topic configurations used for auto-created topics.
Topics are added and modified using the topic tool:

``bin/kafka-topics.sh --zookeeper zk_host:port/chroot --create --topic my_topic_name --partitions 20 --replication-factor 3 --config x=y``

The replication factor controls how many servers will replicate each message that is written. If you have a replication factor of 3 then up to 2 servers can fail before you will lose access to your data. We recommend you use a replication factor of 2 or 3 so that you can transparently bounce machines without interrupting data consumption.

The partition count controls how many logs the topic will be sharded into. There are several impacts of the partition count. First each partition must fit entirely on a single server. So if you have 20 partitions the full data set (and read and write load) will be handled by no more than 20 servers (no counting replicas). Finally the partition count impacts the maximum parallelism of your consumers. 

The configurations added on the command line override the default settings the server has for things like the length of time data should be retained. The complete set of per-topic configurations is documented here.

Modifying topics
^^^^^^^^^^^^^^^^

You can change the configuration or partitioning of a topic using the same topic tool.

To add partitions you can do

``bin/kafka-topics.sh --zookeeper zk_host:port/chroot --alter --topic my_topic_name --partitions 40``
 
Be aware that one use case for partitions is to semantically partition data, and adding partitions doesn't change the partitioning of existing data so this may disturb consumers if they rely on that partition. That is if data is partitioned by hash(key) % number_of_partitions then this partitioning will potentially be shuffled by adding partitions but Kafka will not attempt to automatically redistribute data in any way.

To add configs:

``bin/kafka-topics.sh --zookeeper zk_host:port/chroot --alter --topic my_topic_name --config x=y``

To remove a config:

``bin/kafka-topics.sh --zookeeper zk_host:port/chroot --alter --topic my_topic_name --deleteConfig x``

And finally deleting a topic:

``bin/kafka-topics.sh --zookeeper zk_host:port/chroot --delete --topic my_topic_name``

WARNING: Delete topic functionality is beta in 0.8.1. Please report any bugs that you encounter on the mailing list or JIRA.

Kafka does not currently support reducing the number of partitions for a topic or changing the replication factor.

Graceful shutdown
^^^^^^^^^^^^^^^^^

The Kafka cluster will automatically detect any broker shutdown or failure and elect new leaders for the partitions on that machine. This will occur whether a server fails or it is brought down intentionally for maintenance or configuration changes. For the later cases Kafka supports a more graceful mechanism for stoping a server then just killing it. When a server is stopped gracefully it has two optimizations it will take advantage of:

1. It will sync all its logs to disk to avoid needing to do any log recovery when it restarts (i.e. validating the checksum for all messages in the tail of the log). Log recovery takes time so this speeds up intentional restarts.
2. It will migrate any partitions the server is the leader for to other replicas prior to shutting down. This will make the leadership transfer faster and minimize the time each partition is unavailable to a few milliseconds.

Syncing the logs will happen automatically happen whenever the server is stopped other than by a hard kill, but the controlled leadership migration requires using a special setting: ``controlled.shutdown.enable=true``

Note that controlled shutdown will only succeed if all the partitions hosted on the broker have replicas (i.e. the replication factor is greater than 1 and at least one of these replicas is alive). This is generally what you want since shutting down the last replica would make that topic partition unavailable.

Expanding your cluster
^^^^^^^^^^^^^^^^^^^^^^

Adding servers to a Kafka cluster is easy, just assign them a unique broker id and start up Kafka on your new servers. However these new servers will not automatically be assigned any data partitions, so unless partitions are moved to them they won't be doing any work until new topics are created. So usually when you add machines to your cluster you will want to migrate some existing data to these machines.

The process of migrating data is manually initiated but fully automated. Under the covers what happens is that Kafka will add the new server as a follower of the partition it is migrating and allow it to fully replicate the existing data in that partition. When the new server has fully replicated the contents of this partition and joined the in-sync replica one of the existing replicas will delete their partition's data.

The partition reassignment tool can be used to move partitions across brokers. An ideal partition distribution would ensure even data load and partition sizes across all brokers. In 0.8.1, the partition reassignment tool does not have the capability to automatically study the data distribution in a Kafka cluster and move partitions around to attain an even load distribution. As such, the admin has to figure out which topics or partitions should be moved around.

The partition reassignment tool can run in 3 mutually exclusive modes -

* ``--generate``: In this mode, given a list of topics and a list of brokers, the tool generates a candidate reassignment to move all partitions of the specified topics to the new brokers. This option merely provides a convenient way to generate a partition reassignment plan given a list of topics and target brokers.
* ``--execute``: In this mode, the tool kicks off the reassignment of partitions based on the user provided reassignment plan. (using the ``--reassignment-json-file`` option). This can either be a custom reassignment plan hand crafted by the admin or provided by using the --generate option
* ``--verify``: In this mode, the tool verifies the status of the reassignment for all partitions listed during the last ``--execute``. The status can be either of successfully completed, failed or in progress

Decommissioning brokers
^^^^^^^^^^^^^^^^^^^^^^^

The partition reassignment tool does not have the ability to automatically generate a reassignment plan for decommissioning brokers yet. As such, the admin has to come up with a reassignment plan to move the replica for all partitions hosted on the broker to be decommissioned, to the rest of the brokers. This can be relatively tedious as the reassignment needs to ensure that all the replicas are not moved from the decommissioned broker to only one other broker. To make this process effortless, we plan to add tooling support for decommissioning brokers in ``v0.8.3``.

Increasing replication factor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Increasing the replication factor of an existing partition is easy. Just specify the extra replicas in the custom reassignment json file and use it with the ``--execute`` option to increase the replication factor of the specified partitions.
For instance, the following example increases the replication factor of partition 0 of topic foo from 1 to 3. Before increasing the replication factor, the partition's only replica existed on broker 5. As part of increasing the replication factor, we will add more replicas on brokers 6 and 7.

The first step is to hand craft the custom reassignment plan in a json file-

.. sourcecode:: bash

   cat increase-replication-factor.json
   {"version":1,
    "partitions":[
       {"topic":"foo",
        "partition":0,
        "replicas":[5,6,7]
       }
     ]
   }

Then, use the json file with the ``--execute`` option to start the reassignment process-

.. sourcecode:: bash

   bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --reassignment-json-file    increase-replication-factor.json --execute

   Current partition replica assignment

   {"version":1,
    "partitions":[
       {"topic":"foo",
        "partition":0,
        "replicas":[5]
       }
     ]
   }

   Save this to use as the ``--reassignment-json-file`` option during rollback

   Successfully started reassignment of partitions
   {"version":1,
    "partitions":[
       {"topic":"foo",
        "partition":0,
        "replicas":[5,6,7]
       }
     ]
   }

The ``--verify`` option can be used with the tool to check the status of the partition reassignment. Note that the same ``increase-replication-factor.json`` (used with the ``--execute`` option) should be used with the ``--verify`` option

.. sourcecode:: bash

   bin/kafka-reassign-partitions.sh --zookeeper localhost:2181 --reassignment-json-file increase-replication-factor.json --verify

   Reassignment of partition [foo,0] completed successfully

You can also verify the increase in replication factor with the ``kafka-topics.sh`` tool-

.. sourcecode:: bash

   bin/kafka-topics.sh --zookeeper localhost:2181 --topic foo --describe
   Topic:foo	PartitionCount:1	ReplicationFactor:3	Configs:
   Topic: foo	Partition: 0	Leader: 5	Replicas: 5,6,7	Isr: 5,6,7
	
Performance Tips
~~~~~~~~~~~~~~~~

Picking the number of partitions for a topic
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There isn't really a right answer, we expose this as an option because it is a tradeoff. The simple answer is that the partition count determines the maximum consumer parallelism and so you should set a partition count based on the maximum consumer parallelism you would expect to need (i.e. over-provision). Clusters with up to 10k total partitions are quite workable. Beyond that we don't aggressively test (it should work, but we can't guarantee it).

Here is a more complete list of tradeoffs to consider:

* A partition is basically a directory of log files.
* Each partition must fit entirely on one machine. So if you have only one partition in your topic you cannot scale your write rate or retention beyond the capability of a single machine. If you have 1000 partitions you could potentially use 1000 machines.
* Each partition is totally ordered. If you want a total order over all writes you probably want to have just one partition.
* Each partition is not consumed by more than one consumer thread/process in each consumer group. This allows to have each process consume in a single threaded fashion to guarantee ordering to the consumer within the partition (if we split up a partition of ordered messages and handed them out to multiple consumers even though the messages were stored in order they would be processed out of order at times).
* Many partitions can be consumed by a single process, though. So you can have 1000 partitions all consumed by a single process. Another way to say the above is that the partition count is a bound on the maximum consumer parallelism.
* More partitions will mean more files and hence can lead to smaller writes if you don't have enough memory to properly buffer the writes and coalesce them into larger writes
* Each partition corresponds to several znodes in zookeeper. Zookeeper keeps everything in memory so this can eventually get out of hand.
* More partitions means longer leader fail-over time. Each partition can be handled quickly (milliseconds) but with thousands of partitions this can add up.
* When we checkpoint the consumer position we store one offset per partition so the more partitions the more expensive the position checkpoint is.
* It is possible to later expand the number of partitions BUT when we do so we do not attempt to reorganize the data in the topic. So if you are depending on key-based semantic partitioning in your processing you will have to manually copy data from the old low partition topic to a new higher partition topic if you later need to expand.

Note that I/O and file counts are really about #partitions/#brokers, so adding brokers will fix problems there; but zookeeper handles all partitions for the whole cluster so adding machines doesn't help.

Lagging replicas
^^^^^^^^^^^^^^^^

ISR is the set of replicas that are fully sync-ed up with the leader. In other words, every replica in the ISR has written all committed messages to its local log. In steady state, ISR should always include all replicas of the partition. Occasionally, some replicas fall out of the insync replica list. This could either be due to failed replicas or slow replicas.

A replica can be dropped out of the ISR if it diverges from the leader beyond a certain threshold. This is controlled by 2 parameters: 

* ``replica.lag.time.max.ms``

  This is typically set to a value that reliably detects the failure of a broker. You can set this value appropriately by observing the value of the replica's minimum fetch rate that measures the rate of fetching messages from the leader (``kafka.server:type=ReplicaFetcherManager,name=MinFetchRate,clientId=<Replica>`` where Replica is the id of the replica broker) If that rate is ``n``, set the value for this config to larger than ``1/n * 1000``.

* ``replica.lag.max.messages``

  This is typically set to the observed maximum lag measured in number of bytes on the follower. The JMX bean for this ``kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica``. Note that if ``replica.lag.max.messages`` is too large, it can increase the time to commit a message. If latency becomes a problem, you can increase the number of partitions in a topic. If a replica constantly drops out of and rejoins isr, you may need to increase ``replica.lag.max.messages``. If a replica stays out of ISR for a long time, it may indicate that the follower is not able to fetch data as fast as data is accumulated at the leader. You can increase the follower's fetch throughput by setting a larger value for ``num.replica.fetchers``.

Increasing consumer throughput
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, try to figure out if the consumer is just slow or has stopped. To do so, you can monitor the maximum lag metric ``kafka.consumer:type=ConsumerFetcherManager,name=MaxLag,clientId=([-.\w]+)`` that indicates the number of messages the consumer lags behind the producer. Another metric to monitor is the minimum fetch rate ``kafka.consumer:type=ConsumerFetcherManager,name=MinFetchRate,clientId=([-.\w]+)`` of the consumer. If the MinFetchRate of the consumer drops to almost 0, the consumer is likely to have stopped. If the MinFetchRate is non-zero and relatively constant, but the consumer lag is increasing, it indicates that the consumer is slower than the producer. If so, the typical solution is to increase the degree of parallelism in the consumer. This may require increasing the number of partitions of a topic. 

Increasing producer throughput
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Reducing end-to-end latency
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Software Updates
~~~~~~~~~~~~~~~~

Software updates can be done by upgrading the cluster in a rolling restart fashion. The only time this will not work is while upgrading from 0.7 to 0.8. For instructions on how to migrate from 0.7 to 0.8, please see `this
<https://cwiki.apache.org/confluence/display/KAFKA/Migrating+from+0.7+to+0.8/>`_.

Backup and Restoration
~~~~~~~~~~~~~~~~~~~~~~

The best way to backup a Kafka cluster is to setup a mirror for the cluster. Depending on your setup and requirements, this mirror may be in the same data center or in a remote one. See the section on :ref:`mirroring` for more details.
