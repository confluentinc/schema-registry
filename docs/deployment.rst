Production Deployment
---------------------

This section is not meant to be an exhaustive guide to running your Kafka cluster in production, but it
covers the key things to consider before putting your cluster live

Three main areas are covered:

* Logistical considerations, such as hardware recommendations and deployment strategies
* Configuration changes that are more suited to a production environment
* Post-deployment considerations, such as data rebalancing, multi-data center setup 

.. toctree::
   :maxdepth: 3

Hardware
~~~~~~~~

If you’ve been following the normal development path, you’ve probably been playing with Kafka
on your laptop or on a small cluster of machines laying around. But when it comes time to deploying 
Kafka to production, there are a few recommendations that you should consider. Nothing is a hard-and-fast rule; 
Kafka is used for a wide range of use cases and on a bewildering array of machines. But these recommendations 
provide good starting points based on our experience with production clusters.

Memory
^^^^^^

Kafka relies heavily on the filesystem for storing and caching messages. All data is immediately written to a 
persistent log on the filesystem without necessarily flushing to disk. In effect this just means that it is 
transferred into the kernel's pagecache. A modern OS will happily divert all free memory to disk caching with 
little performance penalty when the memory is reclaimed. Furthermore, Kafka uses heap space very carefully and
does not require setting heap sizes more than 5GB. This will result in a file system cache of up to 28-30GB on 
a 32GB machine.

You need sufficient memory to buffer active readers and writers. You can do a back-of-the-envelope estimate of 
memory needs by assuming you want to be able to buffer for 30 seconds and compute your memory need as 
write_throughput*30.

A machine with 64 GB of RAM is a decent choice, but 32 GB machines are not uncommon. Less than 32 GB tends
to be counterproductive (you end up needing many, many small machines). 

CPUs
^^^^

Most Kafka deployments tend to be rather light on CPU requirements. As such, the exact processor setup matters
less than the other resources. You should choose a modern processor with multiple cores. Common clusters utilize
24 core machines.

If you need to choose between faster CPUs or more cores, choose more cores. The extra concurrency that multiple
cores offers will far outweigh a slightly faster clock speed.

Disks
^^^^^

We recommend using multiple drives to get good throughput and not sharing the same drives used for Kafka data with application logs or other OS filesystem activity to ensure good latency. As of 0.8 you can either RAID these drives together into a single volume or format and mount each drive as its own directory. Since Kafka has replication the redundancy provided by RAID can also be provided at the application level. This choice has several tradeoffs.
If you configure multiple data directories partitions will be assigned round-robin to data directories. Each partition will be entirely in one of the data directories. If data is not well balanced among partitions this can lead to load imbalance between disks.

RAID can potentially do better at balancing load between disks (although it doesn't always seem to) because it balances load at a lower level. The primary downside of RAID is that it is usually a big performance hit for write throughput and reduces the available disk space.

Another potential benefit of RAID is the ability to tolerate disk failures. However our experience has been that rebuilding the RAID array is so I/O intensive that it effectively disables the server, so this does not provide much real availability improvement.

Our recommendation is to configure your Kafka server with multiple log directories, each directory mounted on 
a separate drive.

Finally, avoid network-attached storage (NAS). People routinely claim their NAS solution is faster and more reliable than local drives. Despite these claims, we have never seen NAS live up to its hype. NAS is often slower, displays larger latencies with a wider deviation in average latency, and is a single point of failure.

Network
^^^^^^^

A fast and reliable network is obviously important to performance in a distributed system. Low latency helps ensure that nodes can communicate easily, while high bandwidth helps shard movement and recovery. Modern data-center networking (1 GbE, 10 GbE) is sufficient for the vast majority of clusters.

Avoid clusters that span multiple data centers, even if the data centers are colocated in close proximity. Definitely avoid clusters that span large geographic distances.

Kafka clusters assume that all nodes are equal—not that half the nodes are actually 150ms distant in another data center. Larger latencies tend to exacerbate problems in distributed systems and make debugging and resolution more difficult.

Similar to the NAS argument, everyone claims that their pipe between data centers is robust and low latency. This is true—until it isn’t (a network failure will happen eventually; you can count on it). From our experience, the hassle of managing cross–data center clusters is simply not worth the cost.

General considerations
^^^^^^^^^^^^^^^^^^^^^^

It is possible nowadays to obtain truly enormous machines: hundreds of gigabytes of RAM with dozens of CPU cores. Conversely, it is also possible to spin up thousands of small virtual machines in cloud platforms such as EC2. Which approach is best?

In general, it is better to prefer medium-to-large boxes. Avoid small machines, because you don’t want to manage a cluster with a thousand nodes, and the overhead of simply running Kafka is more apparent on such small boxes.

At the same time, avoid the truly enormous machines. They often lead to imbalanced resource usage (for example, all the memory is being used, but none of the CPU) and can add logistical complexity if you have to run multiple nodes per machine.

JVM
~~~

We recommend running JDK 1.7 u51, and using the G1 collector. If you do this (and we highly recommend it), make
sure you're on u51. We tried out u21 in testing, but we had a number of problems with the GC implementation in 
that version. Our recommended GC tuning looks like this:

``-Xms4g -Xmx4g -XX:PermSize=48m -XX:MaxPermSize=48m -XX:+UseG1GC
-XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35``

For reference, here are the stats on one of LinkedIn's busiest clusters (at peak): 

* 15 brokers 
* 15.5k partitions (replication factor 2) 
* 400k messages/sec in - 70 MB/sec inbound 
* 400 MB/sec+ outbound 

The tuning looks fairly aggressive, but all of the brokers in that cluster have a 90% GC pause time of about 21ms, and they're doing less than 1 young GC per second.

Important Configuration Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kafka ships with very good defaults, especially when it comes to performance- related settings and options. When in doubt, just leave the settings alone.

With that said, there are some logistical configurations that should be changed for production. These changes are necessary either to make your life easier, or because there is no way to set a good default (because it depends on your cluster layout).

``zookeeper.connect``
The list of zookeeper hosts that the broker registers at. It is recommended that you configure this to all the hosts in your zookeeper cluster

* Type: string
* Importance: high

``broker.id``
Integer id that identifies a broker. No two brokers in the same Kafka cluster can have the same id.

* Type: boolean
* Importance: high

``log.dirs``
The directories in which the Kafka log data is located. 

* Type: string
* Default: "/tmp/kafka-logs"
* Importance: high

``advertised.host.name``
Hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may need to be different from the interface to which the broker binds. If this is not set, it will use the value for "host.name" if configured. Otherwise it will use the value returned from java.net.InetAddress.getCanonicalHostName().

* Type: string
* Default: ``host.name``
* Importance: high

``advertised.port``
The port to publish to ZooKeeper for clients to use. In IaaS environments, this may need to be different from the port to which the broker binds. If this is not set, it will publish the same port that the broker binds to

* Type: int
* Default: ``port``
* Importance: high

``num.partitions``
The default number of log partitions for auto-created topics. We recommend increasing this as it is better to over partition a topic. Over partitioning a topic leads to better data balancing as well as aids consumer parallelism. For keyed data, in particular, you want to avoid changing the number of partitions in a topic.

* Type: int
* Default: 1
* Importance: medium

Replication configs
^^^^^^^^^^^^^^^^^^^

``default.replication.factor``

``min.insync.replicas``

``unclean.leader.election.enable``

Don't Touch These Settings!
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Threadpools
^^^^^^^^^^^

TBD

File descriptors and mmap
~~~~~~~~~~~~~~~~~~~~~~~~~

Kafka uses a very large number of files. At the same time, Kafka uses a large number of sockets to communicate with the clients. All of this requires a relatively high number of available file descriptors.

Sadly, many modern Linux distributions ship with a paltry 1,024 file descriptors allowed per process. This is far too low for even a small Kafka node, let alone one that hosts hundreds of partitions.

You should increase your file descriptor count to something very large, such as 100,000. This process is irritatingly difficult and highly dependent on your particular OS and distribution. Consult the documentation for your OS to determine how best to change the allowed file descriptor count.

Zookeeper
~~~~~~~~~

Stable version
^^^^^^^^^^^^^^

LinkedIn and a bunch of companies are running ZooKeeper 3.3.4. Version 3.3.3 has known serious issues regarding ephemeral node deletion and session expirations. After running into those issues in production, LinkedIn upgraded to 3.3.4 and have been running that smoothly for over a year now.

Operationalizing ZooKeeper
^^^^^^^^^^^^^^^^^^^^^^^^^^

Operationally, we do the following for a healthy ZooKeeper installation:

* Redundancy in the physical/hardware/network layout: try not to put them all in the same rack, decent (but don't go nuts) hardware, try to keep redundant power and network paths, etc.
* I/O segregation: if you do a lot of write type traffic you'll almost definitely want the transaction logs on a different disk group than application logs and snapshots (the write to the ZooKeeper service has a synchronous write to disk, which can be slow).
* Application segregation: Unless you really understand the application patterns of other apps that you want to install on the same box, it can be a good idea to run ZooKeeper in isolation (though this can be a balancing act with the capabilities of the hardware).
* Use care with virtualization: It can work, depending on your cluster layout and read/write patterns and SLAs, but the tiny overheads introduced by the virtualization layer can add up and throw off ZooKeeper, as it can be very time sensitive
* ZooKeeper configuration and monitoring: It's java, make sure you give it 'enough' heap space (We usually run them with 3-5G, but that's mostly due to the data set size we have here). Unfortunately we don't have a good formula for it. As far as monitoring, both JMZ and the 4 letter commands are very useful, they do overlap in some cases (and in those cases we prefer the 4 letter commands, they seem more predictable, or at the very least, they work better with the LI monitoring infrastructure)
* Don't overbuild the cluster: large clusters, especially in a write heavy usage pattern, means a lot of intracluster communication (quorums on the writes and subsequent cluster member updates), but don't underbuild it (and risk swamping the cluster).
* Try to run on a 3-5 node cluster: ZooKeeper writes use quorums and inherently that means having an odd number of machines in a cluster. Remember that a 5 node cluster will cause writes to slow down compared to a 3 node cluster, but will allow more fault tolerance.

Overall, we try to keep the ZooKeeper system as small as will handle the load (plus standard growth capacity planning) and as simple as possible. We try not to do anything fancy with the configuration or application layout as compared to the official release as well as keep it as self contained as possible. For these reasons, we tend to skip the OS packaged versions, since it has a tendency to try to put things in the OS standard hierarchy, which can be 'messy', for want of a better way to word it.

.. _mirroring:

Mirroring data between clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We refer to the process of replicating data between Kafka clusters "mirroring" to avoid confusion with the replication that happens amongst the nodes in a single cluster. Kafka comes with a tool for mirroring data between Kafka clusters. The tool reads from one or more source clusters and writes to a destination cluster, like this:

.. image:: mirror-maker.png

A common use case for this kind of mirroring is to provide a replica in another datacenter. This scenario will be discussed in more detail in the next section.

You can run many such mirroring processes to increase throughput and for fault-tolerance (if one process dies, the others will take overs the additional load).

Data will be read from topics in the source cluster and written to a topic with the same name in the destination cluster. In fact the mirror maker is little more than a Kafka consumer and producer hooked together.

The source and destination clusters are completely independent entities: they can have different numbers of partitions and the offsets will not be the same. For this reason the mirror cluster is not really intended as a fault-tolerance mechanism (as the consumer position will be different); for that we recommend using normal in-cluster replication. The mirror maker process will, however, retain and use the message key for partitioning so order is preserved on a per-key basis.

Here is an example showing how to mirror a single topic (named my-topic) from two input clusters:

``bin/kafka-run-class.sh kafka.tools.MirrorMaker --consumer.config consumer-1.properties --consumer.config consumer-2.properties --producer.config producer.properties --whitelist my-topic``

Note that we specify the list of topics with the ``--whitelist`` option. This option allows any regular expression using Java-style regular expressions. So you could mirror two topics named A and B using ``--whitelist 'A|B'``. Or you could mirror all topics using ``--whitelist '*'``. Make sure to quote any regular expression to ensure the shell doesn't try to expand it as a file path. For convenience we allow the use of ',' instead of '|' to specify a list of topics.

Sometimes it is easier to say what it is that you don't want. Instead of using ``--whitelist`` to say what you want to mirror you can use ``--blacklist`` to say what to exclude. This also takes a regular expression argument.

Combining mirroring with the configuration ``auto.create.topics.enable=true`` makes it possible to have a replica cluster that will automatically create and replicate all data in a source cluster even as new topics are added.


