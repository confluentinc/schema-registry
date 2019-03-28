.. _schemaregistry_migrate:

Migrate an Existing |sr| to |ccloud| 
====================================

:ref:`cloud-home` is a fully managed streaming data service based on |cp|. Just
as you can “lift and shift” or "extend to cloud" your Kafka applications from
self-managed Kafka to |ccloud|, you can do the same with |sr-long|.

If you already use |sr| to manage schemas for Kafka applications, and want to
move some or all of that data and schema management to the cloud, you can use
|crep|  :ref:`connect_replicator` to migrate your existing schemas to
|sr-ccloud|. (See :ref:`connect_replicator` and :ref:`replicator_executable`.)

You can set up continuous migration of |sr| to maintain a hybrid deployment ("extend to
cloud") or "lift and shift" all to |ccloud| using a one-time migration.

Continuous Migration
~~~~~~~~~~~~~~~~~~~~

For continuous migration, you can use your self-managed |sr| as a primary and
|sr-ccloud| as a secondary. New schemas will be registered directly to the
self-managed |sr|, and |crep| will continuously copy schemas from it to
|sr-ccloud|, which is set to IMPORT mode.

One-time Migration
~~~~~~~~~~~~~~~~~~

Choose a one-time migration to move all data to a fully-managed |ccloud|
service. In this case, you migrate your existing self-managed |sr| to
|sr-ccloud| as a primary. All new schemas are registered to |sr-ccloud|. In the
scenario, there is no migration from |sr-ccloud| back to the self-managed |sr|.

Prerequisites
-------------

The Run Book describes how to perform a |sr| migration applicable to any type of
deployment (from on-premise or data center to |sr-ccloud|). The examples also
serve as a quick start tutorial you can use to walk through the basics of
migrating schemas from a local cluster to |ccloud|. 

Before you begin, verify that you have:

* |ccloud| `5.2` or newer to serve as the destination |sr|
* a local install of |cp| (for example, from a :ref:`ce-quickstart` download), or other cluster to serve as the origin |sr|

Schema migration requires that you configure and run |crep-full|. If you need
more information than is included in the examples here, refer to the
:ref:`replicator tutorial` <replicator-quickstart>.

Run Book
--------

To migrate |sr| to |ccloud|, follow these steps:

#.  Start the origin cluster.

    If you are running a local cluster (for example, from a :ref:`ce-quickstart` download),
    start only |sr| for the purposes of this tutorial.
    
    .. code:: bash
  
        <path-to-confluent>/bin/confluent schema-registry
    
    Verify that `schema-registry`, `kafka`, and `zookeeper` are running and that
    other services are down, as they can conflict with schema migration.
    
    ::
    
         control-center is [DOWN]
         ksql-server is [DOWN]
         connect is [DOWN]
         kafka-rest is [DOWN]
         schema-registry is [UP]
         kafka is [UP]
         zookeeper is [UP]

#.  Set the destination |sr| to IMPORT mode.  For example: 

    .. code:: bash
    
        curl -u <schema-registry-api-key>:<schema-registry-api-secret> -X PUT -H "Content-Type: application/json" "http://destregistry:8081/mode" --data '{"mode": "IMPORT"}'

#.  Configure a |crep| worker to specify the addresses of broker(s) in the destination cluster, as described in :ref:`config-and-run-replicator`.

    The worker configuration file is in `<path-to-confluent>/etc/kafka/connect-standalone.properties`.

    :: 

    # Connect Standalone Worker configuration
    bootstrap.servers=localhost:9092
                
#.  Configure :ref:`replicator` <replicator-quickstart>` with |sr| and destination cluster information.

    For example, set the following in `etc/kafka-connect-replicator/quickstart-replicator.properties`:

    :: 

        # destination cluster connection info
        dest.kafka.ssl.endpoint.identification.algorithm=https
        dest.kafka.sasl.mechanism=PLAIN
        dest.kafka.request.timeout.ms=20000
        dest.kafka.bootstrap.servers=<path-to-cloud-server>.cloud:9092
        retry.backoff.ms=500
        dest.kafka.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="2OSKHFEI22T7EJ7W" password="+UQUxhdL+R4hkQhzoUrtZg3FXwx/uLchTtXQRZ2NTGXd2zeQNeiBbaNe231tF7F+";
        dest.kafka.security.protocol=SASL_SSL    
    
        # Schema Registry migration topics to replicate from source to destination
        topic.whitelist=_schemas
        schema.topic=_schemas
        
        # Connection settings for destination Confluent Cloud Schema Registry
        schema.registry.url=https://<path-to-cloud-schema-registry>.cloud
        schema.registry.client.basic.auth.credentials.source=USER_INFO
        schema.registry.client.basic.auth.user.info=<schema-registry-api-key>:<schema-registry-api-secret>

    .. tip:: In `quickstart-replicator.properties`, the replication factor is set to `1` for demo purposes. For this schema migration tutorial, and in production, change this to at least 3: `confluent.topic.replication.factor=3`


#.  Start |crep| so that it can perform the schema migration.

    For example:

    .. code:: bash

        <path-to-confluent>/bin/connect-standalone <path-to-confluent>/etc/kafka/connect-standalone.properties \
        <path-to-confluent>/etc/kafka-connect-replicator/quickstart-replicator.properties
                
    The method or commands you use to start |crep| is dependent on your
    application setup, and may differ from this example. See the :ref:`config-and-run-replicator`.
            
#.  Stop all producers that are producing to Kafka.

#.  Wait until the replication lag is 0.

    See :ref:`monitor-replicator-lag`.

#.  Stop |crep|.

#.  Enable mode changes in the self-managed source |sr| properties file by adding the following to the
    configuration and restarting.  
    
    :: 
    
        mode.mutability=true
       
    .. important:: Modes are only supported starting with version 5.2 of |sr|. 
                   This step and the one following (set |sr| to READY-ONLY) are 
                   precautionary and not strictly necessary. If using version `5.1` 
                   of |sr| or earlier, you can skip these two steps if you make  
                   certain to stop all producers so that no further schemas are 
                   registered in the source |sr|.
    
#.  Set the source |sr| to READ-ONLY mode. 

    .. code:: bash
    
        curl -u <schema-registry-api-key>:<schema-registry-api-secret> -X PUT -H "Content-Type: application/json" "http://destregistry:8081/mode" --data '{"mode": "READONLY"}'

#.  Set the destination |sr| to READ-WRITE mode. 

    .. code:: bash
    
        curl -u <schema-registry-api-key>:<schema-registry-api-secret> -X PUT -H "Content-Type: application/json" "http://destregistry:8081/mode" --data '{"mode": "READWRITE"}'    
    
#.  Stop all consumers.

#.  Configure all consumers to point to the destination |sr| in the cloud and restart them.

    For example, if you are configuring |sr| in a Java client, change |sr| URL
    from source to destination either in the code or in a properties file that
    specifies the |sr| URL, type of authentication USER_INFO, and credentials).
    
    See :ref:`sr-tutorial-java-consumers` for further examples.
    
#.  Configure all producers to point to the destination |sr| in the cloud and restart them.

    See :ref:`sr-tutorial-java-producers` for further examples.

#.  (Optional) Stop the source |sr|.


