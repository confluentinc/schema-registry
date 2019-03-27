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
--------------------

For continuous migration, you can use your self-managed |sr| as a primary and
|sr-ccloud| as a secondary. New schemas will be registered directly to the
self-managed |sr|, and |crep| will continuously copy schemas from it to
|sr-ccloud|, which is set to IMPORT mode.

**TBD diagram showing continuous migration**

One-time Migration
------------------

Choose a one-time migration to move all data to a fully-managed |ccloud|
service. In this case, you migrate your existing self-managed |sr| to
|sr-ccloud| as a primary. All new schemas are registered to |sr-ccloud|. In the
scenario, there is no migration from |sr-ccloud| back to the self-managed |sr|.

**TBD diagram showing one-time migration**


--------
Run Book
--------

To migrate |sr| to |ccloud|, follow these steps:

#.  Set the destination Schema Registry to IMPORT mode.  For example: 

    .. code:: bash

        curl -X PUT -H "Content-Type: application/json" -H "Authorization: Basic dGVuYW50Mi1rZXk6bm9oYXNo" "http://destregistry:8081/mode" --data '{"mode": "IMPORT"}'
        
    .. tip:: In the above command, authorization is provided as a Base64 encoded API key and API secret, which is recommended. 
    
    Here is an example showing one way to accomplish Base64 encoding and use environment variables to simplify the command to set |sr| mode. 
    
    - Pipe the key pair through a utility called `base64` and assign the result to a variable `BASIC_AUTH`.
    - Assign the Schema Registry URL to another variable `CCSR_URL`.
    - Finally, use both the variables in the `curl -X PUT -H ` command to set the destination Schema Registry to IMPORT mode.
    
    .. code:: bash

        export BASIC_AUTH=$(echo -n 2GXRGGJJ2BNUNPFE:uZoTd8z/mXWqYPLEU++NTYjOOIZmd2e3BC8btZX4nyI4RNFbItwBSTekT1ntz511 | base64)

        export CCSR_URL=https://psrc-e8j00.us-west-2.aws.devel.cpdev.cloud

        curl -v -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" -H "Authorization: Basic ${BASIC_AUTH}" ${CCSR_URL}/mode --data '{"mode": "IMPORT"}'


#.  Configure Replicator with Schema Registry information.

    :: 
    
        // Schema Registry migration topics
        topic.whitelist=mytopic1,mytopic2,_schemas
        schema.topic=_schemas
        
        // Connection settings for destination Schema Registry
        schema.registry.url=http://somehost:8081
        schema.registry.client.basic.auth.credentials.source=USER_INFO
        schema.registry.client.basic.auth.user.info=<user>:<password>
  
  
    In the example, the first two lines identify the topics to be read from in
    the source cluster.
     
    - `topic.whitelist` lists all topics that will be replicated from source to destination.
    - `schema.topic` defines the topic that contains all the schemas to be replicated.
     
    The last three lines specify connection information for the destination
    |sr|, the same as are configured in |ccloud|:
     
    - `schema.registry.url`is the location of your 
    

#.  Start Replicator so that it can perform the one-time schema migration. 
    
    The method or commands you use to start replicator is dependent on your
    application setup. See the :ref:`replicator tutorial` <replicator-quickstart>`.
        
#.  Stop all producers that are producing to Kafka.

#.  Wait until the replication lag is 0.


    See :ref: replicator-tuning.rst#monitoring-replicator-lag.


#.  Stop Replicator.


#.  Enable mode changes in the self-managed source Schema Registry properties file by adding the following to the
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
    
        curl -X PUT -H "Content-Type: application/json" -H "Authorization: Basic dGVuYW50Mi1rZXk6bm9oYXNo" "http://sourceregistry:8081/mode" --data '{"mode": "READONLY"}'


#.  Set the destination |sr| to READ-WRITE mode. 

    .. code:: bash
    
        curl -X PUT -H "Content-Type: application/json" -H "Authorization: Basic dGVuYW50Mi1rZXk6bm9oYXNo" "http://destregistry:8081/mode" --data '{"mode": "READWRITE"}'
    
    
#.  Stop all consumers.


#.  Configure all consumers to point to the destination |sr| in the cloud and restart them.

    For example, if you are configuring |sr| in a Java client, change |sr| URL
    from source to destination either in the code or in a properties file that
    specifies the |sr| URL, type of authentication USER_INFO, and credentials).
    
    See :ref: schema_registry_tutorial.rst#java-consumers for further examples.
    

#.  Configure all producers to point to the destination |sr| in the cloud and restart them.

    See :ref: schema_registry_tutorial.rst#java-producers for further examples.
    

#.  (Optional) Stop the source |sr|.


