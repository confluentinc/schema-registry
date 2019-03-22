.. _schemaregistry_migrate:

Migrate an Existing |sr| to |ccloud| 
====================================

:ref:`cloud-home` is a fully managed streaming data service based on open source
|ak-tm|. Just as you can “lift and shift” your Kafka applications from
self-managed Kafka to Confluent Cloud, you can do the same with |sr-long|.

If you already use |sr| to manage schemas for Kafka applications, and want
to move some or all of that data and schema management to the cloud, you can use
:ref:`connect_replicator` to migrate your existing schemas to |sr-ccloud|.

Both bridge to cloud use cases ("lift and shift" all to cloud and "extend to
cloud" hybrid deployments) involve a one-time schema migration.  

Before migration, the destination |sr| is empty and set to IMPORT mode to
prevent clients from writing to this |sr| cluster.  

After migration, set the source |sr| to READ-ONLY mode, and the destination |sr|
to READ-WRITE mode.

The detailed procedure is as follows:

#.  Set the destination Schema Registry to IMPORT mode.  For example: 

    .. code:: bash

        curl -X PUT -H "Content-Type: application/json" -H "Authorization: Basic dGVuYW50Mi1rZXk6bm9oYXNo" "http://destregistry:8081/mode" --data '{"mode": "IMPORT"}'

#.  Configure Replicator with Schema Registry information: 


    .. code:: bash
    
        schema.topic=_schemas
        schema.registry.url=http://somehost:8081
        schema.registry.client.basic.auth.credentials.source=USER_INFO
        schema.registry.client.basic.auth.user.info=<user>:<password>
  
    
#.  Start Replicator so that it can perform the one-time schema migration. 

#.  Stop all producers.

#.  Wait until the replication lag is 0.

#.  Stop Replicator.

#.  Enable mode changes in the source Schema Registry by adding the following to the
    configuration and restarting.  (Note: modes are only supported in version 5.2 of
    Schema Registry.  Steps 7 and 8 are only precautionary and are not strictly
    necessary.  If using version 5.1 of Schema Registry or earlier, skips 7 and 8
    can be skipped, as long as all producers have indeed been stopped so that no
    further schemas are registered in the source Schema Registry.)
    
    
    .. code:: bash
    
       mode.mutability=true

    
#.  Set the source |sr| to READ-ONLY mode. 

    .. code:: bash
    
       curl -X PUT -H "Content-Type: application/json" -H "Authorization: Basic dGVuYW50Mi1rZXk6bm9oYXNo" "http://sourceregistry:8081/mode" --data '{"mode": "READONLY"}'

    
#.  Set the source |sr| to READ-ONLY mode. 

    .. code:: bash
    
       curl -X PUT -H "Content-Type: application/json" -H "Authorization: Basic dGVuYW50Mi1rZXk6bm9oYXNo" "http://sourceregistry:8081/mode" --data '{"mode": "READONLY"}'

#.  Set the destination |sr| to READ-WRITE mode. 


    .. code:: bash
    
    curl -X PUT -H "Content-Type: application/json" -H "Authorization: Basic dGVuYW50Mi1rZXk6bm9oYXNo" "http://destregistry:8081/mode" --data '{"mode": "READWRITE"}'
    
    
#.  Stop all consumers.

#.  Configure all consumers to point to the destination |sr| in the cloud and restart them.

#.  Configure all producers to point to the destination |sr| in the cloud and restart them.

#.  (Optional) Stop the source |sr|.
