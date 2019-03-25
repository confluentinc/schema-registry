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

#.  Configure Replicator with Schema Registry information.


    :: 
    
        schema.topic=_schemas
        schema.registry.url=http://somehost:8081
        schema.registry.client.basic.auth.credentials.source=USER_INFO
        schema.registry.client.basic.auth.user.info=<user>:<password>
  
    
#.  Start Replicator so that it can perform the one-time schema migration. 

    .. code:: bash
    
        TBD example "start replicator" command

#.  Stop all producers.

    .. code:: bash

        TBD example "stop producers" command

#.  Wait until the replication lag is 0.

    .. code:: bash

        TBD example output 

#.  Stop Replicator.

    .. code:: bash

        TBD example "stop producers" command

#.  Enable mode changes in the source Schema Registry by adding the following to the
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

    .. code:: bash

        TBD example output 

#.  Configure all consumers to point to the destination |sr| in the cloud and restart them.

    .. code:: bash

        TBD example output 

#.  Configure all producers to point to the destination |sr| in the cloud and restart them.

    .. code:: bash

        TBD example output 

#.  (Optional) Stop the source |sr|.

    .. code:: bash

        TBD example output 
