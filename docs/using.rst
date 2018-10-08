.. _schemaregistry_using:

Using |sr|
==========

.. contents::
    :local:
    :depth: 1

Starting |sr|
-------------
Start |sr| and its dependent services |zk| and Kafka. Each service reads its configuration from its property files under
``etc``.

-------------------------------
Development or Test Environment
-------------------------------

You can use the Confluent CLI to start |sr| and its dependent services with this command:

.. sourcecode:: bash

     confluent start schema-registry

.. include:: ../../includes/cli.rst
      :start-line: 2
      :end-line: 5

----------------------
Production Environment
----------------------

Start each |cp| service in its own terminal using this order of operations:

#. Start |zk|. Run this command in its own terminal.

   .. sourcecode:: bash

      bin/zookeeper-server-start ./etc/kafka/zookeeper.properties

#. Start Kafka. Run this command in its own terminal.

   .. sourcecode:: bash

      bin/kafka-server-start ./etc/kafka/server.properties

#. Start |sr|. Run this command in its own terminal.

   .. sourcecode:: bash

      bin/schema-registry-start ./etc/schema-registry/schema-registry.properties

.. ifconfig:: platform_docs

   See the :ref:`installation` for a more detailed explanation of how
   to get these services up and running.

Common |sr| Usage Examples
--------------------------

.. tip:: For a detailed example that uses |sr| configured with security, see the :ref:`Confluent Platform demo <cp-demo>`.

These examples use curl commands to interact with the |sr| :ref:`API <schemaregistry_api>`.

-------------------------------------------------------------------
Registering a New Version of a Schema Under the Subject "Kafka-key"
-------------------------------------------------------------------

.. sourcecode:: bash

      curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
        http://localhost:8081/subjects/Kafka-key/versions
      {"id":1}


---------------------------------------------------------------------
Registering a New Version of a Schema Under the Subject "Kafka-value"
---------------------------------------------------------------------


.. sourcecode:: bash

      curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
         http://localhost:8081/subjects/Kafka-value/versions
      {"id":1}


---------------------------------------------------------------------
Registering an Existing Schema to a New Subject Name
---------------------------------------------------------------------

Use case: there is an existing schema registered to a subject called ``Kafka1``, and this same schema needs to be available to another subjected called ``Kafka2``.
The following one-line command reads the existing schema from ``Kafka1-value`` and registers it to ``Kafka2-value``.
It assumes the tool ``jq`` is installed on your machine.

.. sourcecode:: bash

      curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "{\"schema\": $(curl -s http://localhost:8081/subjects/Kafka1-value/versions/latest | jq '.schema')}" \
         http://localhost:8081/subjects/Kafka2-value/versions
      {"id":1}


--------------------
Listing All Subjects
--------------------

.. sourcecode:: bash

      curl -X GET http://localhost:8081/subjects
      ["Kafka-value","Kafka-key"]


-----------------------------------------
Fetching a Schema by Globally Unique ID 1
-----------------------------------------

.. sourcecode:: bash

      curl -X GET http://localhost:8081/schemas/ids/1
      {"schema":"\"string\""}

----------------------------------------------------------------------
Listing All Schema Versions Registered Under the Subject "Kafka-value"
----------------------------------------------------------------------

.. sourcecode:: bash

      curl -X GET http://localhost:8081/subjects/Kafka-value/versions
      [1]

--------------------------------------------------------------------
Fetch Version 1 of the Schema Registered Under Subject "Kafka-value"
--------------------------------------------------------------------

.. sourcecode:: bash

      curl -X GET http://localhost:8081/subjects/Kafka-value/versions/1
      {"subject":"Kafka-value","version":1,"id":1,"schema":"\"string\""}

-----------------------------------------------------------------------
Deleting Version 1 of the Schema Registered Under Subject "Kafka-value"
-----------------------------------------------------------------------

.. sourcecode:: bash

      curl -X DELETE http://localhost:8081/subjects/Kafka-value/versions/1
      1

-----------------------------------------------------------
Registering the Same Schema Under the Subject "Kafka-value"
-----------------------------------------------------------

.. sourcecode:: bash

      curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
         http://localhost:8081/subjects/Kafka-value/versions
      {"id":1}

------------------------------------------------------------------------
Deleting the Most Recently Registered Schema Under Subject "Kafka-value"
------------------------------------------------------------------------

.. sourcecode:: bash

      curl -X DELETE http://localhost:8081/subjects/Kafka-value/versions/latest
      2

-----------------------------------------------------------
Registering the Same Schema Under the Subject "Kafka-value"
-----------------------------------------------------------

.. sourcecode:: bash

      curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
         http://localhost:8081/subjects/Kafka-value/versions
      {"id":1}

-------------------------------------------------
Fetching the Schema Again by Globally Unique ID 1
-------------------------------------------------

.. sourcecode:: bash

      curl -X GET http://localhost:8081/schemas/ids/1
      {"schema":"\"string\""}

------------------------------------------------------------
Checking if a Schema Is Registered Under Subject "Kafka-key"
------------------------------------------------------------

.. sourcecode:: bash

      curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
        http://localhost:8081/subjects/Kafka-key
      {"subject":"Kafka-key","version":3,"id":1,"schema":"\"string\""}

------------------------------------------------------------------------------------
Testing Compatibility of a Schema with the Latest Schema Under Subject "Kafka-value"
------------------------------------------------------------------------------------

.. sourcecode:: bash

      curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
        http://localhost:8081/compatibility/subjects/Kafka-value/versions/latest
      {"is_compatible":true}

----------------------------
Getting the Top Level Config
----------------------------

.. sourcecode:: bash

      curl -X GET http://localhost:8081/config
      {"compatibilityLevel":"BACKWARD"}

--------------------------------------------
Updating Compatibility Requirements Globally
--------------------------------------------

.. sourcecode:: bash

      curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"compatibility": "NONE"}' \
        http://localhost:8081/config
      {"compatibility":"NONE"}

-------------------------------------------------------------------
Updating Compatibility Requirements Under the Subject "Kafka-value"
-------------------------------------------------------------------

.. sourcecode:: bash

      curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"compatibility": "BACKWARD"}' \
        http://localhost:8081/config/Kafka-value
      {"compatibility":"BACKWARD"}

-----------------------------------------------------------------------
Deleting All Schema Versions Registered Under the Subject "Kafka-value"
-----------------------------------------------------------------------

.. sourcecode:: bash

      curl -X DELETE http://localhost:8081/subjects/Kafka-value
      [3]

--------------------
Listing All Subjects
--------------------

.. sourcecode:: bash

      curl -X GET http://localhost:8081/subjects
      ["Kafka-key"]

