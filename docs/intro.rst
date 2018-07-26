.. _schemaregistry_intro:

Introduction
============

|sr| provides a serving layer for your metadata. It provides a RESTful interface for storing and retrieving Avro schemas. It stores a versioned history of all schemas, provides multiple compatibility settings and allows evolution of schemas according to the configured compatibility setting. It provides serializers that plug into Kafka clients that handle schema storage and retrieval for Kafka messages that are sent in the Avro format.

Installation
------------

.. ifconfig:: platform_docs

   See the :ref:`installation instructions<installation>` for the Confluent
   Platform. Before starting the |sr| you must start Kafka.
   The :ref:`Confluent Platform quickstart<quickstart>` explains how to start
   these services locally for testing.

.. ifconfig:: not platform_docs

   You can download prebuilt versions of the |sr| as part of the
   `Confluent Platform <http://confluent.io/downloads/>`_. To install from
   source, follow the instructions in the `Development`_ section. Before
   starting the |sr| you must start Kafka.


Deployment
----------

Starting the |sr| service is simple once its dependencies are running.

Note: The |sr| version must not exceed the CP/Kafka version. That's to say |sr| 3.0 will not be compatible with Kafka 0.9.x. See the Requirements section below for version compatibility.

.. sourcecode:: bash

   $ cd confluent-3.3.0/

   # The default settings in schema-registry.properties work automatically with
   # the default settings for local ZooKeeper and Kafka nodes.
   $ bin/schema-registry-start etc/schema-registry/schema-registry.properties

If you installed Debian or RPM packages, you can simply run ``schema-registry-start``
as it will be on your ``PATH``. The ``schema-registry.properties`` file contains
:ref:`configuration settings<schemaregistry_config>`. The default configuration
included with the |sr| includes convenient defaults for a local testing setup and
should be modified for a
production deployment. By default the server starts bound to port 8081, expects |zk|
to be available at ``localhost:2181``, and a Kafka broker at ``localhost:9092``.

If you started the service in the background, you can use the following command to stop it:

.. sourcecode:: bash

   $ bin/schema-registry-stop


Development
-----------

To build a development version, you may need a development versions of
`common <https://github.com/confluentinc/common>`_ and
`rest-utils <https://github.com/confluentinc/rest-utils>`_.  After
installing these, you can build the |sr|
with Maven. All the standard lifecycle phases work. During development, use

.. sourcecode:: bash

   $ mvn compile

to build,

.. sourcecode:: bash

   $ mvn test

to run the unit and integration tests, and

.. sourcecode:: bash

     $ mvn exec:java

to run an instance of the |sr| against a local Kafka cluster (using
the default configuration included with Kafka).

To create a packaged version, optionally skipping the tests:

.. sourcecode:: bash

    $ mvn package [-DskipTests]

This will produce a version ready for production in
``package/target/kafka-schema-registry-package-$VERSION-package`` containing a directory layout
similar
to the packaged binary versions. You can also produce a standalone fat jar using the
``standalone`` profile:

.. sourcecode:: bash

    $ mvn package -P standalone [-DskipTests]

generating
``package/target/kafka-schema-registry-package-$VERSION-standalone.jar``, which includes all the
dependencies as well.


Requirements
------------

- Kafka: 2.0.0-beta180726003306

Contribute
----------

- Source Code: https://github.com/confluentinc/schema-registry
- Issue Tracker: https://github.com/confluentinc/schema-registry/issues

License
-------

The |sr| is licensed under the Apache 2 license.
