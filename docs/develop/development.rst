.. _sr-development:

|sr| Development
================

To build a development version, you may need a development versions of
`common <https://github.com/confluentinc/common>`_ and
`rest-utils <https://github.com/confluentinc/rest-utils>`_.  After
installing these, you can build |sr|
with Maven. All the standard lifecycle phases work. During development, use

.. sourcecode:: bash

     mvn compile

to build,

.. sourcecode:: bash

     mvn test

to run the unit and integration tests, and

.. sourcecode:: bash

       mvn exec:java

to run an instance of |sr| against a local Kafka cluster (using
the default configuration included with Kafka).

To create a packaged version, optionally skipping the tests:

.. sourcecode:: bash

      mvn package [-DskipTests]

This will produce a version ready for production in
``package/target/kafka-schema-registry-package-$VERSION-package`` containing a directory layout
similar
to the packaged binary versions. You can also produce a standalone fat jar using the
``standalone`` profile:

.. sourcecode:: bash

      mvn package -P standalone [-DskipTests]

generating
``package/target/kafka-schema-registry-package-$VERSION-standalone.jar``, which includes all the
dependencies as well.
