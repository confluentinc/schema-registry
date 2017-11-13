.. _schemaregistry_kafka_connect:

Kafka Connect
=============

Kafka Connect and Schema Registry integrate to capture schema information from connectors. :ref:`Kafka Connect converters <connect_converters>`
provide a mechanism for converting data from the internal data types use by Kafka Connect to data types represented as Avro.
The AvroConverter will convert existing Avro data to the internal data types used by Kafka Connect. This will provide schema
information to Sink Connectors.

Example Configuration
---------------------

Configuring Kafka Connect to use the schema registry requires the user to change the ``key.converter`` and ``value.converter``
properties in the :ref:`Connect worker configuration <connect_configuring_workers>`.

.. sourcecode:: properties

    key.converter=io.confluent.connect.avro.AvroConverter
    key.converter.schema.registry.url=http://schema-registry:8081
    value.converter=io.confluent.connect.avro.AvroConverter
    value.converter.schema.registry.url=http://schema-registry:8081


Configuration Options
---------------------

``schema.registry.url``
  Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas.

  * Type: list
  * Default: ""
  * Importance: high

``auto.register.schemas``
  Specify if the Serializer should attempt to register the Schema with Schema Registry

  * Type: boolean
  * Default: true
  * Importance: medium


``max.schemas.per.subject``
  Maximum number of schemas to create or cache locally.

  * Type: int
  * Default: 1000
  * Importance: low