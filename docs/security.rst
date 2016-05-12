.. _schemaregistry_security:

Security Overview
-----------------
The Schema Registry currently supports two security features: communication with a secure Kafka cluster over SSL; and API calls over https. At this time, ZooKeeper security and Kafka SASL authentication are not yet supported.

For more details, check the :ref:`configuration options<schemaregistry_config>`.

Kafka Store
~~~~~~~~~~~
The Schema Registry uses Kafka to persist schemas. The following Kafka security configurations are currently supported:

* SSL encryption
* SSL authentication
