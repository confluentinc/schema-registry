.. _schemaregistry_security:

Security Overview
-----------------
The Schema Registry currently only supports communication with a secure Kafka cluster over SSL. At this time, https and ZooKeeper security are not yet supported. Kafka SASL authentication is not supported yet either.

Kafka Store
~~~~~~~~~~~
The Schema Registry uses Kafka to persist schemas. The following Kafka security configurations are currently supported:

* SSL encryption
* SSL authentication
