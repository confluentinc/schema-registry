.. _schemaregistry_security:

Security Overview
-----------------
The Schema Registry currently supports: communication with a secure Kafka cluster over SSL; authentication with ZooKeeper
over SASL, and API calls over HTTPS. At this time, Kafka SASL authentication is not yet supported.

For more details, check the :ref:`configuration options<schemaregistry_config>`.

Kafka Store
~~~~~~~~~~~
The Schema Registry uses Kafka to persist schemas. The following Kafka security configurations are currently supported:

* SSL encryption
* SSL authentication

ZooKeeper
~~~~~~~~~
ZooKeeper SASL authentication is supported.

Setting up ZooKeeper SASL authentication for the Schema Registry is similar to Kafka's setup. Namely,
create a keytab for the Schema Registry, create a JAAS configuration file, and set the appropriate JAAS Java properties.

In addition to the keytab and JAAS setup, be aware of the `zookeeper.set.acl` setting. This setting, when set to `true`
, enables ZooKeeper ACLs, which limits access to znodes.

Important: if `zookeeper.set.acl` is set to `true`, the Schema Registry's service name must be the same as Kafka's, which
is most likely `kafka`. The Schema Registry's principal must have the correct service name, too. Otherwise, the Schema
Registry will fail to create the `_schemas` topic, which will cause a leader not available error.

If the Schema Registry has a different service name than Kafka, at this time `zookeeper.set.acl` must be set to `false`
in both the Schema Registry and Kafka.
