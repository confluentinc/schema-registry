.. _schemaregistry_security:

Security Overview
-----------------
The |sr| currently supports all Kafka security features, including: communication and authentication with a
secure Kafka cluster over SSL; authentication with |zk| over SASL; authentication with Kafka over SASL; and
end-user REST API calls over HTTPS.

For more details, check the :ref:`configuration options<schemaregistry_config>`.

Kafka Store
~~~~~~~~~~~
The |sr| uses Kafka to persist schemas, and all Kafka security features are supported by the |sr|.

|zk|
~~~~~~~~~
The |sr| supports both unauthenticated and SASL authentication to |zk|.

Setting up |zk| SASL authentication for the |sr| is similar to Kafka's setup. Namely,
create a keytab for the |sr|, create a JAAS configuration file, and set the appropriate JAAS Java properties.

In addition to the keytab and JAAS setup, be aware of the `zookeeper.set.acl` setting. This setting, when set to `true`,
enables |zk| ACLs, which limits access to znodes.

Important: if `zookeeper.set.acl` is set to `true`, the |sr|'s service name must be the same as Kafka's, which
is `kafka` by default. Otherwise, the |sr| will fail to create the `_schemas` topic, which will cause a leader
not available error in the DEBUG log. The |sr| log will show `org.apache.kafka.common.errors.TimeoutException: Timeout expired while fetching topic metadata`
when Kafka does not set |zk| ACLs but the |sr| does. The |sr|'s service name can be set
either with `kafkastore.sasl.kerberos.service.name` or in the JAAS file.

If the |sr| has a different service name than Kafka, at this time `zookeeper.set.acl` must be set to `false`
in both the |sr| and Kafka.
