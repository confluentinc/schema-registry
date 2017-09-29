.. _schemaregistry_security:

Security Overview
-----------------
The Schema Registry currently supports all Kafka security features, including: communication and authentication with a
secure Kafka cluster over SSL; authentication with ZooKeeper over SASL; authentication with Kafka over SASL; and
end-user REST API calls over HTTPS.

For more details, check the :ref:`configuration options<schemaregistry_config>`.

Kafka Store
~~~~~~~~~~~
The Schema Registry uses Kafka to persist schemas, and all Kafka security features are supported by the Schema Registry.

ZooKeeper
~~~~~~~~~
The Schema Registry supports both unauthenticated and SASL authentication to Zookeeper.

Setting up ZooKeeper SASL authentication for the Schema Registry is similar to Kafka's setup. Namely,
create a keytab for the Schema Registry, create a JAAS configuration file, and set the appropriate JAAS Java properties.

In addition to the keytab and JAAS setup, be aware of the `zookeeper.set.acl` setting. This setting, when set to `true`,
enables ZooKeeper ACLs, which limits access to znodes.

Important: if `zookeeper.set.acl` is set to `true`, the Schema Registry's service name must be the same as Kafka's, which
is `kafka` by default. Otherwise, the Schema Registry will fail to create the `_schemas` topic, which will cause a leader
not available error in the DEBUG log. The Schema Registry log will show `org.apache.kafka.common.errors.TimeoutException: Timeout expired while fetching topic metadata`
when Kafka does not set ZooKeeper ACLs but the Schema Registry does. The Schema Registry's service name can be set
either with `kafkastore.sasl.kerberos.service.name` or in the JAAS file.

If the Schema Registry has a different service name than Kafka, at this time `zookeeper.set.acl` must be set to `false`
in both the Schema Registry and Kafka.

Upgrade from HTTP to HTTPS for REST API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default Schema Registry ships with configs that allow you to make REST API calls over HTTP. If you would want to upgrade to HTTPS in an existing cluster, you need to perform the outlined steps:

- Add/Modify the ``listeners`` config  to include HTTPS. For example: http://0.0.0.0:8081,https://0.0.0.0:8082
- Configure the appropriate SSL configurations

    - ``ssl.keystore.`` configs to setup keystore related configs
    - ``ssl.truststore.`` configs to setup truststore related configs (required only when ``ssl.client.auth`` set to true)
   
- Do a rolling bounce of the cluster

By default, all communications(forwarding from slave to master for schema register) between the
schema registry nodes is HTTP. If you need your cluster to not to be open for HTTP, then perform
the following outlined steps:

- Enable HTTPS as mentioned in first section of upgrade (both HTTP & HTTPS will be enabled)
- Configure ``schema.registry.inter.instance.protocol`` to `https` in all the nodes
- Do a rolling bounce of the cluster
- Remove http listener from the ``listeners`` in all the nodes
- Do a rolling bounce of the cluster



