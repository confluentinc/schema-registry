.. _schemaregistry_security:

|sr| Security Overview
----------------------

|sr| currently supports all Kafka security features, including:

* :ref:`SSL encryption <encryption-ssl-schema-registry>` with a secure Kafka cluster
* :ref:`SSL authentication<authentication-ssl-schema-registry>` with a secure Kafka Cluster
* :ref:`SASL authentication<kafka_sasl_auth>`  with a secure Kafka Cluster 
* Authentication with |zk| over SASL
* :ref:`End-user REST API calls over HTTPS<schema_registry_http_https>`

For more details, check the :ref:`configuration options<schemaregistry_config>`.

If you want to see full configurations for a secured |sr|, please refer to the :ref:`Confluent Platform demo<tutorials>`


Kafka Store
~~~~~~~~~~~
|sr| uses Kafka to persist schemas, and all Kafka security features are supported by |sr|.

|zk|
~~~~~~~~~
|sr| supports both unauthenticated and SASL authentication to |zk|.

Setting up |zk| SASL authentication for |sr| is similar to Kafka's setup. Namely,
create a keytab for |sr|, create a JAAS configuration file, and set the appropriate JAAS Java properties.

In addition to the keytab and JAAS setup, be aware of the `zookeeper.set.acl` setting. This setting, when set to `true`,
enables |zk| ACLs, which limits access to znodes.

Important: if `zookeeper.set.acl` is set to `true`, |sr|'s service name must be the same as Kafka's, which
is `kafka` by default. Otherwise, |sr| will fail to create the `_schemas` topic, which will cause a leader
not available error in the DEBUG log. |sr| log will show `org.apache.kafka.common.errors.TimeoutException: Timeout expired while fetching topic metadata`
when Kafka does not set |zk| ACLs but |sr| does. |sr|'s service name can be set
either with `kafkastore.sasl.kerberos.service.name` or in the JAAS file.

If |sr| has a different service name than Kafka, at this time `zookeeper.set.acl` must be set to `false`
in both |sr| and Kafka.


.. _schema_registry_http_https:

Configuring the REST API for HTTP or HTTPS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default |sr| allows you to make REST API calls over HTTP. You may configure |sr| to allow either HTTP or HTTPS or both at the same time.

The following configuration determines the protocol used by |sr|:

``listeners``
  Comma-separated list of listeners that listen for API requests over HTTP or HTTPS or both. If a listener uses HTTPS, the appropriate SSL configuration parameters need to be set as well.

  * Type: list
  * Default: "http://0.0.0.0:8081"
  * Importance: high

On the clients, configure ``schema.registry.url`` to match the configured |sr| listener.


Additional configurations for HTTPS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you configure an HTTPS listener, there are several additional configurations for |sr|.

First, configure the appropriate SSL configurations for the keystore and optionally truststore. The truststore is required only when ``ssl.client.auth`` is set to true.

.. sourcecode:: bash

   ssl.truststore.location=/var/private/ssl/kafka.client.truststore.jks
   ssl.truststore.password=test1234
   ssl.keystore.location=/var/private/ssl/kafka.client.keystore.jks
   ssl.keystore.password=test1234
   ssl.key.password=test1234

You may specify which protocol to use while making calls between the instances of |sr|. The slave to master node calls for writes and deletes will use the specified protocol.

``schema.registry.inter.instance.protocol``
  The protocol used while making calls between the instances of |sr|. The default value is `http`. When `https` is set, `ssl.keystore.` and `ssl.truststore.` configs are used while making the call.

  * Type: string
  * Default: "http"
  * Importance: low

To configure clients to use HTTPS to |sr|:

1. On the client, configure the ``schema.registry.url`` to match the configured listener for HTTPS.

2. On the client, configure the environment variables to set the SSL keystore and truststore. You will need to set the appropriate env variable depending on the client (one of ``KAFKA_OPTS``, ``SCHEMA_REGISTRY_OPTS``, ``KSQL_OPTS``). For example:

.. sourcecode:: bash

        export KAFKA_OPTS="-Djavax.net.ssl.trustStore=/etc/kafka/secrets/kafka.client.truststore.jks \
                  -Djavax.net.ssl.trustStorePassword=confluent \
                  -Djavax.net.ssl.keyStore=/etc/kafka/secrets/kafka.client.keystore.jks \
                  -Djavax.net.ssl.keyStorePassword=confluent"


Migrating from HTTP to HTTPS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To upgrade |sr| to allow REST API calls over HTTPS in an existing cluster:

- Add/Modify the ``listeners`` config  to include HTTPS. For example: http://0.0.0.0:8081,https://0.0.0.0:8082
- Configure |sr| with appropriate SSL configurations to setup the keystore and optionally truststore
- Do a rolling bounce of the cluster

This process enables HTTPS, but still defaults to HTTP so |sr| instances can still communicate before all nodes have been restarted. They will continue to use HTTP as the default until configured not to. To switch to HTTPS as the default and disable HTTP support, perform the following steps:

- Enable HTTPS as mentioned in first section of upgrade (both HTTP & HTTPS will be enabled)
- Configure ``schema.registry.inter.instance.protocol`` to `https` in all the nodes
- Do a rolling bounce of the cluster
- Remove http listener from the ``listeners`` in all the nodes
- Do a rolling bounce of the cluster


Authorizing Access to the Schemas Topic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Relatively few services need access to |sr|, and they are likely internal, so you can restrict access via firewall rules and/or network segmentation.

Note that if you have enabled :ref:`Kafka authorization <kafka_authorization>`, you will need
to grant read and write access to this topic to |sr|'s principal.

.. sourcecode:: bash

     export KAFKA_OPTS="-Djava.security.auth.login.config=<path to JAAS conf file>"

     bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal 'User:<sr-principal>' --allow-host '*' --operation Read --topic _schemas

     bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal 'User:<sr-principal>' --allow-host '*' --operation Write --topic _schemas

.. note::
  **Removing world-level permissions:**
  In previous versions of |sr|, we recommended making the `_schemas` topic world readable and writable. Now that |sr| supports SASL, the world-level permissions can be dropped.
