.. _schema_registry_tutorial:

Confluent Schema Registry Tutorial
==================================

This tutorial provides an example on how to use |sr-long|.


Overview
~~~~~~~~

This tutorial provides a step-by-step example to use |sr-long|.
It walks through the steps to enable client applications to read and write Avro data with compatibility checks as schemas evolve.

Benefits
^^^^^^^^

Kafka producers write data to Kafka topics and Kafka consumers read data from Kafka topics.
There is an implicit "contract" that producers write data with a schema that can be read by consumers, even as producers and consumers evolve their schemas.
|sr-long| helps ensure that this contract is met with compatibility checks.

It is useful to think about schemas as APIs.
Applications depend on APIs and expect any changes made to APIs are still compatible and applications can still run.
Similarly, streaming applications depend on schemas and expect any changes made to schemas are still compatible and they can still run.
Schema evolution requires compatibility checks to ensure that the producer-consumer contract is not broken. 
This is where |sr-long| helps: it provides centralized schema management and compatibility checks as schemas evolve.

For a more in-depth understanding of the benefits of Avro and |sr|, refer to the following resources:

* `Why Avro For Kafka Data <https://www.confluent.io/blog/avro-kafka-data/>`_
* `Yes, Virginia, You Really Do Need a Schema Registry <https://www.confluent.io/blog/schema-registry-kafka-stream-processing-yes-virginia-you-really-need-one/>`_

Target Audience
^^^^^^^^^^^^^^^

The target audience is a developer writing Kafka streaming applications, who wants to build a robust application leveraging Avro data and |sr-long|.

This tutorial is not meant to cover the operational aspects of running the |sr| service. For production deployments of |sr-long|, refer to :ref:`Schema Registry Operations<schemaregistry_operations>`.

Before You Begin
~~~~~~~~~~~~~~~~

Prerequisites
^^^^^^^^^^^^^

Before proceeding with this tutorial

#. Verify have installed on your local machine:

   * Java 1.8 to run |cp|
   * Maven to compile the client Java code
   * `jq` tool to nicely format the results from querying the |sr| REST endpoint

#. Use the :ref:`quickstart` to bring up |cp|. With a single-line command, you can have running on your local machine a basic Kafka cluster with |sr-long| and other services.

   .. sourcecode:: bash

      $ confluent start
   
      This CLI is intended for development only, not for production
      https://docs.confluent.io/current/cli/index.html
   
      Starting zookeeper
      zookeeper is [UP]
      Starting kafka
      kafka is [UP]
      Starting schema-registry
      schema-registry is [UP]
      Starting kafka-rest
      kafka-rest is [UP]
      Starting connect
      connect is [UP]
      Starting ksql-server
      ksql-server is [UP]
      Starting control-center
      control-center is [UP]


#. Clone the |cp| `examples` repo in GitHub and work in the `clients/avro/` subdirectory.

   .. sourcecode:: bash

      $ git clone https://github.com/confluentinc/examples.git
      $ cd examples/clients/avro
   

.. _schema_registry_tutorial_definition:

Terminology Levelset
^^^^^^^^^^^^^^^^^^^^

First let us levelset on terminology: what is a `topic` versus a `schema` versus a `subject`.

A Kafka `topic` contains messages, and each message is a key-value pair.
Either the message key or the message value, or both, can be serialized as Avro.
A `schema` defines the structure of the Avro data format.
The Kafka topic name can be independent of the schema name.
By default, |sr| defines a scope in which schemas for that Kafka topic can evolve, and that scope is the `subject`.

As a practical example, let's say a retail business is streaming transactions in a Kafka topic called `transactions`.
A producer is writing data with a schema `Payment` to that Kafka topic.
If the producer is serializing the message value as Avro, |sr| has a subject called `transactions-value`.
If the producer is also serializing the message key as Avro, |sr| would have a subject called `transactions-key`, but for simplicity, in this tutorial you only consider the message value.
The |sr| subject `transactions-value` has at least one schema called `Payment`.
The subject `transactions-value` defines the scope in which schemas for the topic transactions can evolve and |sr| does compatibility checking within this scope.
If developers evolve the schema `Payment` and produce new messages to the topic `transactions`, |sr| checks that those newly evolved schemas are compatible with older schemas in the subject `transactions-value` and adds those new schemas to the subject.

.. _schema_registry_tutorial_definition:

Schema Definition
~~~~~~~~~~~~~~~~~

The first thing developers need to do is agree on a basic schema for data.
Client applications form a contract: producers will write data in a schema and consumers will be able to read that data.
Of course, applications can use many schemas for many topics, but in this tutorial you will look at one.

Consider the `original Payment schema <https://github.com/confluentinc/examples/blob/DEVX-380/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment.avsc>`_:

.. sourcecode:: json

   $ cat src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment.avsc
   {"namespace": "io.confluent.examples.clients.basicavro",
    "type": "record",
    "name": "Payment",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"}
    ]
   }

Let's break down what this schema defines

* ``namespace``: a fully qualified name that avoids schema naming conflicts
* ``type``: `Avro data type <https://avro.apache.org/docs/1.8.1/spec.html#schemas>`_, one of `record`, `enum, `union`, `array`, `map`, `fixed`
* ``name``: unique schema name in this namespace
* ``fields``: one or more simple or complex data types for a `record`. The first field in this record is called `id`, and it is of type `string`. The second field in this record is called `amount`, and it is of type `double`.


Client Applications Writing Avro
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Apache Kafka applications using Avro data and |sr-long| need to specify at least two configuration parameters:

* Avro serializer or deserializer
* URL to the |sr-long|

There are two basic types of Avro records that your application can use: a specific code-generated class or a generic record.
The examples below demonstrate how to use the specific `Payment` class, because using the specific classes are easier to work with.
However, in scenarios where you need to work dynamically with data of any type, use `GenericRecord <https://docs.confluent.io/current/streams/developer-guide/datatypes.html#avro>`_.


Java Producers
^^^^^^^^^^^^^^

Java applications that have Kafka producers using Avro require ``pom.xml`` files to include:

* Avro dependencies to serialize data as Avro, including ``org.apache.avro.avro`` and ``io.confluent.kafka-avro-serializer``
* Avro plugin ``avro-maven-plugin`` to generate Java class files from the source schema

For a full pom.xml example, refer to this `pom.xml <https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/pom.xml>`_.

Within the application, Java producers that are serializing data as Avro set two main configurations parameters:

* Avro serializer for the Kafka value (or Kafka key)
* URL to the |sr-long|

Then the producer can write records where the Kafka value is of `Payment` class.
When constructing the producer, configure the message value class to use the application's code-generated `Payment` class.
For example:

.. sourcecode:: java

   import io.confluent.kafka.serializers.KafkaAvroSerializer;
   import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

   ...
   props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
   props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
   ...

   ...
   KafkaProducer<String, Payment> producer = new KafkaProducer<String, Payment>(props));
   final Payment payment = new Payment(orderId, 1000.00d);
   final ProducerRecord<String, Payment> record = new ProducerRecord<String, Payment>(TOPIC, payment.getId().toString(), payment);
   producer.send(record);
   ...

For a full Java producer example, refer to `the producer example <https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/src/main/java/io/confluent/examples/clients/basicavro/ProducerExample.java>`_.


Java Consumers
^^^^^^^^^^^^^^

Java applications that have Kafka consumers using Avro require `pom.xml` files to include:

* Avro dependencies to serialize data as Avro, including ``org.apache.avro.avro`` and ``io.confluent.kafka-avro-serializer``
* Avro plugin ``avro-maven-plugin`` to generate Java class files from the source schema

For a full `pom.xml` example, refer to `sample pom.xml <https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/pom.xml>`_.

Within the application, Java consumers that are deserializing data as Avro set two main configurations parameters:

* Avro deserializer for the Kafka value (or Kafka key)
* URL to the |sr-long|

Then the consumer can read records where the Kafka value is of `Payment` class.
To ensure that the object is deserialized using the application's code-generated `Payment` class, configure the deserializer to use Avro `SpecificRecord`, i.e., ``SPECIFIC_AVRO_READER_CONFIG`` should be set to _true_.
For example:

.. sourcecode:: java

   import io.confluent.kafka.serializers.KafkaAvroDeserializer;
   import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

   ...
   props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
   props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); 
   props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
   ...

   ...
   KafkaConsumer<String, Payment> consumer = new KafkaConsumer<>(props));
   consumer.subscribe(Collections.singletonList(TOPIC));
   while (true) {
     ConsumerRecords<String, Payment> records = consumer.poll(100);
     for (ConsumerRecord<String, Payment> record : records) {
       String key = record.key();
       Payment value = record.value();
     }
   }
   ...

For a full Java consumer example, refer to `the consumer example <https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/src/main/java/io/confluent/examples/clients/basicavro/ConsumerExample.java>`_.


Other Kafka Clients
^^^^^^^^^^^^^^^^^^^

The objective of this tutorial is to learn about Avro and |sr| centralized schema management and compatibility checks.
To keep examples simple, this tutorial focuses on Java producers and consumers, but other Kafka clients work in similar ways.
For examples of other Kafka clients interoperating with Avro and |sr|:

* `KSQL <https://docs.confluent.io/current/ksql/docs/installation/server-config/avro-schema.html#configuring-avro-and-sr-for-ksql>`_
* `Kafka Streams <https://docs.confluent.io/current/streams/developer-guide/datatypes.html#avro>`_
* `Kafka Connect <https://docs.confluent.io/current/schema-registry/docs/connect.html#using-kafka-connect-with-sr>`_
* `Confluent REST Proxy <https://docs.confluent.io/current/kafka-rest/docs/api.html#post--topics-(string-topic_name)-partitions-(int-partition_id)>`_
* `Non-Java clients based on librdkafka <https://docs.confluent.io/current/clients/index.html>`_ , including Confluent Python, Confluent Go, Confluent DotNet


Centralized Schema Management
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Schemas in Schema Registry
^^^^^^^^^^^^^^^^^^^^^^^^^^

At this point, you have producers serializing Avro data and consumers deserializing Avro data.
The producers are registering schemas and consumers are retrieving schemas.
You can view subjects and associated schemas via the REST endpoint in |sr|.

View all the subjects registered in |sr| (assuming |sr| is running on the local machine listening on port 8081):

.. sourcecode:: bash

   $ curl --silent -X GET http://localhost:8081/subjects/ | jq .  
   [
     "transactions-value"
   ]

In this example, the Kafka topic `transactions` has messages whose value, i.e., payload, is Avro.
View the associated subject `transactions-value` in |sr|:

.. sourcecode:: bash

   $ curl --silent -X GET http://localhost:8081/subjects/transactions-value/versions/latest | jq .
   {
     "subject": "transactions-value",
     "version": 1,
     "id": 1,
     "schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"
   }

Let's break down what this version of the schema defines

* `subject`: the scope in which schemas for the messages in the topic `transactions` can evolve
* `version`: the schema version for this subject, which starts at 1 for each subject
* `id`: the globally unique schema version id, unique across all schemas in all subjects
* `schema`: the structure that defines the schema format

The schema is identical to the :ref:`schema file defined for Java client applications<schema_registry_tutorial_definition>`.
Notice in the output above, the schema is escaped JSON, i.e., the double quotes are preceded with backslashes.

Based on the schema id, you can also retrieve the associated schema by querying |sr| REST endpoint:

.. sourcecode:: bash

   $ curl --silent -X GET http://localhost:8081/schemas/ids/1 | jq .
   {
     "schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"
   }

If you are using |c3|, you can view the topic schema easily from the UI:

.. figure:: c3-schema-transactions.png
    :align: center



Schema IDs in Messages
^^^^^^^^^^^^^^^^^^^^^^

Integration with |sr-long| means that Kafka messages do not need to be written with the entire Avro schema.
Instead, Kafka messages are written with the schema _id_.
The producers writing the messages and the consumers reading the messages must be using the same |sr| to get the same mapping between a schema and schema id.

In this example, a producer sends the new schema for `Payments` to |sr|.
|sr| registers this schema `Payments` to the subject `transactions-value`, and returns the schema id of `1` to the producer.
The producer caches this mapping between the schema and schema id for subsequent message writes, so it only contacts |sr| on the first schema write.
When a consumer reads this data, it sees the Avro schema id of `1` and sends a schema request to |sr|.
|sr| retrieves the schema associated to schema id `1`, and returns the schema to the consumer.
The consumer caches this mapping between the schema and schema id for subsequent message reads, so it only contacts |sr| the on first schema id read.


Auto Schema Registration
^^^^^^^^^^^^^^^^^^^^^^^^

By default, client applications automatically register new schemas.
If they produce new messages to a new topic, then they will automatically try to register new schemas.
This is very convenient in development environments, but in production environments we recommend that client applications do not automatically register new schemas.
Users should register schemas outside of the client application to control when schemas are registered with |sr-long| and how they evolve.

Within the application, disable automatic schema registration by setting the configuration parameter `auto.register.schemas=false`, as shown in the examples below.

.. sourcecode:: java

   props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

To manually register the schema outside of the application, send the schema to |sr| and associate it with a subject, in this case `transactions-value`.  It returns a schema id of `1`.

.. sourcecode:: bash

   $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"}' http://localhost:8081/subjects/transactions-value/versions
   {"id":1}


Schema Evolution and Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Changing Schemas
^^^^^^^^^^^^^^^^

You have seen the benefit of |sr-long| as being centralized schema management that enables client applications to register and retrieve globally unique schema ids.
The main value, however, is in enabling schema evolution.
Similar to how APIs evolve and need to be compatible for all applications that rely on old and new versions of the API, schemas also evolve and likewise need to be compatible for all applications that rely on old and new versions of the schema.
This schema evolution is a natural behavior of how applications and data develop over time.

|sr-long| allows for schema evolution and provides compatibility checks to ensure that the contract between producers and consumers is not broken.
This is especially important in Kafka because producers and consumers are decoupled applications that are sometimes developed by different teams.
Compatibility checks on schemas allow producers and consumers to update independently and evolve their schemas independently, with assurances that they can read new and legacy data.
|sr| can check compatibility of a new schema against just the latest registered schema, or if configured as transitive then it checks against _all_ previously registered schemas, not just the latest one.

These are the types of `compatibility types <https://docs.confluent.io/current/schema-registry/docs/config.html#avro-compatibility-level>`_:

* ``FORWARD``: consumers using the latest registered schema can read data written by producers using the new schema
* ``FORWARD_TRANSITIVE``: consumers using all previousely registered schemas can read data written by producers using the new schema
* ``BACKWARD``: consumers using the new schema can read data written by producers using the latest registered schema
* ``BACKWARD_TRANSITIVE``: consumers using the new schema can read data written by producers using all previously registered schemas
* ``FULL``: the new schema is forward and backward compatible with the latest registered schema
* ``FULL_TRANSITIVE``: the new schema is forward and backward compatible with all previously registered schemas
* ``NONE``: schema compatibility checks are disabled

By default, |sr| is configured for ``BACKWARD`` compatibility.
You can change this globally or per subject, but for the remainder of this tutorial, leave the default compatibility level to `backward`.


Failing Compatibility Checks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

|sr| checks schema compatibility as schemas evolve to keep the producer-consumer contract.
Without |sr| checking compatibility, your applications could break on schema changes.

In the Payment schema example, let's say the business now tracks additional information for each payment, for example, a field ``region`` that represents the place of sale.
Consider the `Payment2a schema <https://github.com/confluentinc/examples/blob/DEVX-380/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2a.avsc>`_ which includes this extra field ``region``:

.. sourcecode:: json

   $ cat src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2a.avsc
   {"namespace": "io.confluent.examples.clients.basicavro",
    "type": "record",
    "name": "Payment",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "region", "type": "string"}
    ]
   }

Before proceeding, think about whether this schema is backward compatible.
Specifically, ask yourself whether a consumer can use this new schema to read data written by producers using the older schema without the `region` field?
The answer is no.
Consumers will fail reading data with the older schema because the data does not have the `region` field, therefore it is not backward compatible.

Confluent provides a `Schema Registry Maven Plugin <https://docs.confluent.io/current/schema-registry/docs/maven-plugin.html#sr-maven-plugin>`_, which you can use to check compatibility in development.
Our sample `pom.xml <https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/pom.xml#L84-L99>`_ includes this plugin to enable compatibility checks.

.. sourcecode:: xml

      <plugin>
          <groupId>io.confluent</groupId>
          <artifactId>kafka-schema-registry-maven-plugin</artifactId>
          <version>5.0.0</version>
          <configuration>
              <schemaRegistryUrls>
                  <param>http://localhost:8081</param>
              </schemaRegistryUrls>
              <subjects>
                  <transactions-value>src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2a.avsc</transactions-value>
              </subjects>
          </configuration>
          <goals>
              <goal>test-compatibility</goal>
          </goals>
      </plugin>

It is currently configured to check compatibility of the new `Payment2a` schema for the `transactions-value` subject in |sr|.
Run the compatibility check and verify that it fails:

.. sourcecode:: bash

   $ mvn io.confluent:kafka-schema-registry-maven-plugin:5.0.0:test-compatibility
   ...
   [ERROR] Schema examples/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2a.avsc is not compatible with subject(transactions-value)
   ...

You could have also just tried to register the new schema `Payment2a` manually to |sr|, which is a useful way for non-Java clients to check compatibility.
As expected, |sr| rejects it with an error message that it is incompatible.

.. sourcecode:: bash

   $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"region\",\"type\":\"string\"}]}"}' http://localhost:8081/subjects/transactions-value/versions
   {"error_code":409,"message":"Schema being registered is incompatible with an earlier schema"}


Passing Compatibility Checks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To maintain backward compatibility, a new schema must assume default values for the new field if it is not provided.
Consider an updated `Payment2b schema <https://github.com/confluentinc/examples/blob/DEVX-380/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2b.avsc>`_ that has a default value for ``region``:

.. sourcecode:: json

   $ cat src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2b.avsc
   {"namespace": "io.confluent.examples.clients.basicavro",
    "type": "record",
    "name": "Payment",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "region", "type": "string", "default": ""}
    ]
   }

Update the `pom.xml <https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/pom.xml>`_ to refer to `Payment2b.avsc` instead of `Payment2a.avsc`.
Re-run the compatibility check and verify that it passes:

.. sourcecode:: bash

   $ mvn io.confluent:kafka-schema-registry-maven-plugin:5.0.0:test-compatibility
   ...
   [INFO] Schema examples/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2b.avsc is compatible with subject(transactions-value)
   ...

You can try registering the new schema `Payment2b` directly, and it succeeds.

.. sourcecode:: bash

   $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"region\",\"type\":\"string\",\"default\":\"\"}]}"}' http://localhost:8081/subjects/transactions-value/versions
   {"id":2}

View the latest subject for `transactions-value` in |sr|:

.. sourcecode:: bash

   $ curl --silent -X GET http://localhost:8081/subjects/transactions-value/versions/latest | jq .
   {
     "subject": "transactions-value",
     "version": 2,
     "id": 2,
     "schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"region\",\"type\":\"string\",\"default\":\"\"}]}"
   }

Notice the changes:

* `version`: changed from `1` to `2`
* `id`: changed from `1` to `2`
* `schema`: updated with the new field `region` that has a default value


Next Steps
~~~~~~~~~~

* Adapt your applications to use Avro data
* Change compatibility modes to suit your application needs
* Evolve schemas so that they fail compatibility checks
* Evolve schemas so that they pass compatibility checks
