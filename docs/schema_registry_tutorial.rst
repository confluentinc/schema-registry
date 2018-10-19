.. _schema_registry_tutorial:

Confluent Schema Registry Tutorial
==================================

This tutorial provides an example on how to use |sr-long|.

**Contents**

.. contents::
  :local:
  :depth: 2


Overview
~~~~~~~~

This tutorial provides a step-by-step example to use |sr-long|.
It walks through the steps to enable client applications to read and write Avro data with compatibility checks as schemas evolve.

Benefits
^^^^^^^^

Kafka producers write data to Kafka topics and Kafka consumers read data from Kafka topics.
There is an implicit "contract" that producers write data with a schema that can be read by consumers, even as producers and consumers evolve their schemas.

It is useful to think about schemas as APIs.
It is critical that applications built on top of those APIs continue to work even if the APIs have changed.
Similarly, schema evolution requires compatibility checks to ensure that the producer-consumer contract is not broken. 
This is where |sr-long| helps: it provides centralized schema management and compatibility checks as schemas evolve.

For a more in-depth understanding of the benefits of Avro and |sr|, please refer to the following resources:

* `Why Avro For Kafka Data<https://www.confluent.io/blog/avro-kafka-data/>`__
* `Yes, Virginia, You Really Do Need a Schema Registry<https://www.confluent.io/blog/schema-registry-kafka-stream-processing-yes-virginia-you-really-need-one/>`__

Target Audience
^^^^^^^^^^^^^^^

The target audience is a developer writing Kafka streaming applications, who wants to build a robust application leveraging Avro data and |sr-long|.

This tutorial is not meant to cover the operational aspects of running the |sr| service. For production deployments of |sr-long|, please refer to :ref:`Schema Registry Operations<schemaregistry_operations>`.

Prerequisites
^^^^^^^^^^^^^

Before proceeding with this tutorial

# Verify have installed on your local machine:

* Java 1.8 to run |cp|
* Maven to compile the client Java code

# Use the :ref:`quickstart` to bring up |cp|. With a single-line command, you can have running on your local machine a basic Kafka cluster with |sr-long| and other services.

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


# Clone the |cp| `examples` repo in GitHub and work in the `clients/avro/` subdirectory.

.. sourcecode:: bash

   $ git clone https://github.com/confluentinc/examples.git
   $ cd examples/clients/avro


.. _schema_registry_tutorial_definition:

Terminology Levelset
~~~~~~~~~~~~~~~~~~~~

First let us levelset on terminology: what is a `schema` versus a `topic` versus a `subject`.

A Kafka topic contains messages, and each message is a key-value pair.
Either the message key or the message value, or both, can be independently serialized as Avro.
The Kafka topic name is independent of the schema name.
When a producer writes a message to a Kafka topic, it can serialize the message key or message value as Avro (or both).
By default, the subject that is registered in |sr| is derived from the Kafka topic name.

As a practical example, let's say a retail business is streaming transactions in a Kafka topic called `transactions`.
A producer is writing data with a schema `Payment` to that Kafka topic.
If the producer is serializing the message value as Avro, |sr| has a subject called `transactions-value`.
If the producer is also serializing the message key as Avro, |sr| would have a subject called `transactions-key`, but for simplicity, in this tutorial we consider only about the message value.
The |sr| subject `transactions-value` has at least one schema called `Payment`.
The |sr| subject `transactions-value` defines the scope in which schemas for the topic transactions can evolve and |sr| does compatibility checking within this scope.
If developers evolve the schema `Payment` and produce new messages to the topic `transactions`, |sr| checks that those newly evolved schemas are compatible with older schemas in the subject `transactions-value` and adds those new schemas to the subject.

.. _schema_registry_tutorial_definition:

Schema Definition
~~~~~~~~~~~~~~~~~

The first thing developers need to do is agree on a basic schema for data.
Client applications are forming a contract that producers will write data in a compatible schema and consumers will be able to read that data.
Of course, applications can use many schemas for many topics, but in this tutorial we will look at one.

Consider the `first Payment schema<https://github.com/confluentinc/examples/blob/DEVX-380/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment.avsc>`__:

.. sourcecode:: json

   $ cat src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment.avsc
   [
   {"namespace": "io.confluent.examples.clients.basicavro",
    "type": "record",
    "name": "Payment",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"}
    ]
   }
   ]

Let's break down what this schema defines

# `namespace`: a fully qualified name that avoids naming conflicts
# `type`: Avro data type, one of `record`, `enum, `union`, `array`, `map`, `fixed`
# `name`: unique schema name in this namespace
# `fields`: one or more simple or complex data types for a `record`
## the first field in this record is called `id`, and it is of type `string`.
## the second field in this record is called `amount`, and it is of type `double.


Client Applications
~~~~~~~~~~~~~~~~~~~

Generally speaking, Kafka applications using Avro data and |sr-long| need to specify two configuration parameters:

# Avro serializer or deserializer
# URL to the |sr-long|

Java Producers
^^^^^^^^^^^^^^

Java applications that have Kafka producers using Avro require `pom.xml` files to include:

# Avro dependencies to serialize data as Avro, including `org.apache.avro.avro` and `io.confluent.kafka-avro-serializer`
# Avro plugin `avro-maven-plugin` to generate Java class files from the source schema

For a full `pom.xml` example, please refer to this `pom.xml<https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/pom.xml>`__.

Within the application, Java producers that are serializing data as Avro set two main configurations parameters:

# Avro serializer for the Kafka value (or Kafka key)
# URL to the |sr-long|

Then the producer can send records where the Kafka value is of `Payment` class.
For example:

.. sourcecode:: java

   import io.confluent.kafka.serializers.KafkaAvroSerializer;
   import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

   ....
   props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
   props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
   ....

   ....
   KafkaProducer<String, Payment> producer = new KafkaProducer<String, Payment>(props));
   final Payment payment = new Payment(orderId, 1000.00d);
   final ProducerRecord<String, Payment> record = new ProducerRecord<String, Payment>("payments", payment.getId().toString(), payment);
   producer.send(record);
   ....

For a full Java producer example, please refer to `the producer example<https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/src/main/java/io/confluent/examples/clients/basicavro/ProducerExample.java>`__.


Java Consumers
^^^^^^^^^^^^^^

Java applications that have Kafka consumers using Avro require `pom.xml` files to include:

# Avro dependencies to serialize data as Avro, including `org.apache.avro.avro` and `io.confluent.kafka-avro-serializer`
# Avro plugin `avro-maven-plugin` to generate Java class files from the source schema

For a full `pom.xml` example, please refer to `sample pom.xml<https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/pom.xml>`__.

Within the application, Java consumers that are deserializing data as Avro set two main configurations parameters:

# Avro deserializer for the Kafka value (or Kafka key)
# URL to the |sr-long|

Then the consumer can read records where the Kafka value is of `Payment` class.
For example:

.. sourcecode:: java

   import io.confluent.kafka.serializers.KafkaAvroDeserializer;
   import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

   ....
   props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
   props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); 
   props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
   ....

   ....
   KafkaConsumer<String, Payment> consumer = new KafkaConsumer<>(props));
   consumer.subscribe(Collections.singletonList("payments"));
   while (true) {
     ConsumerRecords<String, Payment> records = consumer.poll(100);
     for (ConsumerRecord<String, Payment> record : records) {
       String key = record.key();
       Payment value = record.value();
     }
   }
   ....

For a full Java consumer example, please refer to `the consumer example<https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/src/main/java/io/confluent/examples/clients/basicavro/ConsumerExample.java>`__.


Other Kafka Clients
^^^^^^^^^^^^^^^^^^^

The objective of this tutorial is to learn about Avro and |sr| centralized schema management and compatibility checks.
To keep examples simple, we focus on Java producers and consumers, but other Kafka clients work in similar ways.
For configurations examples of other Kafka clients interoperating with Avro and |sr|:

* `KSQL<https://docs.confluent.io/current/ksql/docs/installation/server-config/avro-schema.html#configuring-avro-and-sr-for-ksql>`__
* `Kafka Streams<https://docs.confluent.io/current/streams/developer-guide/datatypes.html#avro>`__
* `Kafka Connect<https://docs.confluent.io/current/schema-registry/docs/connect.html#using-kafka-connect-with-sr>`__
* `Confluent REST Proxy<https://docs.confluent.io/current/kafka-rest/docs/api.html#post--topics-(string-topic_name)-partitions-(int-partition_id)>`__
* `Non-Java clients based on librdkafka including Confluent Python, Confluent Go, Confluent DotNet<https://docs.confluent.io/current/clients/index.html>`__


Schemas in Schema Registry
~~~~~~~~~~~~~~~~~~~~~~~~~~

By this point, you have producers serializing Avro data and consuemrs deserializing Avro data, and writing schemas to |sr-long|.
You can view subjects and associated schemas via the REST endpoint in |sr|.

First, view all the subjects registered in |sr| (assuming |sr| is running on the local machine listening on port 8081):

.. sourcecode:: bash

   $ curl --silent -X GET http://localhost:8081/subjects/ | jq .  
   [
     "transactions-value"
   ]

In our example, the Kafka topic `transaction` has messages whose value, i.e., payload, is Avro.
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

# `subject`: the scope in which schemas for the messages in the topic `transaction` can evolve
# `version`: the schema version for this subject, which starts at 1 for each subject
# `id`: the globally unique schema version id, unique across all schemas in all subjects
# `schema`: the structure that defines the schema format

Based on the schema id, you can also retrieve the associated schema in |sr|:

.. sourcecode:: bash

   $ curl --silent -X GET http://localhost:8081/schemas/ids/1 | jq .
   {
     "schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"
   }

The schema is identical to the :ref:`schema file defined for Java client applications<schema_registry_tutorial_definition>`.

If you were using KSQL and had registered the topic as shown earlier, you could `DESCRIBE` the schema of the stream from |c3|.

YEVA: insert screenshot


Schema Ids in Messages
~~~~~~~~~~~~~~~~~~~~~~

Integration with |sr-long| means that Kafka messages do not need to be written with the entire Avro schema.
Instead, Kafka messages are written with the schema _id_.
The producers writing the messages and the consumers reading the messages must be using the same |sr| to get the same understanding of mapping between a schema and schema id.

In this example, a producer sends the new schema for `Payments` to |sr|.
|sr| registers this schema `Payments` to the subject `transactions-value`, and returns the schema id of `1` to the producer.
The producer caches this schema to schema id mapping for subsequent message writes, so it only contacts |sr| on first schema write.
When a consumer reads this data, it sees the Avro schema id of `1` and sends a schema request to |sr|.
|sr| retrieves the schema associated to schema id `1`, and returns the schema to the consumer.
The consumer caches this schema to schema id mapping for subsequent message reads, so it only contacts |sr| on first schema id read.


Auto Schema Registration
~~~~~~~~~~~~~~~~~~~~~~~~

Additionally, by default, client applications automatically register new schemas.
If they produce new messages to a new topic, then they will automatically try to register new schemas.
This is very convenient in development environments.
In production, we recommend that client applications do not automatically register new schemas.
They can be done outside the client application to provide control over when schemas are registered with |sr-long| and how they evolve.

Within the application, disable automatic schema registration by setting the configuration parameter `auto.register.schemas=false`, as shown in the examples below.

.. sourcecode:: java

   props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

To manually register the schema outside of the application, send the schema to |sr| and associate it with a subject, in this case `transactions-value`.  It returns a schema id of `1`.

.. sourcecode:: bash

   $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"}' http://localhost:8081/subjects/transactions-value/versions
   {"id":1}


Schema Evolution and Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Up till now, you have seen the benefit of |sr-long| as being centralized schema management that enables client applications to register and retrieve globally unique schema ids.
The main value, however, is in enabling schema evolution.
Similar to how APIs evolve and need to be compatible for all applications that rely on old and new versions of the API, schemas also evolve and likewise need to be compatible for all applications that rely on old and new versions of the schema.
This schema evolution is a natural behavior of how applications and data develop over time.

|sr-long| embraces schema evolution and provides compatibility checks.
These compatibility checks ensure that the contract between producers and consumers are not broken, especially important in Kafka in which producers and consumers are decoupled.
Compatibility checks allow producers and consumers to update independently and evolve their schemas independently, with assurances that they can read new and legacy data.

The types of `compatibility<https://docs.confluent.io/current/avro.html#data-serialization-and-evolution>`__:

* `Forward`: consumers can still read data written by producers using newer schemas
* `Backward`: upgraded consumers can still read data written by producers using older schemas
* `Full`: forward and backward compatible
* `None`: compatibility checks disabled

By default, |sr| is configured for backward compatibility.
You can change this globally or per subject, but for the remainder of this tutorial, we will leave the default compatibility level to `backward`.

In our example of the Payment schema, let's say now some applications are sending additional information for each payment, e.g., a field `region` that represents the place of sale.
Consider the `first Payment schema<https://github.com/confluentinc/examples/blob/DEVX-380/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2a.avsc>`__:

.. sourcecode:: json

   $ cat src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2a.avsc
   [
   {"namespace": "io.confluent.examples.clients.basicavro",
    "type": "record",
    "name": "Payment",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "region", "type": "string"}
    ]
   }
   ]

Before proceeding, think about whether this schema is backward compatible.
Specifically, ask yourself whether a consumer can use this schema to read data written by producers using the older schema without the `region` field?

The answer is no.
Consumers will fail reading data with the older schema, because the older schema does not have the `region` field, so it is not backward compatible.
You can test this by trying to manually register the above schema.

.. sourcecode:: bash

   $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"region\",\"type\":\"string\"}]}"}' http://localhost:8081/subjects/transactions-value/versions
   {"error_code":409,"message":"Schema being registered is incompatible with an earlier schema"}

|sr| rejects the new schema registration, with an error message that it is incompatible.
Without |sr| checking compatibility, your applications would break.

To keep the producer-consumer contract, the new schema must assume default values for the new fields if they are not provided.
Therefore, there must be a default value for `region` to maintain backward compatibility.
Consider another `updated Payment schema<https://github.com/confluentinc/examples/blob/DEVX-380/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2b.avsc>`__:

.. sourcecode:: json

   $ cat src/main/resources/avro/io/confluent/examples/clients/basicavro/Payment2b.avsc
   [
   {"namespace": "io.confluent.examples.clients.basicavro",
    "type": "record",
    "name": "Payment",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "region", "type": "string", "default": ""}
    ]
   }
   ]

Now if you try to manually register this schema, it will succeed:

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

# `version`: changed from `1` to `2`
# `id`: changed from `1` to `2`
# `schema`: changed with the new field `region` with the default value


Next Steps
~~~~~~~~~~

# Adapt your applications to use Avro data
# Change compatibility modes to suit your application needs
# Evolve schemas so that they fail compatibility checks
# Evolve schemas so that they pass compatibility checks
