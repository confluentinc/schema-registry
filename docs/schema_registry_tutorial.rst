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

Kafka producers write data to Kafka topics that is later consumed by Kafka consumers.
There is an implicit contract that producers write data with a schema that can be read by consumers, even as producers and consumers evolve their schemas.
It is useful to think about schemas as APIs, and as such, schema evolution requires compatibility checks to ensure that contract is not broken. 
For a more in-depth understanding of the benefits of Avro and |sr|, please refer to the following resources:

* `Why Avro For Kafka Data<https://www.confluent.io/blog/avro-kafka-data/>`__
* `Yes, Virginia, You Really Do Need a Schema Registry<https://www.confluent.io/blog/schema-registry-kafka-stream-processing-yes-virginia-you-really-need-one/>`__

Target Audience
^^^^^^^^^^^^^^^

The target audience is a developer writing streaming applications and wants compatibility checks and centralized schema management provided by |sr|.

This tutorial is not meant to cover the operational aspects of running the |sr| service. For production deployments of |sr-long|, please refer to :ref:`Schema Registry Operations<schemaregistry_operations>`.

Prerequisites
^^^^^^^^^^^^^

Before proceeding with this tutorial

# Use the :ref:`quickstart` to bring up the |cp|.
# Clone the |cp| `examples` repo in GitHub.

.. sourcecode:: bash

   $ git clone https://github.com/confluentinc/examples.git

You should have installed on your local machine:

* Java 1.8 to run |cp|
* Maven to compile the client Java code

.. note::

   The focus of this tutorial is on Java-based clients, including producers and consumers, or embedded producers and consumers in Kafka Streams API, KSQL, or Kafka Connect.  Non-Java clients, including librdkafka, Confluent Python, Confluent Go, Confluent DotNet, also work with Avro and |sr|.  For those clients, please `Kafka Clients documentation<https://docs.confluent.io/current/clients/index.html>`__.

Schemas
~~~~~~~

.. _schema_registry_tutorial_definition:

Schema Definition
^^^^^^^^^^^^^^^^^

The first thing to do is to agree on a basic schema for your topic data.
Client applications are forming a contract that producers will write data in a compatible schema and consumers will be able to read that data.
Of course there applications can use many schemas, but for this tutorial, we will look at one.
Consider the following schema from https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/src/main/resources/avro/io/confluent/examples/clients/basicavro/payment.avsc.

.. sourcecode:: json

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


Schema vs Topic vs Subject
^^^^^^^^^^^^^^^^^^^^^^^^^^

It is important to understand the terminology, so let's define what is a `schema` versus a `topic` versus a `subject`.

A Kafka topic contains messages, and each message is a key-value pair.
Either the message key or the message value, or both, can be independently serialized as Avro.
The Kafka topic name is independent of the schema name.
When a producer writes a message to a Kafka topic, it can serialize the message key or message value as Avro.
By default, the subject that is registered in |sr| is derived from the Kafka topic name.

As a practical example, let's say a producer is writing data with a schema `Payment` to a Kafka topic called `Raleigh`.
If the producer is serializing the message value as Avro, |sr| has a subject called `Raleigh-value`.
If the producer is also serializing the message key as Avro, |sr| has a subject called `Raleigh-key`, but in this tutorial for simplicity we talk only about the message value.
The |sr| subject `Raleigh-value` has at least one schema called `Payment`.
The |sr| subject `Raleigh-value` defines the scope in which schemas for the topic Raleigh can evolve and |sr| does compatibility checking within this scope.
If developers evolve the schema `Payment`, the |sr| subject `Raleigh-value` will check that those newly evolved schemas are compatible and add those new schemas to the subject.


Writing Applications
~~~~~~~~~~~~~~~~~~~~

Generally speaking, Kafka applications using Avro data and |sr-long| need to specify two configuration parameters:

# Avro serializer or deserializer
# URL to the |sr-long|

The sections below show how to do it in various clients.

Java Producers
^^^^^^^^^^^^^^

Java applications that have Kafka producers using Avro require `pom.xml` files to include:

# Avro dependencies to serialize data as Avro, including `org.apache.avro.avro` and `io.confluent.kafka-avro-serializer`
# Avro plugin `avro-maven-plugin` to generate Java class files from the source schema

For a full `pom.xml` example, please refer to `sample pom.xml<https://github.com/confluentinc/examples/blob/5.0.0-post/clients/avro/pom.xml>`__.

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


Kafka Streams
^^^^^^^^^^^^^

Java applications using the Kafka Streams API with Avro require `pom.xml` files to include:

# Avro dependencies to serialize data as Avro, including `org.apache.avro.avro`, `io.confluent.kafka-avro-serializer`, and `io.confluent.kafka-streams-avro-serde`
# Avro plugin `avro-maven-plugin` to generate Java class files from the source schema

For a full `pom.xml` example, please refer to `sample pom.xml<https://github.com/confluentinc/examples/blob/5.0.0-post/connect-streams-pipeline/pom.xml>`__.

Within the application, the Kafka streams properties require to set two main configurations parameters:

# Avro serializer/deserializer for the Kafka value (or Kafka key)
# URL to the |sr-long|

Then the streams application can write and read records where the Kafka value is of `Payment` class.
For example:

.. sourcecode:: java

   import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
   import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

   ....
   props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
   props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
   ....

For a full Java Kafka Streams example, please refer to `the streams example<https://github.com/confluentinc/kafka-streams-examples/blob/5.0.x/src/main/java/io/confluent/examples/streams/WikipediaFeedAvroExample.java`__.

KSQL
^^^^

KSQL queries with Avro require setting the configuration parameter:

# URL to the |sr-long|

Then KSQL queries can write serialized Avro data to Kafka topics or read from Kafka topics with serialized Avro data.
For example:

.. sourcecode:: sql

   ksql> set 'ksql.schema.registry.url'='http://localhost:8081';
   ....
   ksql> CREATE STREAM raleighStream WITH (KAFKA_TOPIC='Raleigh', VALUE_FORMAT='AVRO');
   ksql> CREATE STREAM newRaleighStream WITH (VALUE_FORMAT='AVRO') AS SELECT * FROM raleighStream;

For a full KSQLexample, please refer to `the KSQL example<https://github.com/confluentinc/examples/blob/5.0.0-post/pageviews>`__.


Kafka Connect
^^^^^^^^^^^^^

Kafka Connect using Avro require its embedded producers and consumers to be configured as follows:

# Avro Converter for the Kafka value (or Kafka key)
# URL to the |sr-long|

This enables, by default, the source connectors to serialize Avro data and sink connectors to deserialize Avro data.
For example:

.. sourcecode:: bash

   value.converter=io.confluent.connect.avro.AvroConverter
   value.converter.schema.registry.url=http://localhost:8081
   value.converter.schemas.enable=true

For a full Kafka Connect, please refer to `the connect example<https://github.com/confluentinc/examples/blob/5.0.0-post/connect-streams-pipeline/jdbcspecificavro-connector.properties>`__.

Schemas and Schema Ids
~~~~~~~~~~~~~~~~~~~~~~

Schemas in Schema Registry
^^^^^^^^^^^^^^^^^^^^^^^^^^

By this point, you have producers serializing Avro data and consuemrs deserializing Avro data, and writing schemas to |sr-long|.
You can view subjects and associated schemas via the REST endpoint in |sr|.

First, view all the subjects registered in |sr| (assuming |sr| is running on the local machine listening on port 8081):

.. sourcecode:: bash

   $ curl --silent -X GET http://localhost:8081/subjects/ | jq .  
   [
     "Raleigh-value"
   ]

In our example, the Kafka topic `Raleigh` has messages whose value, i.e., payload, is Avro.
View the associated subject `Raleigh-value` in |sr|:

.. sourcecode:: bash

   $ curl --silent -X GET http://localhost:8081/subjects/Raleigh-value/versions/latest | jq .
   {
     "subject": "Raleigh-value",
     "version": 1,
     "id": 1,
     "schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"
   }

Let's break down what this version of the schema defines

# `subject`: the scope in which schemas for the messages in the topic `Raleigh` can evolve
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
^^^^^^^^^^^^^^^^^^^^^^

Integration with |sr-long| means that Kafka messages do not need to be written with the entire Avro schema.
Instead, Kafka messages are written with the schema _id_.
The producers writing the messages and the consumers reading the messages must be using the same |sr| to get the same understanding of mapping between a schema and schema id.

In this example, a producer sends the new schema for `Payments` to |sr|.
|sr| registers this schema `Payments` to the subject `Raleigh-value`, and returns the schema id of `1` to the producer.
The producer caches this schema to schema id mapping for subsequent message writes, so it only contacts |sr| on first schema write.
When a consumer reads this data, it sees the Avro schema id of `1` and sends a schema request to |sr|.
|sr| retrieves the schema associated to schema id `1`, and returns the schema to the consumer.
The consumer caches this schema to schema id mapping for subsequent message reads, so it only contacts |sr| on first schema id read.


Auto Schema Registration
^^^^^^^^^^^^^^^^^^^^^^^^

Additionally, by default, client applications automatically register new schemas.
If they produce new messages to a new topic, then they will automatically try to register new schemas.
This is very convenient in development environments.
In production, we recommend that client applications do not automatically register new schemas.
They can be done outside the client application to provide control over when schemas are registered with |sr-long| and how they evolve.

Within the application, disable automatic schema registration by setting the configuration parameter `auto.register.schemas=false`, as shown in the examples below.

.. sourcecode:: java

   props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

To manually register the schema outside of the application, send the schema to |sr| and associate it with a subject, in this case `Raleigh-value`.  It returns a schema id of `1`.

.. sourcecode:: bash

   $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"io.confluent.examples.clients.basicavro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"}' http://localhost:8081/subjects/Raleigh-value/versions
   {"id":1}


Schema Evolution and Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
