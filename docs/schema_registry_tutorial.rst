.. _schema_registry_tutorial:

Confluent Schema Registry Tutorial
==================================

This tutorial provides an example on how to use |sr-long|.

**Contents**

.. contents::
  :local:
  :depth: 1


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

A Kafka topic contains messages, and each message is a key-value pair.
Either the message key or the message value, or both, can be independently serialized as Avro.
The Kafka topic name is independent of the schema name.
When a producer writes a message to a Kafka topic, it can serialize the message key or message value as Avro.
What gets registered in |sr| is called a subject derived from the Kafka topic name.

As a practical example, let's say a producer is writing data to a Kafka topic called `Raleigh`.
If the producer is serializing the message key as Avro, |sr| has a subject called `Raleigh-key`.
If the producer is serializing the message value as Avro, |sr| has a subject called `Raleigh-value`, but in this tutorial, we will consider where only the message value is serialized as Avro.
The |sr| subject `Raleigh-value` essentially defines a "namespace", which is different from the namespace in the schema.
This subject `Raleigh-value` is the scope in which schemas can evolve and |sr| does compatibility checking.

