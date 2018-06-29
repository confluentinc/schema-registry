.. _serializer_and_formatter:

Serializer and Formatter
========================

In this document, we describe how to use Avro with the Kafka Java client and console tools.


Assuming that you have the |sr| source code checked out at ``/tmp/schema-registry``, the
following is how you can obtain all needed jars.

.. sourcecode:: bash

   mvn package

The jars can be found in

.. sourcecode:: bash

   /tmp/schema-registrypackage/target/package-$VERSION-package/share/java/avro-serializer/

Serializer
----------

You can plug ``KafkaAvroSerializer`` into KafkaProducer to send messages of Avro type to Kafka.
Currently, we support primitive types of ``null``, ``Boolean``, ``Integer``,
``Long``, ``Float``,
``Double``, ``String``,
``byte[]``, and complex type of ``IndexedRecord``. Sending data of other types
to ``KafkaAvroSerializer`` will
cause a ``SerializationException``. Typically, ``IndexedRecord`` will be used for the value of the Kafka
message. If used, the key of the Kafka message is often of one of the primitive types. When sending
a message to a topic *t*, the Avro schema for the key and the value will be automatically registered
in the |sr| under the subject *t-key* and *t-value*, respectively, if the compatibility
test passes. The only exception is that the ``null`` type is never registered in the |sr|.

In the following example, we send a message with key of type string and value of type Avro record
to Kafka. A ``SerializationException`` may occur during the send call, if the data is not well formed.

.. sourcecode:: bash

    import org.apache.avro.Schema;
    import org.apache.avro.generic.GenericData;
    import org.apache.avro.generic.GenericRecord;
    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerConfig;
    import org.apache.kafka.clients.producer.ProducerRecord;
    import java.util.Properties;

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put("schema.registry.url", "http://localhost:8081");
    KafkaProducer producer = new KafkaProducer(props);

    String key = "key1";
    String userSchema = "{\"type\":\"record\"," +
                        "\"name\":\"myrecord\"," +
                        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("f1", "value1");

    ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", key, avroRecord);
    try {
      producer.send(record);
    } catch(SerializationException e) {
      // may need to do something with it
    }
    // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
    // then close the producer to free its resources.
    finally {
      producer.flush();
      producer.close();
    }

You can plug in ``KafkaAvroDeserializer`` to ``KafkaConsumer`` to receive messages of any Avro type from Kafka.
In the following example, we receive messages with key of type ``string`` and value of type Avro record
from Kafka. When getting the message key or value, a ``SerializationException`` may occur if the data is
not well formed.

.. sourcecode:: bash

    import org.apache.kafka.clients.consumer.Consumer;
    import org.apache.kafka.clients.consumer.ConsumerRecord;
    import org.apache.kafka.clients.consumer.ConsumerRecords;
    import org.apache.kafka.clients.consumer.KafkaConsumer;
    import org.apache.kafka.clients.consumer.ConsumerConfig;

    import org.apache.avro.generic.GenericRecord;

    import java.io.FileInputStream;
    import java.io.IOException;
    import java.io.InputStream;
    import java.nio.file.Files;
    import java.nio.file.Paths;
    import java.util.Arrays;
    import java.util.Properties;
    import java.util.Random;

    Properties props = new Properties();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");


    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("schema.registry.url", "http://localhost:8081");

    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    String topic = "topic1";
    final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Arrays.asList(topic));

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
        }
      }
    } finally {
      consumer.close();
    }


Subject Name Strategy
^^^^^^^^^^^^^^^^^^^^^

KafkaAvroSerializer and KafkaAvroDeserializer default to using *<topicName>-Key*
and *<topicName>-value* as the corresponding subject name while registering or retrieving the
schema.

This behavior can be modified by using the following configs

``key.subject.name.strategy``
  Determines how to construct the subject name under which the key schema is registered with the
  schema registry.

  Any implementation of ``io.confluent.kafka.serializers.subject.SubjectNameStrategy`` can be specified. By default, <topic>-key is used as subject.

  * Type: class
  * Default: class io.confluent.kafka.serializers.subject.TopicNameStrategy
  * Importance: medium

``value.subject.name.strategy``
  Determines how to construct the subject name under which the value schema is registered with the schema registry.

  Any implementation of ``io.confluent.kafka.serializers.subject.SubjectNameStrategy`` can be specified. By default, <topic>-value is used as subject.

  * Type: class
  * Default: class io.confluent.kafka.serializers.subject.TopicNameStrategy
  * Importance: medium

The other available options that can be configured out of the box include

``io.confluent.kafka.serializers.subject.RecordNameStrategy``

 For any Avro record type that is published to Kafka, registers the schema
 in the registry under the fully-qualified record name (regardless of the
 topic). This strategy allows a topic to contain a mixture of different
 record types, since no intra-topic compatibility checking is performed.
 Instead, checks compatibility of any occurrences of the same record name
 across **all** topics.

``io.confluent.kafka.serializers.subject.TopicRecordNameStrategy``

 For any Avro record type that is published to Kafka topic <topicName>,
 registers the schema in the registry under the subject name
 <topicName>-<recordName>, where <recordName> is the
 fully-qualified Avro record name. This strategy allows a topic to contain
 a mixture of different record types, since no intra-topic compatibility
 checking is performed. Moreover, different topics may contain mutually
 incompatible versions of the same record name, since the compatibility
 check is scoped to a particular record name within a particular topic.

Basic Auth Security
^^^^^^^^^^^^^^^^^^^

Schema Registry supports ability to authenticate requests using Basic Auth headers. You can send
the Basic Auth headers by setting the following configuration in your producer or consumer example

``basic.auth.credentials.source``
  Specify how to pick the credentials for Basic Auth header. The supported values are URL,
  USER_INFO and SASL_INHERIT

  * Type: string
  * Default: "URL"
  * Importance: medium


**URL** - The user info is configured as part of the ``schema.registry.url`` config in the
form of ``http://<username>:<password>@sr-host:<sr-port>``

**USER_INFO** - The user info is configured using the below configuration.
``basic.auth.user.info``
  Specify the user info for Basic Auth in the form of {username}:{password}

  * Type: password
  * Default: ""
  * Importance: medium

**SASL_INHERIT** - Inherit the settings used by the Kafka client to communicate with the broker
using SASL SCRAM or SASL PLAIN.

Formatter
---------

You can use ``kafka-avro-console-producer`` and ``kafka-avro-console-consumer`` respectively to send and
receive Avro data in JSON format from the console. Under the hood, they use ``AvroMessageReader`` and
``AvroMessageFormatter`` to convert between Avro and JSON.

To run the Kafka console tools, first make sure that |zk|, Kafka and the |sr| server
are all started. In the following examples, we use the default value of the |sr| URL.

You can configure that by supplying

.. sourcecode:: bash

   --property schema.registry.url=address of your |sr|

in the commandline arguments of ``kafka-avro-console-producer`` and ``kafka-avro-console-consumer``.

In the following example, we send Avro records in JSON as the message value (make sure there is no space in the schema string).

.. sourcecode:: bash

   bin/kafka-avro-console-producer --broker-list localhost:9092 --topic t1 \
     --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

   In the shell, type in the following.
     {"f1": "value1"}


In the following example, we read the value of the messages in JSON.

.. sourcecode:: bash

   bin/kafka-avro-console-consumer --topic t1 \
     --zookeeper localhost:2181

   You should see following in the console.
     {"f1": "value1"}


In the following example, we send strings and Avro records in JSON as the key and the value of the
message, respectively.

.. sourcecode:: bash

   bin/kafka-avro-console-producer --broker-list localhost:9092 --topic t2 \
     --property parse.key=true \
     --property key.schema='{"type":"string"}' \
     --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

   In the shell, type in the following.
     "key1" \t {"f1": "value1"}

In the following example, we read both the key and the value of the messages in JSON,

.. sourcecode:: bash

   bin/kafka-avro-console-consumer --topic t2 \
     --zookeeper localhost:2181 \
     --property print.key=true

   You should see following in the console.
      "key1" \t {"f1": "value1"}


If the topic contains a  key in a format other than avro, you can specify your own key
deserializer

.. sourcecode:: bash

   bin/kafka-avro-console-consumer --topic t2 \
     --zookeeper localhost:2181 \
     --property print.key=true
     --key.deserializer=org.apache.kafka.common.serialization.StringDeserializer


Wire Format
-----------

Most users can use the serializers and formatter directly and never worry about the details of how Avro messages are mapped
to bytes. However, if you're working with a language that Confluent has not developed serializers for, or simply want a deeper
understanding of how the Confluent Platform works, you may need more detail on how data is mapped to low-level bytes.

The wire format currently has only a couple of components:

=====  ========== ===========
Bytes  Area       Description
=====  ========== ===========
0      Magic Byte Confluent serialization format version number; currently always ``0``.
1-4    Schema ID  4-byte schema ID as returned by the |sr|
5-...  Data       Avro serialized data in `Avro's binary encoding
                  <https://avro.apache.org/docs/1.8.1/spec.html#binary_encoding>`_. The only exception is raw bytes, which
                  will be written directly without any special Avro encoding.
=====  ========== ===========

Note that all components are encoded with big-endian ordering, i.e. standard network byte order.

Compatibility Guarantees
^^^^^^^^^^^^^^^^^^^^^^^^

The serialization format used by Confluent Platform serializers is guaranteed to be stable over major releases without any
changes without advanced warning. This is critical because the serialization format affects how keys are mapped across
partitions. Because many applications depend on keys with the same *logical* format being routed to the same physical
partition, it is usually important that the physical *byte* format of serialized data does not change unexpectedly for an
application. Even the smallest modification can result in records with the same *logical key* being routed to different
partitions because messages are routed to partitions based on the hash of the key.

In order to ensure there is no variation even as the serializers are updated with new formats, the serializers are very
conservative when updating output formats. To ensure stability for clients, Confluent Platform and its serializers ensure the
following:

* The format (including magic byte) will not change without significant warning over multiple Confluent Platform **major
  releases**. Although the default may eventually be changed infrequently to allow adoption of new features by default, this
  will be done *very* conservatively and with at least one major release between changes, during which the relevant changes
  will result in user-facing warnings so no users will be caught off guard by the need for transition. Very significant,
  compatibility-affecting changes will guarantee at least 1 major release of warning and 2 major releases before an
  incompatible change will be made.
* Within the version specified by the magic byte, the format will never change in any backwards-incompatible way. Any changes
  made will be fully backward compatible with documentation in release notes and at least one version of warning will be
  provided if it introduces a new serialization feature which requires additional downstream support.
* Deserialization will be supported over multiple major releases. This does not guarantee indefinite support, but support for
  deserializing any earlier formats will be supported indefinitely as long as there is no notified reason for
  incompatibility.

For more information about compatibility or support, reach out to the `community mailing list
<https://groups.google.com/forum/#!forum/confluent-platform>`_.
