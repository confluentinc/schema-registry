.. _serializer_and_formatter:

Serializer and Formatter
========================

In this document, we describe how to use Avro with the Kafka Java client and console tools.


Assuming that you have the Schema Registry source code checked out at ``/tmp/schema-registry``, the
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
in the schema registry under the subject *t-key* and *t-value*, respectively, if the compatibility
test passes. The only exception is that the ``null`` type is never registered in the schema registry.

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

You can plug in ``KafkaAvroDecoder`` to ``KafkaConsumer`` to receive messages of any Avro type from Kafka.
In the following example, we receive messages with key of type ``string`` and value of type Avro record
from Kafka. When getting the message key or value, a ``SerializationException`` may occur if the data is
not well formed.

.. sourcecode:: bash

    import org.apache.avro.generic.IndexedRecord;
    import kafka.consumer.ConsumerConfig;
    import kafka.consumer.ConsumerIterator;
    import kafka.consumer.KafkaStream;
    import kafka.javaapi.consumer.ConsumerConnector;
    import io.confluent.kafka.serializers.KafkaAvroDecoder;
    import kafka.message.MessageAndMetadata;
    import kafka.utils.VerifiableProperties;
    import org.apache.kafka.common.errors.SerializationException;
    import java.util.*;

    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "group1");
    props.put("schema.registry.url", "http://localhost:8081");

    String topic = "topic1";
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, new Integer(1));

    VerifiableProperties vProps = new VerifiableProperties(props);
    KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
    KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);

    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

    Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumer.createMessageStreams(
        topicCountMap, keyDecoder, valueDecoder);
    KafkaStream stream = consumerMap.get(topic).get(0);
    ConsumerIterator it = stream.iterator();
    while (it.hasNext()) {
      MessageAndMetadata messageAndMetadata = it.next();
      try {
        String key = (String) messageAndMetadata.key();
        IndexedRecord value = (IndexedRecord) messageAndMetadata.message();

        ...
      } catch(SerializationException e) {
        // may need to do something with it
      }
    }

We recommend users use the new producer in ``org.apache.kafka.clients.producer.KafkaProducer``. If
you are using a version of Kafka older than 0.8.2.0, you can plug ``KafkaAvroEncoder`` into the old
producer in ``kafka.javaapi.producer``. However, there will be some limitations. You can only use
``KafkaAvroEncoder`` for serializing the value of the message and only send value of type Avro record.
The Avro schema for the value will be registered under the subject *recordName-value*, where
*recordName* is the name of the Avro record. Because of this, the same Avro record type shouldn't
be used in more than one topic.

Formatter
---------

You can use ``kafka-avro-console-producer`` and ``kafka-avro-console-consumer`` respectively to send and
receive Avro data in JSON format from the console. Under the hood, they use ``AvroMessageReader`` and
``AvroMessageFormatter`` to convert between Avro and JSON.

To run the Kafka console tools, first make sure that Zookeeper, Kafka and the Schema Registry server
are all started. In the following examples, we use the default value of the schema registry URL.
You can configure that by supplying

.. sourcecode:: bash

   --property schema.registry.url=address of your schema registry

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
