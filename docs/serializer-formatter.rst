Serializer and Formatter
========================

In this document, we describe how to use Avro in Kafka java client and Kafka console tools.

Serializer
----------

You can plug KafkaAvroSerializer into KafkaProducer to send messages of any Avro type (both
primitive and complex) to Kafka. The following are the java types supported by Avro: null, Boolean,
Integer, Long, Float, Double, String, byte[], and IndexedRecord. Sending data of other types to
KafkaAvroSerializer will cause a SerializationException. When sending a message to a topic *t*,
the Avro schema for the key and the value will be automatically registered in the Schema Registry
server under the subject *t-key* and *t-value*, respectively, if the compatibility test passes.

In the following example, we send a message with key of type string and value of type Avro record
to Kafka.

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
    props.put("schema.registry.url", "http://localhost:8080");
    KafkaProducer producer = new KafkaProducer(props);

    String key = "key1";
    String userSchema = "{\"type\":\"record\"," +
                        "\"name\":\"myrecord\"," +
                        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("f1", "value1");

    record = new ProducerRecord<Object, Object>("topic1", key, avroRecord);
    producer.send(record);

You can plug in KafkaAvroDecoder to KafkaConsumer to receive messages of any Avro type from Kafka.
In the following example, we receive messages with key of type string and value of type Avro record
from Kafka.

.. sourcecode:: bash

    import org.apache.avro.generic.IndexedRecord;
    import kafka.consumer.ConsumerConfig;
    import kafka.consumer.ConsumerIterator;
    import kafka.consumer.KafkaStream;
    import kafka.javaapi.consumer.ConsumerConnector;
    import java.util.Properties;

    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "group1");
    props.put("schema.registry.url", "http://localhost:8080");

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));

    VerifiableProperties vProps = new VerifiableProperties(props);
    KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
    KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);

    kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(vProps));

    Map<String, List<KafkaStream>> consumerMap = consumer.createMessageStreams(
        topicCountMap, keyDecoder, valueDecoder);
    KafkaStream stream = consumerMap.get(topic).get(0);
    ConsumerIterator it = stream.iterator();
    while (it.hasNext()) {
      MessageAndMetadata messageAndMetadata = it.next();
      String key = (String) messageAndMetadata.key();
      String value = (IndexedRecord) messageAndMetadata.message();

      ...
    }

We recommend users use the new producer in org.apache.kafka.clients.producer.KafkaProducer. If
you are using a version of Kafka older than 0.8.2.0, you can plug KafkaAvroEncoder into the old
producer in kafka.javaapi.producer. However, there will be some limitations. You can only use
KafkaAvroEncoder for serializing the value of the message and only send value of type Avro record.
The Avro schema for the value will be registered under the subject *recordName-value*, where
*recordName* is the name of the Avro record.

In the following example, we send a message with key of type string and value of type Avro record
to Kafka. Note that unlike the example in the new producer, we use a StringEncoder for serializing
the key and therefore there is no schema registration for the key.

.. sourcecode:: bash

    import kafka.javaapi.producer.Producer;
    import kafka.producer.KeyedMessage;
    import kafka.producer.ProducerConfig;
    import kafka.utils.VerifiableProperties;
    import org.apache.avro.Schema;
    import org.apache.avro.generic.GenericData;
    import org.apache.avro.generic.GenericRecord;
    import java.util.Properties;

    Properties props = new Properties();
    props.put("serializer.class", "io.confluent.kafka.serializers.KafkaAvroEncoder");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", brokerList);
    props.put("schema.registry.url", "http://localhost:8080");

    Producer producer = new Producer<String, Object>(new ProducerConfig(props));
    String key = "key1";
    String userSchema = "{\"type\":\"record\"," +
                        "\"name\":\"myrecord\"," +
                        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("f1", "value1");

    KeyedMessage<String, Object> message = new KeyedMessage<String, Object>(topic, key, avroRecord);
    producer.send(message);

Formatter
---------

You can plug AvroMessageReader and AvroMessageFormatter into kafka-console-producer and
kafka-console-consumer respectively to send and receive Avro data in json format from the console.

To run the Kafka console tools, first make sure that Zookeeper, Kafka and Schema Registry server
are all started. Second, make sure the jars for AvroMessageReader and AvroMessageFormatter are
included in the classpath of kafka-console-producer.sh and kafka-console-consumer.sh.

Assuming that you have the Schema Registry source code checked out at /tmp/schema-registry, the
following is how you can obtain all needed jars.

.. sourcecode:: bash

   mvn package
   The jars can be found at /tmp/schema-registrypackage/target/package-0.1-SNAPSHOT-package/share/java/avro-serializer/

In the following example, we send Avro records in json as the message value (make sure there is no space in the schema string).

.. sourcecode:: bash

   CLASSPATH=/tmp/schema-registry/package/target/package-0.1-SNAPSHOT-package/share/java/avro-serializer/* \
   bin/kafka-console-producer.sh --broker-list localhost:9092 --topic t1 \
     --line-reader io.confluent.kafka.formatter.AvroMessageReader \
     --property schema.registry.url=http://localhost:8080 \
     --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

   In the shell, type in the following.
     {"f1": "value1"}

In the following example, we read the value of the messages in json.

.. sourcecode:: bash

   CLASSPATH=/tmp/schema-registry/package/target/package-0.1-SNAPSHOT-package/share/java/avro-serializer/* \
   bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t1 \
     --zookeeper localhost:2181 --formatter io.confluent.kafka.formatter.AvroMessageFormatter \
     --property schema.registry.url=http://localhost:8080

   You should see following in the console.
     {"f1": "value1"}


In the following example, we send strings and Avro records in json as the key and the value of the
message, respectively.

.. sourcecode:: bash

   CLASSPATH=/tmp/schema-registry/package/target/package-0.1-SNAPSHOT-package/share/java/avro-serializer/* \
   bin/kafka-console-producer.sh --broker-list localhost:9092 --topic t2 \
     --line-reader io.confluent.kafka.formatter.AvroMessageReader \
     --property schema.registry.url=http://localhost:8080 \
     --property parse.key=true \
     --property key.schema='{"type":"string"}' \
     --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

   In the shell, type in the following.
     "key1" \t {"f1": "value1"}

In the following example, we read both the key and the value of the messages in JSON,

.. sourcecode:: bash

   CLASSPATH=/tmp/schema-registry/package/target/package-0.1-SNAPSHOT-package/share/java/avro-serializer/* \
   bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --topic t2 \
     --zookeeper localhost:2181 --formatter io.confluent.kafka.formatter.AvroMessageFormatter \
     --property schema.registry.url=http://localhost:8080 \
     --property print.key=true

   You should see following in the console.
      "key1" \t {"f1": "value1"}
