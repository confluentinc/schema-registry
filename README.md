schema-registry
===============

Schema registry for Kafka

Quickstart
----------

1. Start ZooKeeper from the standard Kafka install
./bin/zookeeper-server.sh config/zookeeper.properties

2. Start Kafka from the standard Kafka install
./bin/kafka-server.sh config/server.properties

3. Start the REST server by running io.confluent.kafka.schemaregistry.rest.Main

4. Register a schema
curl -v -H "Content-Type: application/vnd.schemaregistry.v1+json" -X POST -i http://localhost:8080/topics/Kafka/value/versions -d '
 {"schema":"Hello World"}'

5. List all topics
curl -v -H "Content-Type: application/vnd.schemaregistry.v1+json" -X GET http://localhost:8080/topics

6. List all versions of a topic's schema
curl -v -H "Content-Type: application/vnd.schemaregistry.v1+json" -X GET http://localhost:8080/topics/Kafka/value/versions

7. Get a particular version of a topic's schema
curl -v -H "Content-Type: application/vnd.schemaregistry.v1+json" -X GET http://localhost:8080/Kafka/value/versions/1

