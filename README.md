schema-registry
===============
Schema registry for Kafka

Quickstart
----------

1. Start ZooKeeper from the standard Kafka install
./bin/zookeeper-server-start.sh config/zookeeper.properties

2. Start Kafka from the standard Kafka install
./bin/kafka-server-start.sh config/server.properties

3. Start the REST server by running io.confluent.kafka.schemaregistry.rest.Main:
mvn package
bin/schema-registry-start config/schema-registry.properties

    # Register a new version of a schema under the subject "Kafka-key"
    $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
        http://localhost:8081/subjects/Kafka-key/versions

    # Register a new version of a schema under the subject "Kafka-value"
    $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
         http://localhost:8081/subjects/Kafka-value/versions

    # List all subjects
    $ curl -X GET -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        http://localhost:8081/subjects

    # List all schema versions registered under the subject "Kafka-value"
    $ curl -X GET -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        http://localhost:8081/subjects/Kafka-value/versions

    # Fetch a schema by globally unique id 1
    $ curl -X GET -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        http://localhost:8081/schemas/ids/1

    # Fetch version 3 of the schema registered under subject "Kafka-value"
    $ curl -X GET -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        http://localhost:8081/subjects/Kafka-value/versions/3

    # Fetch the most recently registered schema under subject "Kafka-value"
    $ curl -X GET -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        http://localhost:8081/subjects/Kafka-value/versions/latest

    # Check whether a schema has been registered under subject "Kafka-key"
    $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
        http://localhost:8081/subjects/Kafka-key

    # Test compatibility of a schema with the latest schema under subject "Kafka-value"
    $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
        http://localhost:8081/compatibility/subjects/Kafka-value/versions/latest

    # Get top level config
    $ curl -X GET -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        http://localhost:8081/config

    # Update compatibility requirements globally
    $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"compatibility": "NONE"}' \
        http://localhost:8081/config

    # Update compatibility requirements under the subject "Kafka-value"
    $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"compatibility": "BACKWARD"}' \
        http://localhost:8081/config
