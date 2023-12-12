Schema Registry
================

Confluent Schema Registry provides a serving layer for your metadata. It provides a RESTful interface for storing and retrieving your Avro®, JSON Schema, and Protobuf schemas. It stores a versioned history of all schemas based on a specified subject name strategy, provides multiple compatibility settings and allows evolution of schemas according to the configured compatibility settings and expanded support for these schema types. It provides serializers that plug into Apache Kafka® clients that handle schema storage and retrieval for Kafka messages that are sent in any of the supported formats.

This README includes the following sections:

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!-- generated with [DocToc](https://github.com/thlorenz/doctoc), see the link for install and instructions for use -->

- [Documentation](#documentation)
- [Quickstart API Usage examples](#quickstart-api-usage-examples)
- [Installation](#installation)
- [Deployment](#deployment)
- [Development](#development)
- [OpenAPI Spec](#openapi-spec)
- [Contribute](#contribute)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

Documentation
-------------

Here are a few links to Schema Registry pages in the Confluent Documentation.

- [Installing and Configuring Schema Registry](https://docs.confluent.io/current/schema-registry/installation/index.html)
- [Schema Management overview](https://docs.confluent.io/current/schema-registry/index.html)
- [Schema Registry Tutorial](https://docs.confluent.io/current/schema-registry/schema_registry_tutorial.html)
- [Schema Registry API reference](https://docs.confluent.io/current/schema-registry/develop/api.html)
- [Serializers, Deserializers for supported schema types](https://docs.confluent.io/current/schema-registry/serializer-formatter.html)
- [Kafka Clients](https://docs.confluent.io/current/clients/index.html#kafka-clients)
- [Schema Registry on Confluent Cloud](https://docs.confluent.io/current/quickstart/cloud-quickstart/schema-registry.html)

Quickstart API Usage examples
-----------------------------

The following assumes you have Kafka and an [instance of the Schema Registry](https://docs.confluent.io/current/schema-registry/installation/index.html)
running using the default settings. These examples, and more, are also available at [API Usage examples](https://docs.confluent.io/current/schema-registry/using.html) on [docs.confluent.io](https://docs.confluent.io/current/).

```bash
# Register a new version of a schema under the subject "Kafka-key"
$ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schema": "{\"type\": \"string\"}"}' \
    http://localhost:8081/subjects/Kafka-key/versions
  {"id":1}

# Register a new version of a schema under the subject "Kafka-value"
$ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schema": "{\"type\": \"string\"}"}' \
     http://localhost:8081/subjects/Kafka-value/versions
  {"id":1}

# List all subjects
$ curl -X GET http://localhost:8081/subjects
  ["Kafka-value","Kafka-key"]

# List all schema versions registered under the subject "Kafka-value"
$ curl -X GET http://localhost:8081/subjects/Kafka-value/versions
  [1]

# Fetch a schema by globally unique id 1
$ curl -X GET http://localhost:8081/schemas/ids/1
  {"schema":"\"string\""}

# Fetch version 1 of the schema registered under subject "Kafka-value"
$ curl -X GET http://localhost:8081/subjects/Kafka-value/versions/1
  {"subject":"Kafka-value","version":1,"id":1,"schema":"\"string\""}

# Fetch the most recently registered schema under subject "Kafka-value"
$ curl -X GET http://localhost:8081/subjects/Kafka-value/versions/latest
  {"subject":"Kafka-value","version":1,"id":1,"schema":"\"string\""}

# Delete version 3 of the schema registered under subject "Kafka-value"
$ curl -X DELETE http://localhost:8081/subjects/Kafka-value/versions/3
  3

# Delete all versions of the schema registered under subject "Kafka-value"
$ curl -X DELETE http://localhost:8081/subjects/Kafka-value
  [1, 2, 3, 4, 5]

# Check whether a schema has been registered under subject "Kafka-key"
$ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schema": "{\"type\": \"string\"}"}' \
    http://localhost:8081/subjects/Kafka-key
  {"subject":"Kafka-key","version":1,"id":1,"schema":"\"string\""}

# Test compatibility of a schema with the latest schema under subject "Kafka-value"
$ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schema": "{\"type\": \"string\"}"}' \
    http://localhost:8081/compatibility/subjects/Kafka-value/versions/latest
  {"is_compatible":true}

# Get top level config
$ curl -X GET http://localhost:8081/config
  {"compatibilityLevel":"BACKWARD"}

# Update compatibility requirements globally
$ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "NONE"}' \
    http://localhost:8081/config
  {"compatibility":"NONE"}

# Update compatibility requirements under the subject "Kafka-value"
$ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "BACKWARD"}' \
    http://localhost:8081/config/Kafka-value
  {"compatibility":"BACKWARD"}
```

Installation
------------

You can download prebuilt versions of the schema registry as part of the
[Confluent Platform](http://confluent.io/downloads/). To install from source,
follow the instructions in the Development section.

Deployment
----------

The REST interface to schema registry includes a built-in Jetty server. The
wrapper scripts ``bin/schema-registry-start`` and ``bin/schema-registry-stop``
are the recommended method of starting and stopping the service.

Development
-----------

To build a development version, you may need a development versions of
[common](https://github.com/confluentinc/common) and
[rest-utils](https://github.com/confluentinc/rest-utils).  After
installing these, you can build the Schema Registry
with Maven.

This project uses the [Google Java code style](https://google.github.io/styleguide/javaguide.html)
to keep code clean and consistent.

To build:

```bash
mvn compile
```

To run the unit and integration tests:

```bash
mvn test
```

To run an instance of Schema Registry against a local Kafka cluster (using the default configuration included with Kafka):

```bash
mvn exec:java -pl :kafka-schema-registry -Dexec.args="config/schema-registry.properties"
```

To create a packaged version, optionally skipping the tests:

```bash
mvn package [-DskipTests]
```

It produces:

- Schema registry in `package-schema-registry/target/kafka-schema-registry-package-$VERSION-package`
- Serde tools for avro/json/protobuf in `package-kafka-serde-tools/target/kafka-serde-tools-package-$VERSION-package`

Each of the produced contains a directory layout similar to the packaged binary versions.

You can also produce a standalone fat JAR of schema registry using the `standalone` profile:

```bash
mvn package -P standalone [-DskipTests]
```

This generates `package-schema-registry/target/kafka-schema-registry-package-$VERSION-standalone.jar`, which includes all the dependencies as well.

OpenAPI Spec
------------

OpenAPI (formerly known as Swagger) specifications are built automatically using `swagger-maven-plugin`
on `compile` phase.


Contribute
----------

Thanks for helping us to make Schema Registry even better!

- Source Code: https://github.com/confluentinc/schema-registry
- Issue Tracker: https://github.com/confluentinc/schema-registry/issues

License
-------

The project is licensed under the [Confluent Community License](LICENSE-ConfluentCommunity), except for the `client` and `avro-*` libs,
which are under the [Apache 2.0 license](LICENSE-Apache).
See LICENSE file in each subfolder for detailed license agreement.
