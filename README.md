Schema Registry
================

Schema Registry provides a serving layer for your metadata. It provides a
RESTful interface for storing and retrieving Avro schemas. It stores a versioned
history of all schemas, provides multiple compatibility settings and allows
evolution of schemas according to the configured compatibility setting. It
provides serializers that plug into Kafka clients that handle schema storage and
retrieval for Kafka messages that are sent in the Avro format.

Quickstart
----------

The following assumes you have Kafka and an instance of the Schema Registry running using the default settings.

    # Register a new version of a schema under the subject "Kafka-key"
    $ curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
        http://localhost:8081/subjects/Kafka-key/versions

    # Register a new version of a schema under the subject "Kafka-value"
    $ curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
         http://localhost:8081/subjects/Kafka-value/versions

    # List all subjects
    $ curl -X GET -i http://localhost:8081/subjects

    # List all schema versions registered under the subject "Kafka-value"
    $ curl -X GET -i http://localhost:8081/subjects/Kafka-value/versions

    # Fetch a schema by globally unique id 1
    $ curl -X GET -i http://localhost:8081/schemas/ids/1

    # Fetch version 1 of the schema registered under subject "Kafka-value"
    $ curl -X GET -i http://localhost:8081/subjects/Kafka-value/versions/1

    # Fetch the most recently registered schema under subject "Kafka-value"
    $ curl -X GET -i http://localhost:8081/subjects/Kafka-value/versions/latest

    # Check whether a schema has been registered under subject "Kafka-key"
    $ curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
        http://localhost:8081/subjects/Kafka-key

    # Test compatibility of a schema with the latest schema under subject "Kafka-value"
    $ curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"schema": "{\"type\": \"string\"}"}' \
        http://localhost:8081/compatibility/subjects/Kafka-value/versions/latest

    # Get top level config
    $ curl -X GET -i http://localhost:8081/config

    # Update compatibility requirements globally
    $ curl -X PUT -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"compatibility": "NONE"}' \
        http://localhost:8081/config

    # Update compatibility requirements under the subject "Kafka-value"
    $ curl -X PUT -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data '{"compatibility": "BACKWARD"}' \
        http://localhost:8081/config/Kafka-value

Installation
------------

You can download prebuilt versions of the Kafka REST Proxy as part of the
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
[common](https://github.com/confluentinc/common>) and
[rest-utils](https://github.com/confluentinc/rest-utils>).  After
installing these, you can build the Schema Registry
with Maven.

This project uses the [Google java code style](https://google-styleguide.googlecode.com/svn/trunk/javaguide.html)
to keep code clean and consistant.

Maven Plugin
-----------

## schema-registry:download

This plugin is used to download AVRO schemas for the requested subjects.

|        Name        |                                                      Description                                                      |  Type  | Required | Default |
|:------------------:|:---------------------------------------------------------------------------------------------------------------------:|:------:|:--------:|:-------:|
| schemaRegistryUrls | Urls for the schema registry instance to connect to                                                                   |  List  |    yes   |         |
| outputDirectory    | Output directory to write the schemas to.                                                                             | String |    yes   |         |
| schemaExtension    | The file extension to use for the output file name. This must begin with a '.' character.                             | String |    no    |  .avsc  |
| subjectPatterns    | The subject patterns to download. This is a list of regular expressions. Patterns must match the entire subject name. | List   |    no    |   ^.+$  |

```
<plugin>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-maven-plugin</artifactId>
    <version>3.1.0-SNAPSHOT</version>
    <configuration>
        <schemaRegistryUrls>
            <param>http://192.168.99.100:8081</param>
        </schemaRegistryUrls>
        <outputDirectory>src/main/avro</outputDirectory>
    </configuration>

</plugin>
```

```
<plugin>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-maven-plugin</artifactId>
    <version>3.1.0-SNAPSHOT</version>
    <configuration>
        <schemaRegistryUrls>
            <param>http://192.168.99.100:8081</param>
        </schemaRegistryUrls>
        <outputDirectory>src/main/avro</outputDirectory>
        <subjectPatterns>
            <param>^TestSubject000-(Key|Value)$</param>
        </subjectPatterns>
    </configuration>
</plugin>
```

## schema-registry:register

This plugin is used to register AVRO schemas for subjects.

|        Name        |                                                      Description                                                      |  Type  | Required | Default |
|:------------------:|:---------------------------------------------------------------------------------------------------------------------:|:------:|:--------:|:-------:|
| schemaRegistryUrls | Urls for the schema registry instance to connect to                                                                   |  List  |    yes   |         |
| subjects           | Map containing subject to schema path of the subjects to be registered.                                               |   Map  |    yes   |         |

```
<plugin>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-maven-plugin</artifactId>
    <version>3.1.0-SNAPSHOT</version>
    <configuration>
        <schemaRegistryUrls>
            <param>http://192.168.99.100:8081</param>
        </schemaRegistryUrls>
        <subjects>
            <TestSubject000-Key>src/main/avro/TestSubject000-Key.avsc</TestSubject000-Key>
            <TestSubject000-Value>src/main/avro/TestSubject000-Value.avsc</TestSubject000-Value>
        <subjects>
    </configuration>
    <goals>
        <goal>register</goal>
    </goals>
</plugin>
```

## schema-registry:test-compatibility

|        Name        |                                                      Description                                                      |  Type  | Required | Default |
|:------------------:|:---------------------------------------------------------------------------------------------------------------------:|:------:|:--------:|:-------:|
| schemaRegistryUrls | Urls for the schema registry instance to connect to                                                                   |  List  |    yes   |         |
| subjects           | Map containing subject to schema path of the subjects to be registered.                                               |   Map  |    yes   |         |

```
<plugin>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-maven-plugin</artifactId>
    <version>3.1.0-SNAPSHOT</version>
    <configuration>
        <schemaRegistryUrls>
            <param>http://192.168.99.100:8081</param>
        </schemaRegistryUrls>
        <subjects>
            <TestSubject000-Key>src/main/avro/TestSubject000-Key.avsc</TestSubject000-Key>
            <TestSubject000-Value>src/main/avro/TestSubject000-Value.avsc</TestSubject000-Value>
        <subjects>
    </configuration>
    <goals>
        <goal>test-compatibility</goal>
    </goals>
</plugin>
```


Contribute
----------

- Source Code: https://github.com/confluentinc/schema-registry
- Issue Tracker: https://github.com/confluentinc/schema-registry/issues

License
-------

The project is licensed under the Apache 2 license.
