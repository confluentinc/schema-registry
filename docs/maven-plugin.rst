Maven Plugin
------------

A Maven plugin is available to help throughout the development process.


Available Mojos
---------------

## schema-registry:download

This plugin is used to download AVRO schemas for the requested subjects and write them to a folder on the local file system.

|        Name        |                                                      Description                                                      |  Type   | Required | Default |
|:------------------:|:---------------------------------------------------------------------------------------------------------------------:|:-------:|:--------:|:-------:|
| schemaRegistryUrls | Urls for the schema registry instance to connect to                                                                   |  List   |    yes   |         |
| outputDirectory    | Output directory to write the schemas to.                                                                             | String  |    yes   |         |
| schemaExtension    | The file extension to use for the output file name. This must begin with a '.' character.                             | String  |    no    |  .avsc  |
| subjectPatterns    | The subject patterns to download. This is a list of regular expressions. Patterns must match the entire subject name. | List    |    yes   |         |
| prettyPrintSchemas | Flag to determine if the schemas should be pretty printed when written to disk.                                       | Booelan |    no    |   true  |

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
            <TestSubject000-key>src/main/avro/TestSubject000-Key.avsc</TestSubject000-key>
            <TestSubject000-value>src/main/avro/TestSubject000-Value.avsc</TestSubject000-value>
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
            <TestSubject000-key>src/main/avro/TestSubject000-Key.avsc</TestSubject000-key>
            <TestSubject000-value>src/main/avro/TestSubject000-Value.avsc</TestSubject000-value>
        <subjects>
    </configuration>
    <goals>
        <goal>test-compatibility</goal>
    </goals>
</plugin>
```
