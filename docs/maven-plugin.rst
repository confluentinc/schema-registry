Maven Plugin
------------

A Maven plugin is available to help throughout the development process.

schema-registry:download
========================

The `download` goal is used to pull down schemas from a schema registry server. This goal can be used to
This plugin is used to download AVRO schemas for the requested subjects and write them to a folder on the local file system.

``schemaRegistryUrls``
  Schema Registry Urls to connect to.

  * Type: String[]
  * Required: true

``outputDirectory``
  Output directory to write the schemas to.

  * Type: File
  * Required: true

``schemaExtension``
  The file extension to use for the output file name. This must begin with a '.' character.

  * Type: File
  * Required: false
  * Default: .avsc

``subjectPatterns``
  The subject patterns to download. This is a list of regular expressions. Patterns must match the entire subject name.

  * Type: String[]
  * Required: true

``prettyPrintSchemas``
  Flag to determine if the schemas should be pretty printed when written to disk.

  * Type: Boolean
  * Required: false
  * Default: true

::

    <plugin>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-schema-registry-maven-plugin</artifactId>
        <version>3.1.1</version>
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

schema-registry:test-compatibility
==================================

This goal is used to read schemas from the local file system and test them for compatibility against the
Schema Registry server(s). This goal can be used in a continuous integration pipeline to ensure that schemas in the
project are compatible with the schemas in another environment.

``schemaRegistryUrls``
  Schema Registry Urls to connect to.

  * Type: String[]
  * Required: true

``subjects``
  Map containing subject to schema path of the subjects to be registered.

  * Type: Map<String, File>
  * Required: true

::

    <plugin>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-schema-registry-maven-plugin</artifactId>
        <version>3.1.1</version>
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

schema-registry:register
========================

This goal is used to read schemas from the local file system and register them on the target Schema Registry server(s).
This goal can be used in a continuous deployment pipeline to push schemas to a new environment.

``schemaRegistryUrls``
  Schema Registry Urls to connect to.

  * Type: String[]
  * Required: true

``subjects``
  Map containing subject to schema path of the subjects to be registered.

  * Type: Map<String, File>
  * Required: true

::

    <plugin>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-schema-registry-maven-plugin</artifactId>
        <version>3.1.1</version>
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


