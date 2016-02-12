.. _schemaregistry_changelog:

Changelog
=========

Version 2.0.1
-------------

Schema Registry
~~~~~~~~~~~~~~~

* `PR-286 <https://github.com/confluentinc/schema-registry/pull/286>`_ - Update Kafka version to 0.9.0.1-cp1.

Version 2.0.0
-------------

Schema Registry
~~~~~~~~~~~~~~~

* `PR-141 <https://github.com/confluentinc/schema-registry/pull/141>`_ - Incorrect path to log4j.properties file for simple zip file layout
* `PR-143 <https://github.com/confluentinc/schema-registry/pull/143>`_ - schema-registry-start does not work with -daemon argument
* `PR-152 <https://github.com/confluentinc/schema-registry/pull/152>`_ - Added point about Google code style to Readme.
* `PR-163 <https://github.com/confluentinc/schema-registry/pull/163>`_ - Expose more information when registry fails to start
* `PR-165 <https://github.com/confluentinc/schema-registry/pull/165>`_ - Add compatibility support for SchemaRegistryClient
* `PR-167 <https://github.com/confluentinc/schema-registry/pull/167>`_ - Update the versionCache with a new schemaVersionMap for a subject
* `PR-169 <https://github.com/confluentinc/schema-registry/pull/169>`_ - Use correct URL to update compatibility setting of a subject
* `PR-180 <https://github.com/confluentinc/schema-registry/pull/180>`_ - GH-177: Remove unneeded content-type headers for GET ops in quickstart and README
* `PR-184 <https://github.com/confluentinc/schema-registry/pull/184>`_ - Support multiple registry urls in client
* `PR-186 <https://github.com/confluentinc/schema-registry/pull/186>`_ - Rename LocalSchemaRegistryClient to make it clear it is only intended to be used as a mock in tests. Fixes #185.
* `PR-187 <https://github.com/confluentinc/schema-registry/pull/187>`_ - Correct example response for POST /subjects/(string: subject)/versions
* `PR-188 <https://github.com/confluentinc/schema-registry/pull/188>`_ - Address GH-168; enable unit testing of CachedSchemaRegistryClient
* `PR-195 <https://github.com/confluentinc/schema-registry/pull/195>`_ - Fixed typo in Exception message
* `PR-196 <https://github.com/confluentinc/schema-registry/pull/196>`_ - Require Java 7
* `PR-197 <https://github.com/confluentinc/schema-registry/pull/197>`_ - Enable test code sharing
* `PR-198 <https://github.com/confluentinc/schema-registry/pull/198>`_ - Update jersey, jackson and junit versions to match rest-utils
* `PR-202 <https://github.com/confluentinc/schema-registry/pull/202>`_ - Correct minor docs error in example response
* `PR-203 <https://github.com/confluentinc/schema-registry/pull/203>`_ - Issue 194 rename main
* `PR-207 <https://github.com/confluentinc/schema-registry/pull/207>`_ - Issue #170: PUT /config/(string: subject) should return 4xx for unknown subjects
* `PR-210 <https://github.com/confluentinc/schema-registry/pull/210>`_ - Issue #208: Several RestApiTest test cases don't test proper exception behavior
* `PR-219 <https://github.com/confluentinc/schema-registry/pull/219>`_ - Update Kafka version to 0.8.3-SNAPSHOT so we can start developing using upcoming 0.8.3 features.
* `PR-237 <https://github.com/confluentinc/schema-registry/pull/237>`_ - Update uses of ZkUtils to match changes made in KAFKA-2639.
* `PR-240 <https://github.com/confluentinc/schema-registry/pull/240>`_ - Fixes Typo in the docs. 'actsas' -> 'acts as'
* `PR-242 <https://github.com/confluentinc/schema-registry/pull/242>`_ - Fixed schema registry build against kafka trunk
* `PR-243 <https://github.com/confluentinc/schema-registry/pull/243>`_ - Use x.y.z versioning scheme (i.e. 2.0.0-SNAPSHOT)
* `PR-245 <https://github.com/confluentinc/schema-registry/pull/245>`_ - Fix mvn assembly setup
* `PR-252 <https://github.com/confluentinc/schema-registry/pull/252>`_ - Use Kafka compiled with Scala 2.11
* `PR-257 <https://github.com/confluentinc/schema-registry/pull/257>`_ - Updated classpath in schema-registry-run-class to reflect changes in pom.xml
* `PR-258 <https://github.com/confluentinc/schema-registry/pull/258>`_ - CC-53: Add worker configs for Avro Kafka Connect that integrates with schema registry.

Serializers, Formatters, and Converters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `PR-146 <https://github.com/confluentinc/schema-registry/pull/146>`_ - Added decoder to return a specific Avro record from bytes.
* `PR-162 <https://github.com/confluentinc/schema-registry/pull/162>`_ - Add implementation of new Deserializer interface.
* `PR-192 <https://github.com/confluentinc/schema-registry/pull/192>`_ - Add new module for Kafka JSON serialization stuff
* `PR-193 <https://github.com/confluentinc/schema-registry/pull/193>`_ - Add more JSON codec support
* `PR-200 <https://github.com/confluentinc/schema-registry/pull/200>`_ - Switch serializer config classes to use AbstractConfig from confluent-common instead of from Kafka.
* `PR-222 <https://github.com/confluentinc/schema-registry/pull/222>`_ - Add AvroConverter in new copycat-avro-converter jar to convert Copycat and Avro data.
* `PR-234 <https://github.com/confluentinc/schema-registry/pull/234>`_ - Add AvroConverter support for Decimal, Date, Time, and Timestamp logical types.
* `PR-235 <https://github.com/confluentinc/schema-registry/pull/235>`_ - Add caching of schema conversions in AvroData and AvroConverter.
* `PR-247 <https://github.com/confluentinc/schema-registry/pull/247>`_ - Update for Copycat -> Kafka Connect renaming.
* `PR-251 <https://github.com/confluentinc/schema-registry/pull/251>`_ - Add tests of conversion of null values from Kafka Connect and fix handling of null for int8 and int16 types.
* `PR-254 <https://github.com/confluentinc/schema-registry/pull/254>`_ - Ensure AvroConverter passes through null values without adding schemas and that deserialized null values are converted to SchemaAndValue.NULL.
* `PR-255 <https://github.com/confluentinc/schema-registry/pull/255>`_ - CC-44: Include version when deserializing Kafka Connect data.
* `PR-256 <https://github.com/confluentinc/schema-registry/pull/256>`_ - Encode null values for schemaless array entries and map keys and values as an Anything record.
