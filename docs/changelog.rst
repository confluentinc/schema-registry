.. _schemaregistry_changelog:

Changelog
=========

Version 4.0.0
-------------

Upgrade Notes
^^^^^^^^^^^^^

In 4.0.0, initial creation or validation of the topic used to store schemas has been
reimplemented to use native Kafka protocol requests instead of accessing |zk| directly. This
means that you are no longer required to have direct access to the |zk| cluster backing your
Kafka cluster. However, note that this also requires appropriate permissions to create topics (on
first execution of the |sr|) or describe topics and configs (on subsequent executions
to validate the topic is configured correctly). If you have authentication and authorization enabled
on your Kafka cluster, you must ensure your principal has the correct permissions before
upgrading the |sr| cluster. Your principal must have the following permissions:

Create Schemas Topic

=========  ===========================  ===============================================
Operation  Resource                     Reason
=========  ===========================  ===============================================
Describe   Topic: ``kafkastore.topic``  Check existence of topic
Create     Cluster                      Create the schemas topic, set compaction policy
=========  ===========================  ===============================================

Validate Schemas Topic

===============  ===========================  =============================================
Operation        Resource                     Reason
===============  ===========================  =============================================
Describe         Topic: ``kafkastore.topic``  Check existence of topic
DescribeConfigs  Topic: ``kafkastore.topic``  Validate correct compaction policy on topic
===============  ===========================  =============================================

Version 3.3.0
-------------

* Upgrade avro to 1.8.2
* `PR-561 <https://github.com/confluentinc/schema-registry/pull/561>`_ - Use TO_AVRO_LOGICAL_CONVERTERS to convert default values that are Kafka connect logical data types to internal format which correspond to schema type. Logic copied from AvroData fromConnectData
* `PR-549 <https://github.com/confluentinc/schema-registry/pull/549>`_ - Replace usage of deprecated ZkUtils.DefaultAcls()
* Allow for some retries when validating that all nodes have the same master
* Relocate Avro serdes under a new `avro` package
* Increment Magic Byte for SchemaKey and add compatibility tests
* Add Delete Schema support
* Added avro-serde for Kafka Streams. Pulled from the example project.
* `#506 <https://github.com/confluentinc/schema-registry/issues/506>`_ - The AvroMessageFormatter passes byte[] to an Avro encoder, but Avro only likes ByteBuffer. So we need to ByteBuffer.wrap() instead.
* Added optional kafkastore.group.id config to override the one automatically created by the |sr|
* `PR-476 <https://github.com/confluentinc/schema-registry/pull/476>`_ - Adapt to KAFKA-4636 changes: Per listener security settings overrides (KIP-103)


Version 3.2.1
-------------

* `PR-503 <https://github.com/confluentinc/schema-registry/pull/503>`_ - CLIENTS-244: Update 3.2.0 changelog
* `PR-499 <https://github.com/confluentinc/schema-registry/pull/499>`_ - making sure |sr| doesn't start with uncompacted topic
* `PR-497 <https://github.com/confluentinc/schema-registry/pull/497>`_ - CLIENTS-103: Fix ArrayIndexOutOfBoundsException in SchemaRegistryPerformance by counting the registration attempt even if it failed.
* `PR-493 <https://github.com/confluentinc/schema-registry/pull/493>`_ - Fixes for CLIENTS-257
* `PR-494 <https://github.com/confluentinc/schema-registry/pull/494>`_ - MINOR: Add compact schemas topic doc
* `PR-458 <https://github.com/confluentinc/schema-registry/pull/458>`_ - CLIENTS-104: Add a few retries during startup to allow for slow metadata propagation after creating the _schemas topic.

Version 3.2.0
-------------

* `PR-428 <https://github.com/confluentinc/schema-registry/pull/428>`_ - Maven Checkstyle
* `PR-425 <https://github.com/confluentinc/schema-registry/pull/425>`_ - Logical Type support
* `PR-440 <https://github.com/confluentinc/schema-registry/pull/440>`_ - Documentation changes to reflect pull request 415
* `PR-451 <https://github.com/confluentinc/schema-registry/pull/451>`_ - Generalize schema incompatibility message.
* `PR-457 <https://github.com/confluentinc/schema-registry/pull/457>`_ - Update ClusterTestHarness to use o.a.k.common.utils.Time.
* `PR-458 <https://github.com/confluentinc/schema-registry/pull/458>`_ - CLIENTS-104: Add a few retries during startup to allow for slow metadata propagation after creating the _schemas topic.
* `PR-464 <https://github.com/confluentinc/schema-registry/pull/464>`_ - Improve request URL building in client
* `PR-465 <https://github.com/confluentinc/schema-registry/pull/465>`_ - Add topic to error string to make debugging easier.
* `PR-448 <https://github.com/confluentinc/schema-registry/pull/448>`_ - Fixes the following avro issues (complex union, document preservation, output schema != input schema)
* `PR-468 <https://github.com/confluentinc/schema-registry/pull/468>`_ - CC-443: AvroData's caches should be synchronized for thread-safety
* `PR-473 <https://github.com/confluentinc/schema-registry/pull/473>`_ - Fix build to work post KIP-103.
* `PR-474 <https://github.com/confluentinc/schema-registry/pull/474>`_ - Fix broker endpoint extraction to correctly translate to the non-ListenerName version that clients use to initiate broker connections.
* `PR-472 <https://github.com/confluentinc/schema-registry/pull/472>`_ - Handle primitive types when specific.avro.reader is true
* `PR-477 <https://github.com/confluentinc/schema-registry/pull/477>`_ - Docchangefor3.2
* `PR-488 <https://github.com/confluentinc/schema-registry/pull/488>`_ - Don't re-invoke SchemaBuilder.version() and SchemaBuilder.name() if the value has already been set.
* `PR-494 <https://github.com/confluentinc/schema-registry/pull/494>`_ - MINOR: Add compact schemas topic doc

Version 3.1.1
-------------
No changes

Version 3.1.0
-------------

|sr|
~~~~~~~~~~~~~~~

* `PR-429 <https://github.com/confluentinc/schema-registry/pull/429>`_ - Moving licenses and notices to a new format, generated by an internal script.
* `PR-415 <https://github.com/confluentinc/schema-registry/pull/415>`_ - Option to apply fully transitive schema compatibility checking
* `PR-412 <https://github.com/confluentinc/schema-registry/pull/412>`_ - Require bash since we use some bashisms and fix a copyright.
* `PR-396 <https://github.com/confluentinc/schema-registry/pull/396>`_ - |zk| and Kafka SASL support.
* `PR-384 <https://github.com/confluentinc/schema-registry/pull/384>`_ - Update the link to Google Java code Style
* `PR-372 <https://github.com/confluentinc/schema-registry/pull/372>`_ - Update KafkaStore to use moved TopicExistsException class.
* `PR-373 <https://github.com/confluentinc/schema-registry/pull/373>`_ - Added get all subjects.
* `PR-364 <https://github.com/confluentinc/schema-registry/pull/364>`_ - Increase testing timeouts from 5000ms to 15000ms
* `PR-346 <https://github.com/confluentinc/schema-registry/pull/346>`_ - configured log4j to write to log file

Serializers, Formatters, and Converters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `PR-403 <https://github.com/confluentinc/schema-registry/pull/403>`_ - CLIENTS-101: Document wire format for Avro.
* `PR-379 <https://github.com/confluentinc/schema-registry/pull/379>`_ - Maven plugin
* `PR-355 <https://github.com/confluentinc/schema-registry/pull/355>`_ - Adding support for Fixed data types
* `PR-352 <https://github.com/confluentinc/schema-registry/pull/352>`_ - Fix schemas.cache.config can't be overrided

Version 3.0.1
-------------

|sr|
~~~~~~~~~~~~~~~

* `PR-369 <https://github.com/confluentinc/schema-registry/pull/369>`_ - Introducing a new config: kafkastore.bootstrap.servers
* `PR-371 <https://github.com/confluentinc/schema-registry/pull/371>`_ - Fixing a bug where the listener port wasn't used in |zk|
* `PR-390 <https://github.com/confluentinc/schema-registry/pull/390>`_ - Include cURL output in quickstart
* `PR-392 <https://github.com/confluentinc/schema-registry/pull/392>`_ - Fix schemes used for listeners

Serializers, Formatters, and Converters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `PR-363 <https://github.com/confluentinc/schema-registry/pull/363>`_ - 3.0.x duplicate schema support
* `PR-365 <https://github.com/confluentinc/schema-registry/pull/365>`_ - Fix API compatibility regression from #363
* `PR-378 <https://github.com/confluentinc/schema-registry/pull/378>`_ - Support null namespace for Array of Records

Version 3.0.0
-------------

|sr|
~~~~~~~~~~~~~~~

* `PR-212 <https://github.com/confluentinc/schema-registry/pull/212>`_ - change the documentation on port to have a high
  priority and list it higher up in the docs
* `PR-298 <https://github.com/confluentinc/schema-registry/pull/298>`_ - Bump version to 3.0.0-SNAPSHOT and Kafka dependency
  to 0.10.0.0-SNAPSHOT
* `PR-300 <https://github.com/confluentinc/schema-registry/pull/300>`_ - Using the new 0.9 Kafka consumer.
* `PR-302 <https://github.com/confluentinc/schema-registry/pull/302>`_ - Fix build to handle rack aware changes in Kafka.
* `PR-305 <https://github.com/confluentinc/schema-registry/pull/305>`_ - Update to match changed methods in CoreUtils
* `PR-317 <https://github.com/confluentinc/schema-registry/pull/317>`_ - Change the 'host.name' importance to high
* `PR-319 <https://github.com/confluentinc/schema-registry/pull/319>`_ - KafkaStore SSL support.
* `PR-320 <https://github.com/confluentinc/schema-registry/pull/320>`_ - API reference uses 'integer' Avro type which isn't
  supported. 'int' is supported.
* `PR-329 <https://github.com/confluentinc/schema-registry/pull/329>`_ - https support.

Serializers, Formatters, and Converters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `PR-264 <https://github.com/confluentinc/schema-registry/pull/264>`_ - Add null checks to json serializer/deserializer
* `PR-274 <https://github.com/confluentinc/schema-registry/pull/274>`_ - Add support for Avro projections in decoders
* `PR-275 <https://github.com/confluentinc/schema-registry/pull/275>`_ - Fixed references to confluent common version
* `PR-276 <https://github.com/confluentinc/schema-registry/pull/276>`_ - Unit tests and bugfix for NPE when using nested
  optional fields
* `PR-278 <https://github.com/confluentinc/schema-registry/pull/278>`_ - Test cases for optional nested structs
* `PR-280 <https://github.com/confluentinc/schema-registry/pull/280>`_ - Fix fromConnectData for optional complex types
* `PR-290 <https://github.com/confluentinc/schema-registry/pull/290>`_ - Issue #284 Cannot set max.schemas.per.subject due to
  cast exception
* `PR-297 <https://github.com/confluentinc/schema-registry/pull/297>`_ - Allows any CharSequence implementation to be
  considered a string
* `PR-318 <https://github.com/confluentinc/schema-registry/pull/318>`_ - Minor cleanup
* `PR-323 <https://github.com/confluentinc/schema-registry/pull/323>`_ - Fix #142 - handle parsing non-json responses in the
  RestService
* `PR-332 <https://github.com/confluentinc/schema-registry/pull/332>`_ - Add status storage topic to Connect Avro sample config.

Version 2.0.1
-------------

|sr|
~~~~~~~~~~~~~~~

* `PR-286 <https://github.com/confluentinc/schema-registry/pull/286>`_ - Update Kafka version to 0.9.0.1-cp1.

Version 2.0.0
-------------

|sr|
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
* `PR-242 <https://github.com/confluentinc/schema-registry/pull/242>`_ - Fixed |sr| build against kafka trunk
* `PR-243 <https://github.com/confluentinc/schema-registry/pull/243>`_ - Use x.y.z versioning scheme (i.e. 2.0.0-SNAPSHOT)
* `PR-245 <https://github.com/confluentinc/schema-registry/pull/245>`_ - Fix mvn assembly setup
* `PR-252 <https://github.com/confluentinc/schema-registry/pull/252>`_ - Use Kafka compiled with Scala 2.11
* `PR-257 <https://github.com/confluentinc/schema-registry/pull/257>`_ - Updated classpath in schema-registry-run-class to reflect changes in pom.xml
* `PR-258 <https://github.com/confluentinc/schema-registry/pull/258>`_ - CC-53: Add worker configs for Avro Kafka Connect that integrates with |sr|.

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
