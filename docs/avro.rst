.. _schema_evolution_and_compatibility:

Schema Evolution and Compatibility
==================================

Schema Evolution
----------------

An important aspect of data management is schema evolution.  After the initial schema is defined, applications may
need to evolve it over time. When this happens, it's critical for the downstream consumers to be able to handle data
encoded with both the old and the new schema seamlessly. This is an area that tends to be overlooked in practice until
you run into your first production issues.  Without thinking through data management and schema evolution carefully,
people often pay a much higher cost later on.

When using Avro, one of the most important things is to manage its schemas and consider how those
schemas should evolve. :ref:`Confluent Schema Registry <schemaregistry_intro>` is built for exactly that purpose.
Schema compatibility checking is implemented in |sr| by versioning every single schema.
The compatibility type determines how |sr| compares the new schema with previous versions of a schema, for a given subject.
When a schema is first created for a subject, it gets a unique id and it gets a version number, i.e., version 1.
When the schema is updated (if it passes compatibility checks), it gets a new unique id and it gets an incremented version number, i.e., version 2.

These are the compatibility types:

.. include:: includes/compatibility_list.rst

The compatibility types can be grouped into the following four common patterns of schema evolution:

1. :ref:`BACKWARD compatibility <avro-backward_compatibility>`
2. :ref:`FORWARD compatibility <avro-forward_compatibility>`
3. :ref:`FULL compatibility <avro-full_compatibility>`
4. :ref:`no compatibility checking <avro-none_compatibility>`

Transitive
^^^^^^^^^^

.. include:: includes/transitive.rst

Transitive compatibility checking is important once you have more than two versions for a given subject.
Refer to an `example of schema changes <https://github.com/confluentinc/schema-registry/issues/209>`__ which are incrementally compatible, but not transitively so.



Compatibility Types
-------------------

.. _avro-backward_compatibility:

Backward Compatibility
^^^^^^^^^^^^^^^^^^^^^^

``BACKWARD`` compatibility means that data produced with an older schema can be read by consumers written to use a newer schema.
For example, if there are three schemas for a subject that change in order `X-2`, `X-1`, and `X` then ``BACKWARD`` compatibility ensures that consumers using the newest schema `X` can process data written by producers using schema `X` or `X-1`, but not necessarily `X-2`.

An example of a backward compatible change is a removal of a field. A consumer that was developed to process events without this field will be able to process events written with the old schema and contain the field – the consumer will just ignore that field.

Consider the case where all the data in Kafka is also loaded into HDFS, and we want to run SQL queries (e.g., using
Apache Hive) over all the data. Here, it is important that the same SQL queries continue to work even as the data is
undergoing changes over time.  To support this kind of use case, we can evolve the schemas in a backward compatible way.
Avro has a set of `rules <http://avro.apache.org/docs/1.7.7/spec.html#Schema+Resolution>`_ on what changes are allowed
in the new schema for it to be backward compatible. If all schemas are evolved in a backward compatible way, we can
always use the latest schema to query all the data uniformly.

For example, an application can evolve the
:ref:`user schema from the previous section <serialization_and_evolution-avro>` to the following by adding a new field
``favorite_color``:

.. sourcecode:: json

    {"namespace": "example.avro",
     "type": "record",
     "name": "user",
     "fields": [
         {"name": "name", "type": "string"},
         {"name": "favorite_number",  "type": "int"},
         {"name": "favorite_color", "type": "string", "default": "green"}
     ]
    }

Note that the new field ``favorite_color`` has the default value "green". This allows data encoded with the old schema
to be read with the new one. The default value specified in the new schema will be used for the missing field when
deserializing the data encoded with the old schema.  Had the default value been omitted in the new field, the new
schema would not be backward compatible with the old one since it's not clear what value should be assigned to the new
field, which is missing in the old data.

.. note::

    **Avro implementation details:**
    Take a look at `ResolvingDecoder <https://github.com/apache/avro/blob/release-1.7.7/lang/java/avro/src/main/java/org/apache/avro/io/ResolvingDecoder.java>`__
    in the Apache Avro project to understand how, for data that was encoded with an older schema, Avro decodes that
    data with a newer, backward-compatible schema.

If the compatibility is set to ``BACKWARD_TRANSITIVE`` (not just ``BACKWARD``), then it means that consumers using the new schema can read data written by producers using all previously registered schemas (not just the latest schema).
For example, if there are three schemas for a subject that change in order `X-2`, `X-1`, and `X` then ``BACKWARD_TRANSITIVE`` compatibility ensures that consumers using the newest schema `X` can process data written by producers using schema `X`, `X-1`, or `X-2`.


.. _avro-forward_compatibility:

Forward Compatibility
^^^^^^^^^^^^^^^^^^^^^

``FORWARD`` compatibility means that data produced with a newer schema can be read by consumers written to use an older schema, even though they may not be able to use the full capabilities of the new schema.
For example, if there are three schemas for a subject that change in order `X-2`, `X-1`, and `X` then ``FORWARD`` compatibility ensures that data written by producers using the newer schema `X` can be processed by consumers using schema `X` or `X-1`, but not necessarily `X-2`.

An example of a forward compatible schema modification is adding a new field. In most data formats, consumers that were written to process events without the new field will be able to continue doing so even when they receive new events that contain the new field.

Consider a use case where a consumer has application logic tied to a particular version of the schema. When the schema
evolves, the application logic may not be updated immediately. Therefore, we need to be able to project data with newer
schemas onto the (older) schema that the application understands. To support this use case, we can evolve the schemas
in a forward compatible way: data encoded with the new schema can be read with the old schema.  For example, the new
user schema we looked at in the previous section on :ref:`backward compatibility <avro-backward_compatibility>` is also
forward compatible with the old one.  When projecting data written with the new schema to the old one, the new field is
simply dropped.  Had the new schema dropped the original field ``favorite_number`` (number, not color), it would not be
forward compatible with the original user schema since we wouldn't know how to fill in the value for ``favorite_number``
for the new data because the original schema did not specify a default value for that field.

If the compatibility is set to ``FORWARD_TRANSITIVE`` (not just ``FORWARD``), then it means that consumers using all previousely registered schemas (not just the latest schema) can read data written by producers using the new schema.
For example, if there are three schemas for a subject that change in order `X-2`, `X-1`, and `X` then ``FORWARD_TRANSITIVE`` compatibility ensures that data written by producers using the newer schema `X` can be processed by consumers using schema `X`, `X-1`, or `X-2`.


.. _avro-full_compatibility:

Full Compatibility
^^^^^^^^^^^^^^^^^^

``FULL`` compatibility means schemas are both backward **and** forward compatible.
Schemas evolve in a fully compatible way: old data can be read with the new schema, and new data can also be read with the old schema.
For example, if there are three schemas for a subject that change in order `X-2`, `X-1`, and `X` then ``FULL`` compatibility ensures that consumers using the newest schema `X` can process data written by producers using schema `X` or `X-1`, but not necessarily `X-2`, and that data written by producers using the newer schema `X` can be processed by consumers using schema `X` or `X-1`, but not necessarily `X-2`.

In some data formats, such as JSON, there are no full-compatible changes. Every modification is either only forward or only backward compatible. But in other data formats, like Avro, you can define fields with default values. In that case adding or removing a field with a default value is a fully compatible change.

If the compatibility is set to ``FULL_TRANSITIVE`` (not just ``FULL``), then it means that the new schema is forward and backward compatible with all previously registered schemas (not just the latest schema).
For example, if there are three schemas for a subject that change in order `X-2`, `X-1`, and `X` then ``FULL_TRANSITIVE`` compatibility ensures that consumers using the newest schema `X` can process data written by producers using schema `X`, `X-1`, or `X-2`, and that data written by producers using the newer schema `X` can be processed by consumers using schema `X`, `X-1`, or `X-2`.


.. _avro-none_compatibility:

No Compatibility Checking
^^^^^^^^^^^^^^^^^^^^^^^^^

``NONE`` compatibility type means schema compatibility checks are disabled.

Sometimes we make incompatible changes.
For example, modifying a field type from ``Number`` to ``String``.
In this case, you will either need to upgrade all producers and consumers to the new schema version at the same time, or more likely – create a brand-new topic and start migrating applications to use the new topic and new schema, avoiding the need to handle two incompatible versions in the same topic.


Summary Compatibility Types
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Here is a summary of the types of schema changes allowed for the different compatibility types, for a given subject.

+------------------------------------+-------------------------------------+-------------------------------------+
| Compatibility Type                 | Changes allowed                     | Check against which schemas         |
+====================================+=====================================+=====================================+
| ``BACKWARD``                       | - Delete fields                     | Latest                              |
|                                    | - Add optional fields               |                                     |
+------------------------------------+-------------------------------------+-------------------------------------+
| ``BACKWARD_TRANSITIVE``            | - Delete fields                     | All previous                        |
|                                    | - Add optional fields               |                                     |
+------------------------------------+-------------------------------------+-------------------------------------+
| ``FORWARD``                        | - Add fields                        | Latest                              |
|                                    | - Delete optional fields            |                                     |
+------------------------------------+-------------------------------------+-------------------------------------+
| ``FORWARD_TRANSITIVE``             | - Add fields                        | All previous                        |
|                                    | - Delete optional fields            |                                     |
+------------------------------------+-------------------------------------+-------------------------------------+
| ``FULL``                           | - Modify optional fields            | Latest                              |
+------------------------------------+-------------------------------------+-------------------------------------+
| ``FULL_TRANSITIVE``                | - Modify optional fields            | All previous                        |
+------------------------------------+-------------------------------------+-------------------------------------+
| ``NONE``                           | - All changes are accepted          | Checking disabled                   |
+------------------------------------+-------------------------------------+-------------------------------------+


Examples
^^^^^^^^

Each of the sections above has an example of the compatibility type.
An additional reference is the `Avro compatibility test suite <https://github.com/confluentinc/schema-registry/blob/master/core/src/test/java/io/confluent/kafka/schemaregistry/avro/AvroCompatibilityTest.java>`__, which presents multiple test cases with two schemas and the respective result of the compatibility test between them.


Using Compatibility Types
-------------------------

You can find out the details on how to use |sr| to store Avro schemas and enforce certain compatibility rules during schema evolution by looking at the :ref:`schemaregistry_api`.
Here are some tips to get you started.

To check the currently configured compatibility type, view the configured setting:

#.  `Using the Schema Registry REST API <https://docs.confluent.io/current/schema-registry/docs/using.html#getting-the-top-level-config>`__.

To set the compatibility level, you may configure it in one of two ways:

#. `In your client application <https://docs.confluent.io/current/schema-registry/docs/config.html#avro-compatibility-level>`__
#. `Using the Schema Registry REST API <https://docs.confluent.io/current/schema-registry/docs/using.html#updating-compatibility-requirements-globally>`__

To validate the compatibility of a given schema, you may test it one of two ways:

#. `Using the Schema Registry Maven Plugin <https://docs.confluent.io/current/schema-registry/docs/maven-plugin.html#schema-registry-test-compatibility>`__
#. `Using the Schema Registry REST API <https://docs.confluent.io/current/schema-registry/docs/using.html#testing-compatibility-of-a-schema-with-the-latest-schema-under-subject-kafka-value>`__

Refer to the |sr-long| Tutorial which has an example of :ref:`checking schema compatibility <schema_registry_tutorial>`.

