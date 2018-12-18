.. _serialization_and_evolution:

Data Serialization and Evolution
================================

When sending data over the network or storing it in a file, we need a
way to encode the data into bytes. The area of data serialization has
a long history, but has evolved quite a bit over the last few
years. People started with programming language specific serialization
such as Java serialization, which makes consuming the data in other
languages inconvenient. People then moved to language agnostic formats
such as JSON.

However, formats like JSON lack a strictly defined format, which has two significant drawbacks:

1. **Data consumers may not understand data producers:** The lack of structure makes consuming data in these formats
   more challenging because fields can be arbitrarily added or removed, and data can even be corrupted.  This drawback
   becomes more severe the more applications or teams across an organization begin consuming a data feed: if an
   upstream team can make arbitrary changes to the data format at their discretion, then it becomes very difficult to
   ensure that all downstream consumers will (continue to) be able to interpret the data.  What's missing is a
   "contract" (cf. schema below) for data between the producers and the consumers, similar to the contract of an API.
2. **Overhead and verbosity:** They are verbose because field names and type information have to be explicitly
   represented in the serialized format, despite the fact that are identical across all messages.

A few cross-language serialization libraries have emerged that require the data structure to be formally defined by
some sort of schemas. These libraries include `Avro <http://avro.apache.org>`_,
`Thrift <http://thrift.apache.org>`_, and `Protocol Buffers <https://github.com/google/protobuf>`_.  The advantage of
having a schema is that it clearly specifies the structure, the type and the meaning (through documentation) of the
data.  With a schema, data can also be encoded more efficiently.

If you don't know where to start, then we particularly recommend :ref:`Avro <serialization_and_evolution-avro>`, which
is also supported in the Confluent Platform.  In the next section we briefly introduce Avro, and how it can be used to
support typical schema evolution.


.. _serialization_and_evolution-avro:

Avro
----


Defining an Avro Schema
^^^^^^^^^^^^^^^^^^^^^^^

An Avro schema defines the data structure in a JSON format.

The following is an example Avro schema that specifies a user record with two fields: ``name`` and ``favorite_number``
of type ``string`` and ``int``, respectively.

.. sourcecode:: json

    {"namespace": "example.avro",
     "type": "record",
     "name": "user",
     "fields": [
         {"name": "name", "type": "string"},
         {"name": "favorite_number",  "type": "int"}
     ]
    }

You can then use this Avro schema, for example, to serialize a Java object (POJO) into bytes, and deserialize these
bytes back into the Java object.

One of the interesting things about Avro is that it not only requires
a schema during data serialization, but also during data
deserialization. Because the schema is provided at decoding time,
metadata such as the field names don't have to be explicitly encoded
in the data. This makes the binary encoding of Avro data very compact.


Schema Evolution
^^^^^^^^^^^^^^^^

An important aspect of data management is **schema evolution**.  After the initial schema is defined, applications may
need to evolve it over time. When this happens, it's critical for the downstream consumers to be able to handle data
encoded with both the old and the new schema seamlessly. This is an area that tends to be overlooked in practice until
you run into your first production issues.  Without thinking through data management and schema evolution carefully,
people often pay a much higher cost later on.

There are three common patterns of schema evolution:

1. :ref:`backward compatibility <avro-backward_compatibility>`
2. :ref:`forward compatibility <avro-forward_compatibility>`
3. :ref:`full compatibility <avro-full_compatibility>`


.. _avro-backward_compatibility:

Backward Compatibility
^^^^^^^^^^^^^^^^^^^^^^

Backward compatibility means that data encoded with an older schema can be read with a newer schema.

Consider the case where all the data in Kafka is also loaded into HDFS, and we want to run SQL queries (e.g., using
Apache Hive) over all the data. Here, it is important that the same SQl queries continue to work even as the data is
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
deserializing the data encoded with the old schema.  Had the default value been ommitted in the new field, the new
schema would not be backward compatible with the old one since it's not clear what value should be assigned to the new
field, which is missing in the old data.

.. note::

    **Avro implementation details:**
    Take a look at `ResolvingDecoder <https://github.com/apache/avro/blob/release-1.7.7/lang/java/avro/src/main/java/org/apache/avro/io/ResolvingDecoder.java>`__
    in the Apache Avro project to understand how, for data that was encoded with an older schema, Avro decodes that
    data with a newer, backward-compatible schema.


.. _avro-forward_compatibility:

Forward Compatibility
^^^^^^^^^^^^^^^^^^^^^

Forward compatibility means that data encoded with a newer schema can be read with an older schema.

Consider a use case where a consumer has application logic tied to a particular version of the schema. When the schema
evolves, the application logic may not be updated immediately. Therefore, we need to be able to project data with newer
schemas onto the (older) schema that the application understands. To support this use case, we can evolve the schemas
in a forward compatible way: data encoded with the new schema can be read with the old schema.  For example, the new
user schema we looked at in the previous section on :ref:`backward compatibility <avro-backward_compatibility>` is also
forward compatible with the old one.  When projecting data written with the new schema to the old one, the new field is
simply dropped.  Had the new schema dropped the original field ``favorite_number`` (number, not color), it would not be
forward compatible with the original user schema since we wouldn't know how to fill in the value for ``favorite_number``
for the new data because the original schema did not specify a default value for that field.


.. _avro-full_compatibility:

Full Compatibility
^^^^^^^^^^^^^^^^^^

Full compatibility means schemas are backward **and** forward compatible.

To support both previous use cases on the same data, we can evolve the schemas in a fully compatible way: old data can
be read with the new schema, and new data can also be read with the old schema.


|sr-long|
^^^^^^^^^

As you can see, when using Avro, one of the most important things is to manage its schemas and reason about how those
schemas should evolve. :ref:`Confluent Schema Registry <schemaregistry_intro>` is built for exactly that purpose.
You can find out the details on how to use it to store Avro schemas and enforce certain compatibility rules during
schema evolution by looking at the :ref:`schemaregistry_api`.
