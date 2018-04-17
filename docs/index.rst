.. Schema Registry documentation master file, created by
   sphinx-quickstart on Wed Dec 17 14:17:15 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

|sr|
================

|sr| is part of the `Confluent Open Source <https://www.confluent.io/product/confluent-open-source/>`_ and `Confluent Enterprise <https://www.confluent.io/product/confluent-enterprise/>`_ distributions.  The |sr| stores a versioned history of all schemas and allows for the evolution of schemas according to the configured compatibility settings and expanded Avro support.

Contents:

.. toctree::
   :maxdepth: 3

   intro
   changelog
   api
   config
   design
   operations
   multidc
   security
   serializer-formatter
   schema-deletion-guidelines
   maven-plugin
   connect

To see a working example of |sr|, check out :ref:`Confluent Platform demo<cp-demo>`. The demo shows you how to deploy a Kafka streaming ETL, including Schema Registry, using KSQL for stream processing.
