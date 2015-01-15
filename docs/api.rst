API Reference
=============

Overview
--------

Content Types
^^^^^^^^^^^^^

The schema registry REST server uses content types for both requests and responses to indicate the serialization format of the data as well as the version of the API being used. Currently, the only serialization format supported is JSON and the only version of the API is ``v1``. However, to remain compatible with future versions, you *should* specify preferred content types in requests and check the content types of responses.

The preferred format for content types is ``application/vnd.schemaregistry.v1+json``, where ``v1`` is the API version and ``json`` is the serialization format. However, other less specific content types are permitted, including ``application/vnd.schemaregistry+json`` to indicate no specific API version should be used
(the most recent stable version will be used), ``application/json``, and ``application/octet-stream``. The latter two are only supported for compatibility and ease of use.

Your requests *should* specify the most specific format and version information possible via the HTTP ``Accept`` header::

      Accept: application/vnd.schemaregistry.v1+json

The server also supports content negotiation, so you may include multiple, weighted preferences::

      Accept: application/vnd.schemaregistry.v1+json; q=0.9, application/json; q=0.5

which can be useful when, for example, a new version of the API is preferred but
you cannot be certain it is available yet.

Errors
^^^^^^

All API endpoints use a standard error message format for any requests that return an HTTP status indicating an error (any 400 or 500 statuses). For example, an request entity that omits a required field may generate the following response:

   .. sourcecode:: http

      HTTP/1.1 422 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
          "error_code": 422,
          "message": "schema may not be empty"
      }

Although it is good practice to check the status code, you may safely parse the response of any non-DELETE API calls and check for the presence of an ``error_code`` field to detect errors.

Subjects
--------

The subjects resource provides a list of all registered subjects in your schema registry. A subject refers to the name under which the schema is registered. If you are using the schema registry for Kafka, then a subject refers to either a "<topic>,key" or "<topic>,value" depending on whether you are registering the key schema for that topic or the value schema. 

.. http:get:: /subjects

   Get a list of registered subjects. 

   :>jsonarr string name: Subject

   **Example request**:

   .. sourcecode:: http

      GET /subjects HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      ["subject1", "subject2"]

.. http:get:: /subjects/(string:subject)

   Get metadata about a specific subject.

   :param string subject: Name of the subject to get metadata about

   :>json string name: Name of the subject
   :>json string compatibility: The compatibility level for this subject. Null if it was never overriden

   :statuscode 404: Subject not found

   **Example request**:

   .. sourcecode:: http

      GET /subjects/test HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "name": "test",
        "compatibility": "full"
      }

Schemas 
----------

.. http:get:: /subjects/{subject}/versions

   Get a list of versions registered under the specified subject.

   :param string subject: the name of the subject

   :>jsonarr int version: version of the schema registered under this subject

   **Example request**:

   .. sourcecode:: http

      GET /subjects/test/versions HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      [
        1, 2, 3, 4
      ]


.. http:get:: /subjects/(string:subject)/versions/(int:version)

   Get a specific version of the schema registered under this subject

   :param string subject: Name of the subject
   :param int version: Version of the schema to be returned

   :>json string name: Name of the subject that this schema is registered under
   :>json int version: Version of the returned schema
   :>json string schema: The Avro schema string

   :statuscode 404:
      * Error code 40401 -- Subject not found
      * Error code 40402 -- Version not found

   **Example request**:

   .. sourcecode:: http

      GET /subjects/test/versions/1 HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "name": 1,
        "version": 1,
        "schema": "{\"type\": \"string\"}"
      }

.. http:post:: /subjects/(string:subject)/versions

   Register a new schema under the specified subject. If successfully registered, this returns the unique identifier of this schema in the registry. The returned identifier should be used to retrieve this schema from the registry and is different from the schema's version which is associated with the subject. 

   A schema should be compatible with the previously registered schemas (if there are any) as per the configured compatibility level. The configured compatibility level can be obtained by issuing a ``GET http:get:: /config/{subject}``. If that returns null, then ``GET http:get:: /config`` 

   :param string subject: Subject under which the schema will be registered
   :reqjson schema: The Avro schema string

   :statuscode 404: Subject not found

   **Example request**:

   .. sourcecode:: http

      POST /subjects/test/versions HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      { 
        "type": "record",
        "name": "test",
        "fields":
           [ 
             {
               "type": "string",
               "name": "field1"
             },
             {
               "type": "integer",
               "name": "field2"
             }
           ]
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      1


Config
---------

The config resource allows you to inspect the cluster-level configuration values as well as subject overrides. 

.. http:post:: /config

   Update global compatibility level.

   :<json string compatibility: New global compatibility level. Must be one of NONE, FULL, FORWARD, BACKWARD

   .. sourcecode:: http

      POST /consumers/testgroup/ HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      {
        "compatibility": "FULL",
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

.. http:get:: /config

   Get global compatibility level.

   :>json string compatibility: New global compatibility level. Will be one of NONE, FULL, FORWARD, BACKWARD

   **Example request**:

   .. sourcecode:: http

      GET /config HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "compatibility": "FULL"
      }

.. http:post:: /config/(string:subject)

   Update compatibility level for the specified subject.

   :param string subject: Name of the subject
   :<json string compatibility: New global compatibility level. Must be one of NONE, FULL, FORWARD, BACKWARD

   :statuscode 404: Subject not found

   **Example request**:

   .. sourcecode:: http

      POST /config/test HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      {
        "compatibility": "FULL",
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

.. http:get:: /config/(string:subject)

   Get global compatibility level.

   :param string subject: Name of the subject
   :>json string compatibility: New global compatibility level. Will be one of NONE, FULL, FORWARD, BACKWARD
  
   :statuscode 404: Subject not found

   **Example request**:

	.. sourcecode:: http

	   GET /config/test HTTP/1.1
	   Host: schemaregistry.example.com
	   Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

	  HTTP/1.1 200 OK
	  Content-Type: application/vnd.schemaregistry.v1+json

	  {
	     "compatibility": "FULL"
	  }
