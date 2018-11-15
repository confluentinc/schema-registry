.. _schemaregistry_api:

|sr| API Reference
==================

.. _heading2-1:

Overview
--------

.. _heading3-1:

Compatibility
^^^^^^^^^^^^^

The |sr| server can enforce certain compatibility rules when new schemas are registered in a subject. Currently, we support the following compatibility rules.

* Backward compatibility (default): A new schema is backwards compatible if it can be used to read the data written in the latest registered schema.
* Transitive backward compatibility: A new schema is transitively backwards compatible if it can be used to read the data written in all previously registered schemas. Backward compatibility is useful for loading data into systems like Hadoop since one can always query data of all versions using the latest schema.
* Forward compatibility: A new schema is forward compatible if the latest registered schema can read data written in this schema.
* Transitive forward compatibility: A new schema is transitively forward compatible if all previous schemas can read data written in this schema. Forward compatibility is useful for consumer applications that can only deal with data in a particular version that may not always be the latest version.
* Full compatibility: A new schema is fully compatible if it’s both backward and forward compatible with the latest registered schema.
* Transitive full compatibility: A new schema is transitively full compatible if it’s both backward and forward compatible with all previously registered schemas.
* No compatibility: A new schema can be any schema as long as it’s a valid Avro.

We recommend keeping the default backward compatibility since it's common to have all data loaded into Hadoop.

.. ifconfig:: platform_docs

   More details on Avro schema resolution can be found :ref:`here <serialization_and_evolution>`.

.. ifconfig:: not platform_docs

   More details on Avro schema resolution can be found `here <http://avro.apache.org/docs/1.7.7/spec.html#Schema+Resolution>`_.

.. _heading3-2:

Content Types
^^^^^^^^^^^^^

The |sr| REST server uses content types for both requests and responses to indicate the serialization format of the data as well as the version of the API being used. Currently, the only serialization format supported is JSON and the only version of the API is ``v1``. However, to remain compatible with future versions, you *should* specify preferred content types in requests and check the content types of responses.

The preferred format for content types is ``application/vnd.schemaregistry.v1+json``, where ``v1`` is the API version and ``json`` is the serialization format. However, other less specific content types are permitted, including ``application/vnd.schemaregistry+json`` to indicate no specific API version should be used
(the most recent stable version will be used), ``application/json``, and ``application/octet-stream``. The latter two are only supported for compatibility and ease of use.

Your requests *should* specify the most specific format and version information possible via the HTTP ``Accept`` header::

      Accept: application/vnd.schemaregistry.v1+json

The server also supports content negotiation, so you may include multiple, weighted preferences::

      Accept: application/vnd.schemaregistry.v1+json; q=0.9, application/json; q=0.5

which can be useful when, for example, a new version of the API is preferred but
you cannot be certain it is available yet.

.. _heading3-3:

Errors
^^^^^^

All API endpoints use a standard error message format for any requests that return an HTTP status indicating an error (any 400 or 500 statuses). For example, a request entity that omits a required field may generate the following response:

   .. sourcecode:: http

      HTTP/1.1 422 Unprocessable Entity
      Content-Type: application/vnd.schemaregistry.v1+json

      {
          "error_code": 422,
          "message": "schema may not be empty"
      }

Although it is good practice to check the status code, you may safely parse the response of any non-DELETE API calls and check for the presence of an ``error_code`` field to detect errors.

.. _heading2-2:

Schemas
-------

.. http:get:: /schemas/ids/{int: id}

   Get the schema string identified by the input ID.

   :param int id: the globally unique identifier of the schema

   :>json string schema: Schema string identified by the ID

   :statuscode 404:
      * Error code 40403 -- Schema not found
   :statuscode 500:
      * Error code 50001 -- Error in the backend datastore

   **Example request**:

   .. sourcecode:: http

      GET /schemas/ids/1 HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "schema": "{\"type\": \"string\"}"
      }

.. _heading2-3:

Subjects
--------

The subjects resource provides a list of all registered subjects in your |sr|. A subject refers to the name under which the schema is registered. If you are using |sr| for Kafka, then a subject refers to either a "<topic>-key" or "<topic>-value" depending on whether you are registering the key schema for that topic or the value schema.

.. http:get:: /subjects

   Get a list of registered subjects. 

   :>jsonarr string name: Subject

   :statuscode 500: 
      * Error code 50001 -- Error in the backend datastore

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

.. http:get:: /subjects/(string: subject)/versions

   Get a list of versions registered under the specified subject.

   :param string subject: the name of the subject

   :>jsonarr int version: version of the schema registered under this subject

   :statuscode 404:
      * Error code 40401 -- Subject not found
   :statuscode 500:
      * Error code 50001 -- Error in the backend datastore

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

.. http:delete:: /subjects/(string: subject)

   Deletes the specified subject and its associated compatibility level if registered. It is recommended to use this API only when a topic needs to be recycled or in development environment.

   :param string subject: the name of the subject

   :>jsonarr int version: version of the schema deleted under this subject

   :statuscode 404:
      * Error code 40401 -- Subject not found
   :statuscode 500:
      * Error code 50001 -- Error in the backend datastore

   **Example request**:

   .. sourcecode:: http

      DELETE /subjects/test HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      [
        1, 2, 3, 4
      ]

.. http:get:: /subjects/(string: subject)/versions/(versionId: version)

   Get a specific version of the schema registered under this subject

   :param string subject: Name of the subject
   :param versionId version: Version of the schema to be returned. Valid values for versionId are between [1,2^31-1] or the string "latest". "latest" returns the last registered schema under the specified subject. Note that there may be a new latest schema that gets registered right after this request is served.  

   :>json string name: Name of the subject that this schema is registered under
   :>json int version: Version of the returned schema
   :>json string schema: The Avro schema string

   :statuscode 404:
      * Error code 40401 -- Subject not found
      * Error code 40402 -- Version not found
   :statuscode 422: 
      * Error code 42202 -- Invalid version
   :statuscode 500:
      * Error code 50001 -- Error in the backend data store

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
        "name": "test",
        "version": 1,
        "schema": "{\"type\": \"string\"}"
      }

.. http:get:: /subjects/(string: subject)/versions/(versionId: version)/schema

   Get the avro schema for the specified version of this subject. The unescaped schema only is returned.

   :param string subject: Name of the subject
   :param versionId version: Version of the schema to be returned. Valid values for versionId are between [1,2^31-1] or the string "latest". "latest" returns the last registered schema under the specified subject. Note that there may be a new latest schema that gets registered right after this request is served.

   :>json string schema: The Avro schema string (unescaped)

   :statuscode 404:
      * Error code 40401 -- Subject not found
      * Error code 40402 -- Version not found
   :statuscode 422:
      * Error code 42202 -- Invalid version
   :statuscode 500:
      * Error code 50001 -- Error in the backend data store

   **Example request**:

   .. sourcecode:: http

      GET /subjects/test/versions/1/schema HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {"type": "string"}

.. http:post:: /subjects/(string: subject)/versions

   Register a new schema under the specified subject. If successfully registered, this returns the unique identifier of this schema in the registry. The returned identifier should be used to retrieve this schema from the schemas resource and is different from the schema's version which is associated with the subject.
   If the same schema is registered under a different subject, the same identifier will be returned. However, the version of the schema may be different under different subjects.

   A schema should be compatible with the previously registered schema or schemas (if there are any) as per the configured compatibility level. The configured compatibility level can be obtained by issuing a ``GET http:get:: /config/(string: subject)``. If that returns null, then ``GET http:get:: /config``

   When there are multiple instances of |sr| running in the same cluster, the schema registration request will be forwarded to one of the instances designated as the master. If the master is not available, the client will get an error code indicating that the forwarding has failed.

   :param string subject: Subject under which the schema will be registered
   :reqjson schema: The Avro schema string

   :statuscode 409: Incompatible Avro schema
   :statuscode 422: 
      * Error code 42201 -- Invalid Avro schema
   :statuscode 500:
      * Error code 50001 -- Error in the backend data store
      * Error code 50002 -- Operation timed out
      * Error code 50003 -- Error while forwarding the request to the master

   **Example request**:

   .. sourcecode:: http

      POST /subjects/test/versions HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      {
        "schema":
          "{
             \"type\": \"record\",
             \"name\": \"test\",
             \"fields\":
               [
                 {
                   \"type\": \"string\",
                   \"name\": \"field1\"
                 },
                 {
                   \"type\": \"int\",
                   \"name\": \"field2\"
                 }
               ]
           }"
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {"id":1}

.. http:post:: /subjects/(string: subject)

   Check if a schema has already been registered under the specified subject. If so, this returns the schema string along with its globally unique identifier, its version under this subject and the subject name.  

   :param string subject: Subject under which the schema will be registered
	
   :>json string subject: Name of the subject that this schema is registered under
   :>json int id: Globally unique identifier of the schema
   :>json int version: Version of the returned schema
   :>json string schema: The Avro schema string
	
   :statuscode 404:
      * Error code 40401 -- Subject not found
      * Error code 40403 -- Schema not found
   :statuscode 500: Internal server error

   **Example request**:

   .. sourcecode:: http

      POST /subjects/test HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      {
	    "schema":
	       "{
		      \"type\": \"record\",
		      \"name\": \"test\",
		      \"fields\":
		        [
		          {
		            \"type\": \"string\",
		            \"name\": \"field1\"
		          },
		          {
		            \"type\": \"int\",
		            \"name\": \"field2\"
		          }
		        ]
		    }"
	  }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json
           
      {
	    "subject": "test",
	    "id": 1
	    "version": 3
	    "schema":           
	       "{
		      \"type\": \"record\",
		      \"name\": \"test\",
		      \"fields\":
		        [ 
		          {
		            \"type\": \"string\",
		            \"name\": \"field1\"
		          },
		          {
		            \"type\": \"int\",
		            \"name\": \"field2\"
		          }
		        ]
		    }"
	  }

.. http:delete:: /subjects/(string: subject)/versions/(versionId: version)

   Deletes a specific version of the schema registered under this subject. This only deletes the version and the schema ID remains intact making it still possible to decode data using the schema ID. This API is recommended to be used only in development environments or under extreme circumstances where-in, its required to delete a previously registered schema for compatibility purposes or re-register previously registered schema.

   :param string subject: Name of the subject
   :param versionId version: Version of the schema to be deleted. Valid values for versionId are between [1,2^31-1] or the string "latest". "latest" deletes the last registered schema under the specified subject. Note that there may be a new latest schema that gets registered right after this request is served.

   :>json int: Version of the deleted schema

   :statuscode 404:
      * Error code 40401 -- Subject not found
      * Error code 40402 -- Version not found
   :statuscode 422:
      * Error code 42202 -- Invalid version
   :statuscode 500:
      * Error code 50001 -- Error in the backend data store

   **Example request**:

   .. sourcecode:: http

      DELETE /subjects/test/versions/1 HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      1

.. _heading2-4:

Compatibility
-------------

The compatibility resource allows the user to test schemas for compatibility against specific versions of a subject's schema.

.. http:post:: /compatibility/subjects/(string: subject)/versions/(versionId: version)

   Test input schema against a particular version of a subject's schema for compatibility. Note that the compatibility level applied for the check is the configured compatibility level for the subject (``http:get:: /config/(string: subject)``). If this subject's compatibility level was never changed, then the global compatibility level applies (``http:get:: /config``).

   :param string subject: Subject of the schema version against which compatibility is to be tested
   :param versionId version: Version of the subject's schema against which compatibility is to be tested. Valid values for versionId are between [1,2^31-1] or the string "latest". "latest" checks compatibility of the input schema with the last registered schema under the specified subject
    	
   :>json boolean is_compatible: True, if compatible. False otherwise
	
   :statuscode 404:
      * Error code 40401 -- Subject not found
      * Error code 40402 -- Version not found
   :statuscode 422: 
      * Error code 42201 -- Invalid Avro schema
      * Error code 42202 -- Invalid version
   :statuscode 500:
      * Error code 50001 -- Error in the backend data store

   **Example request**:

   .. sourcecode:: http

      POST /compatibility/subjects/test/versions/latest HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      { 
        "schema":
          "{
             \"type\": \"record\",
             \"name\": \"test\",
             \"fields\":
               [
                 {
                   \"type\": \"string\",
                   \"name\": \"field1\"
                 },
                 {
                   \"type\": \"int\",
                   \"name\": \"field2\"
                 }
               ]
           }"
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "is_compatible": true
      }


.. _heading2-5:

Config
------

The config resource allows you to inspect the cluster-level configuration values as well as subject overrides. 

.. http:put:: /config

   Update global compatibility level.

   When there are multiple instances of |sr| running in the same cluster, the update request will be forwarded to one of the instances designated as the master. If the master is not available, the client will get an error code indicating that the forwarding has failed.

   :<json string compatibility: New global compatibility level. Must be one of NONE, FULL, FORWARD, BACKWARD

   :statuscode 422: 
      * Error code 42203 -- Invalid compatibility level
   :statuscode 500:
      * Error code 50001 -- Error in the backend data store
      * Error code 50003 -- Error while forwarding the request to the master

   .. sourcecode:: http

      PUT /config HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      {
        "compatibility": "FULL",
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "compatibility": "FULL",
      }

.. http:get:: /config

   Get global compatibility level.

   :>json string compatibility: New global compatibility level. Will be one of NONE, FULL, FORWARD, BACKWARD

   :statuscode 500:
      * Error code 50001 -- Error in the backend data store

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

.. http:put:: /config/(string: subject)

   Update compatibility level for the specified subject.

   :param string subject: Name of the subject
   :<json string compatibility: New global compatibility level. Must be one of NONE, FULL, FORWARD, BACKWARD

   :statuscode 422: 
      * Error code 42203 -- Invalid compatibility level
   :statuscode 500:
      * Error code 50001 -- Error in the backend data store
      * Error code 50003 -- Error while forwarding the request to the master

   **Example request**:

   .. sourcecode:: http

      PUT /config/test HTTP/1.1
      Host: schemaregistry.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      {
        "compatibility": "FULL",
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "compatibility": "FULL",
      }

.. http:get:: /config/(string: subject)

   Get compatibility level for a subject.

   :param string subject: Name of the subject
   :>json string compatibility: New global compatibility level. Will be one of NONE, FULL, FORWARD, BACKWARD
  
   :statuscode 404: Subject not found
   :statuscode 500:
      * Error code 50001 -- Error in the backend data store

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
