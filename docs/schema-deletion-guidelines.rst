.. _schemaregistry_deletion:

Schema Deletion Guidelines
==========================

|sr| API supports deleting a specific schema version or all versions of a subject. The API only deletes the version and the underlying schema ID would still be available for any lookup.

.. sourcecode:: bash

    # Deletes all schema versions registered under the subject "Kafka-value"
      curl -X DELETE http://localhost:8081/subjects/Kafka-value
      [1]

    # Deletes version 1 of the schema registered under subject "Kafka-value"
      curl -X DELETE http://localhost:8081/subjects/Kafka-value/versions/1
      1

    # Deletes the most recently registered schema under subject "Kafka-value"
      curl -X DELETE http://localhost:8081/subjects/Kafka-value/versions/latest
      1

The above API's are primarily intended to be used be in development environment where it's common to go through iterations before finalizing a schema. While it's not recommended to be used in a production environment, there are few scenarios where these API's can be used in production but with utmost care.

- A new schema to be registered has compatibility issues with one of the existing schema versions
- An old version of the schema needs to be registered again for the same subject
- The schema's are used only in real-time streaming systems and the older version(s) are absolutely no longer required
- A topic needs to be recycled

It is also important to note that any registered compatibility settings for the subject would also be deleted while using Delete Subject or when you delete the only available schema version.
