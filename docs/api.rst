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

Although it is good practice to check the status code, you may safely parse the
response of any non-DELETE API calls and check for the presence of an
``error_code`` field to detect errors.

Subjects
--------

The topics resource provides information about the topics in your Kafka cluster and their current state. It also lets
you produce messages by making ``POST`` requests to specific topics.

.. http:get:: /subjects

   Get a list of Kafka topics. Each returned topic includes the topic name and
   some basic metadata.

   :>jsonarr string name: Name of the topic
   :>jsonarr int num_partitions: Number of partitions in the topic

   **Example request**:

   .. sourcecode:: http

      GET /topics HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      [
        {
          "name": "topic1",
          "num_partitions": 3
        },
        {
          "name": "topic2",
          "num_partitions": 1
        }
      ]

.. http:get:: /topics/(string:topic_name)

   Get metadata about a specific topic.

   :param string topic_name: Name of the topic to get metadata about

   :>json string name: Name of the topic
   :>json int num_partitions: Number of partitions in the topic

   **Example request**:

   .. sourcecode:: http

      GET /topics/test HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "name": "test",
        "num_partitions": 3
      }

.. http:post:: /topics/(string:topic_name)

   Produce messages to a topic, optionally specifying keys or partitions for the messages.

   :param string topic_name: Name of the topic to produce the messages to
   :<jsonarr string key: Base64-encoded message key or null (optional)
   :<jsonarr string value: Base64-encoded message value (required)
   :<jsonarr int partition: Partition to store the message in

   :>jsonarr int partition: Partition the message was published to
   :>jsonarr long offset: Offset of the message

   **Example request**:

   .. sourcecode:: http

      POST /topics/test HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      [
        {
          "key": "a2V5",
          "value": "Y29uZmx1ZW50"
        },
        {
          "value": "a2Fma2E=",
          "partition": "1"
        },
        {
          "value": "bG9ncw=="
        }
      ]

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      [
        {
          "partition": 3,
          "offset": 100
        },
        {
          "partition": 1,
          "offset": 101
        },
        {
          "partition": 2,
          "offset": 102
        }
      ]

Partitions
----------

The partitions resource provides per-partition metadata, including the current leaders and replicas for each partition.
It also allows you to produce messages to single partition using ``POST`` requests.

.. http:get:: /topics/{topic_name}/partitions

   Get a list of partitions for the topic.

   :param string topic_name: the name of the topic

   :>jsonarr int partition: ID of the partition
   :>jsonarr int leader: Broker ID of the leader for this partition
   :>jsonarr array replicas: List of brokers acting as replicas for this partition
   :>jsonarr int replicas[i].broker: Broker ID of the replica
   :>jsonarr boolean replicas[i].leader: true if this broker is the leader for the partition
   :>jsonarr boolean replicas[i].in_sync: true if the replica is in sync with the leader

   **Example request**:

   .. sourcecode:: http

      GET /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      [
        {
          "partition": 1,
          "leader": 1,
          "replicas": [
            {
              "broker": 1,
              "leader": true,
              "in_sync": true,
            },
            {
              "broker": 2,
              "leader": false,
              "in_sync": true,
            },
            {
              "broker": 3,
              "leader": false,
              "in_sync": false,
            }
          ]
        },
        {
          "partition": 2,
          "leader": 2,
          "replicas": [
            {
              "broker": 1,
              "leader": false,
              "in_sync": true,
            },
            {
              "broker": 2,
              "leader": true,
              "in_sync": true,
            },
            {
              "broker": 3,
              "leader": false,
              "in_sync": false,
            }
          ]
        }
      ]


.. http:get:: /topics/(string:topic_name)/partitions/(int:partition_id)

   Get metadata about a single partition in the topic.

   :param string topic_name: Name of the topic
   :param int partition_id: ID of the partition to inspect

   :>json int partition: ID of the partition
   :>json int leader: Broker ID of the leader for this partition
   :>json array replicas: List of brokers acting as replicas for this partition
   :>json int replicas[i].broker: Broker ID of the replica
   :>json boolean replicas[i].leader: true if this broker is the leader for the partition
   :>json boolean replicas[i].in_sync: true if the replica is in sync with the leader

   **Example request**:

   .. sourcecode:: http

      GET /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "partition": 1,
        "leader": 1,
        "replicas": [
          {
            "broker": 1,
            "leader": true,
            "in_sync": true,
          },
          {
            "broker": 2,
            "leader": false,
            "in_sync": true,
          },
          {
            "broker": 3,
            "leader": false,
            "in_sync": false,
          }
        ]
      }

.. http:post:: /topics/(string:topic_name)/partitions/(int:partition_id)

   Produce messages to one partition of the topic.

   :param string topic_name: Topic to produce the messages to
   :param int partition_id: Partition to produce the messages to
   :<jsonarr string key: Base64-encoded message key or null (optional)
   :<jsonarr string value: Base64-encoded message value (required)

   :>jsonarr int partition: Partition the message was published to
   :>jsonarr long offset: Offset of the published message

   **Example request**:

   .. sourcecode:: http

      POST /topics/test/partitions/1 HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      [
        {
          "key": "a2V5",
          "value": "Y29uZmx1ZW50"
        },
        {
          "value": "a2Fma2E="
        }
      ]

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      [
        {
          "partition": 1,
          "offset": 100,
        },
        {
          "partition": 1,
          "offset": 101,
        }
      ]


Consumers
---------

The consumers resource provides access to the current state of consumer groups, allows you to create a consumer in a
consumer group and consume messages from topics and partitions.

Because consumers are stateful, any consumer instances created with the REST API are tied to a specific REST proxy
instance. A full URL is provided when the instance is created and it should be used to construct any subsequent
requests. Failing to use the returned URL for future consume requests will end up adding new
consumers to the group. If a REST proxy instance is shutdown, it will attempt to cleanly destroy
any consumers before it is terminated.

.. http:post:: /consumers/(string:group_name)

   Create a new consumer instance in the consumer group.

   Note that the response includes a URL including the host since the consumer is stateful and tied
   to a specific REST proxy instance. Subsequent examples in this section use a ``Host`` header
   for this specific REST proxy instance.

   :param string group_name: The name of the consumer group to join
   :<json string id: Unique ID for the consumer instance in this group. If omitted, one will be automatically generated
                     using the REST proxy ID and an auto-incrementing number
   :<json string auto.offset.reset: Sets the ``auto.offset.reset`` setting for the consumer
   :<json string auto.commit.enable: Sets the ``auto.commit.enable`` setting for the consumer

   :>json string instance_id: Unique ID for the consumer instance in this group. If provided in the initial request,
                              this will be identical to ``id``.
   :>json string base_uri: Base URI used to construct URIs for subsequent requests against this consumer instance. This
                           will be of the form ``http://hostname:port/consumers/consumer_group/instances/instance_id``.

   .. sourcecode:: http

      POST /consumers/testgroup/ HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

      {
        "id": "my_consumer",
        "auto.offset.reset": "true",
        "auto.commit.enable": "false"
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "instance_id": "my_consumer",
        "base_uri": "http://proxy-instance.kafkaproxy.example.com/consumers/testgroup/instances/my_consumer"
      }

.. http:post:: /consumers/(string:group_name)/instances/(string:instance)

   Commit offsets for the consumer. Returns a list of the partitions with the committed offsets.

   The body of this request is empty. The offsets are determined by the current state of the consumer instance on the
   proxy. The returned state includes both ``consumed`` and ``committed`` offsets. After a successful commit, these
   should be identical; however, both are included so the output format is consistent with other API calls that return
   the offsets.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   :>jsonarr string topic: Name of the topic for which an offset was committed
   :>jsonarr int partition: Partition ID for which an offset was committed
   :>jsonarr long consumed: The offset of the most recently consumed message
   :>jsonarr long committed: The committed offset value. If the commit was successful, this should be identical to
                             ``consumed``.

   **Example request**:

   .. sourcecode:: http

      POST /consumers/testgroup/instances/my_consumer HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      [
        {
          "topic": "test",
          "partition": 1,
          "consumed": 100,
          "committed": 100
        },
        {
          "topic": "test",
          "partition": 2,
          "consumed": 200,
          "committed": 200
        },
        {
          "topic": "test2",
          "partition": 1,
          "consumed": 50,
          "committed": 50
        }
      ]

.. http:delete:: /consumers/(string:group_name)/instances/(string:instance)

   Destroy the consumer instance.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance

   **Example request**:

   .. sourcecode:: http

      DELETE /consumers/testgroup/instances/my_consumer HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 204 No Content

.. http:get:: /consumers/(string:group_name)/instances/(string:instance)/topics/(string:topic_name)

   Consume messages from a topic. If the consumer is not yet subscribed to the topic, this adds them as a subscriber,
   possibly causing a consumer rebalance.

   Note that this request *must* be made to the specific REST proxy instance holding the consumer
   instance.

   :param string group_name: The name of the consumer group
   :param string instance: The ID of the consumer instance
   :param string topic_name: The topic to consume messages from.

   :>jsonarr string key: Base64-encoded message key or null
   :>jsonarr string value: Base64-encoded message value
   :>jsonarr int partition: Partition of the message
   :>jsonarr long offset: Offset of the message

   **Example request**:

   .. sourcecode:: http

      GET /consumers/testgroup/instances/my_consumer/topics/test_topic HTTP/1.1
      Host: proxy-instance.kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      [
        {
          "topic": "test",
          "partition": 1,
          "consumed": 100,
          "committed": 100
        },
        {
          "topic": "test",
          "partition": 2,
          "consumed": 200,
          "committed": 200
        },
        {
          "topic": "test2",
          "partition": 1,
          "consumed": 50,
          "committed": 50
        }
      ]


Brokers
-------

The brokers resource provides access to the current state of Kafka brokers in the cluster.

.. http:get:: /brokers

   Get a list of brokers.

   :>json array brokers: List of broker IDs

   **Example request**:

   .. sourcecode:: http

      GET /brokers HTTP/1.1
      Host: kafkaproxy.example.com
      Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/vnd.schemaregistry.v1+json

      {
        "brokers": [1, 2, 3]
      }

