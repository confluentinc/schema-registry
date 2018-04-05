.. _schemaregistry_monitoring:

Monitoring
----------

The |sr| reports a variety of metrics through JMX. It can also be configured to report
stats using additional pluggable stats reporters using the ``metrics.reporters`` configuration
option. The easiest way to view the available metrics is to use jconsole to
browse JMX MBeans.

The |sr| has two types of metrics. Global metrics help you monitor the overall health of
the service. Per-endpoint metrics monitor each API endpoint request method and are
prefixed by a name of the endpoint (e.g. ``subjects.get-schema``). These help you
understand how the proxy is being used and track down specific performance problems.

Global Metrics
~~~~~~~~~~~~~~

**MBean: kafka.schema.registry:type=jetty-metrics**

  ``connections-active``
    Total number of active TCP connections.

  ``connections-accepted-rate`` (deprecated since 2.0)
    * In 1.x: The average rate per second of accepted TCP connections.
    * In 2.x: Same as ``connections-opened-rate``.

  ``connections-opened-rate``
    The average rate per second of opened TCP connections.

  ``connections-closed-rate``
    The average rate per second of closed TCP connections.


**MBean: kafka.schema.registry:type=master-slave-role**

  ``master-slave-role``
    The current role of this |sr| instance. A value of 1 indicates this instance is
    the master, 0 indicates it is a slave.


Per-Endpoint Metrics
~~~~~~~~~~~~~~~~~~~~

The following are the metrics available for each endpoint request method. Metrics for all
requests are also aggregated into a global instance for each one. These aggregate instances have
no prefix in their name.

**MBean: kafka.schema.registry:type=jersey-metrics**

  ``<endpoint>.request-byte-rate``
    Bytes/second of incoming requests

  ``<endpoint>.request-error-rate``
    The average number of requests per second that resulted in HTTP error responses

  ``<endpoint>.request-latency-avg``
    The average request latency in ms

  ``<endpoint>.request-latency-max``
    The maximum request latency in ms

  ``<endpoint>.request-rate``
    The average number of HTTP requests per second.

  ``<endpoint>.request-size-avg``
    The average request size in bytes

  ``<endpoint>.request-size-max``
    The maximum request size in bytes

  ``<endpoint>.response-byte-rate``
    Bytes/second of outgoing responses

  ``<endpoint>.response-rate``
    The average number of HTTP responses per second.

  ``<endpoint>.response-size-avg``
    The average response size in bytes

  ``<endpoint>.response-size-max``
    The maximum response size in bytes


Endpoints
~~~~~~~~~

The following is a list of all the API endpoint methods. The naming should map intuitively to
each of the API operations. To create a full metric name, prefix a per-endpoint metric name with
one of these values. For example, to find the rate of ``GET /brokers`` API calls, combine the
endpoint name ``brokers.list`` with the metric name ``request-rate`` to get
``brokers.list.request-rate``.

  ========================================== =======================================================
  ``compatibility.subjects.versions.verify`` ``POST /compatibility/subjects/{subject}/versions/{version}``
  ``schemas.ids.get-schema``                 ``GET /schemas/ids/{id}``
  ``subjects.get-schema``                    ``POST /subjects/{subject}``
  ``subjects.list``                          ``GET /subjects``
  ``subjects.versions.get-schema``           ``GET /subjects/{subject}/versions/{version}``
  ``subjects.versions.get-schema.only``      ``GET /subjects/{subject}/versions/{version}/schema``
  ``subjects.versions.list``                 ``GET /subjects/{subject}/versions``
  ``subjects.versions.register``             ``POST /subjects/{subject}/versions``
  ========================================== =======================================================
