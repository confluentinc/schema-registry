.. backend

Kafka is used as |sr| storage backend.
The special Kafka topic ``<kafkastore.topic>`` (default ``_schemas``), with a single partition, is used as a highly available write ahead log.
All schemas, subject/version and ID metadata, and compatibility settings are appended as messages to this log.
A |sr| instance therefore both produces and consumes messages under the ``_schemas`` topic.
It produces messages to the log when, for example, new schemas are registered under a subject, or when updates to compatibility settings are registered.
|sr| consumes from the ``_schemas`` log in a background thread, and updates its local caches on consumption of each new ``_schemas`` message to reflect the newly added schema or compatibility setting.
Updating local state from the Kafka log in this manner ensures durability, ordering, and easy recoverability.
