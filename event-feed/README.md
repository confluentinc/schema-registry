1. Event feed receives request in CloudEvent data format. 
2. Event feed service currently only support topic delta events or snapshot events.
3. CloudEvent data format (bare minimum):
CloudEvent over http
- Content-Type: application/json, or application/protobuf.
- ce-id: required by CloudEvent serdes.
- ce-subject: ENUM. topic, or topicAndClusterLink. Required by server.
- ce-partitionkey: kafka cluster id. Required by server.
- ce-specversion: 1.0. Required by CloudEvent serdes.
- ce-source: Confluent crn. Required by CloudEvent serdes.
- ce-type: ENUM. DELTA/SNAPSHOT. Required by CloudEvent serdes.