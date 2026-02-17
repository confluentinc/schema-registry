package io.confluent.eventfeed.storage.entities;

import io.cloudevents.CloudEvent;

import java.util.Objects;

public class ValidatedCloudEvent {
  private String partitionKey;
  private String topic;
  private CloudEvent cloudEvent;

  public ValidatedCloudEvent(String topic, String partitionKey, CloudEvent cloudEvent) {
    this.topic = topic;
    this.partitionKey = partitionKey;
    this.cloudEvent = cloudEvent;
  }

  public String  getPartitionKey() {
    return partitionKey;
  }

  public String getTopic() {
    return topic;
  }

  public CloudEvent getCloudEvent() {
    return cloudEvent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValidatedCloudEvent that = (ValidatedCloudEvent) o;
    return Objects.equals(partitionKey, that.partitionKey)
            && Objects.equals(topic, that.topic)
            && Objects.equals(cloudEvent, that.cloudEvent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionKey, topic, cloudEvent);
  }
}
