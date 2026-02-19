/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
