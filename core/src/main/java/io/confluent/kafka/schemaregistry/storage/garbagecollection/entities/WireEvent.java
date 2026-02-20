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

package io.confluent.kafka.schemaregistry.storage.garbagecollection.entities;

import io.cloudevents.CloudEvent;

import java.util.Map;
import java.util.Objects;

public class WireEvent {
  private final Map<String, String> headers;
  private final CloudEvent cloudEvent;

  public WireEvent(Map<String, String> headers, CloudEvent cloudEvent) {
    this.headers = headers;
    this.cloudEvent = cloudEvent;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public CloudEvent getCloudEvent() {
    return cloudEvent;
  }

  public void validate() {
    if (cloudEvent == null) {
      throw new IllegalArgumentException("WireEvent missing required field: cloudEvent");
    }
    if (cloudEvent.getId() == null) {
      throw new IllegalArgumentException("CloudEvent missing required field: id");
    }
    if (cloudEvent.getType() == null) {
      throw new IllegalArgumentException("CloudEvent missing required field: type");
    }
    if (cloudEvent.getSubject() == null) {
      throw new IllegalArgumentException("CloudEvent missing required field: subject");
    }
    if (cloudEvent.getDataContentType() == null) {
      throw new IllegalArgumentException("CloudEvent missing required field: datacontenttype");
    }
    if (cloudEvent.getData() == null) {
      throw new IllegalArgumentException("CloudEvent missing required field: data");
    }
    if (cloudEvent.getTime() == null) {
      throw new IllegalArgumentException("CloudEvent missing required field: time");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WireEvent wireEvent = (WireEvent) o;
    return Objects.equals(headers, wireEvent.headers)
        && Objects.equals(cloudEvent, wireEvent.cloudEvent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(headers, cloudEvent);
  }

}