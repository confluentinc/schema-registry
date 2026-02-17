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

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;

public class WireEvent {
  private final Map<String, String> headers;
  // A list of headers in the very outer envelop
  /*
  private final int ce_page;
  private final boolean ce_lastpage; // Used by topic snapshot
  private final int ce_total; // Used by cluster snapshot
  private final String _lsrc/ce_lsrc;
   */
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
    String id = cloudEvent.getId();
    String type = cloudEvent.getType();
    String subject = cloudEvent.getSubject();
    String contentType = cloudEvent.getDataContentType();
    byte[] data = cloudEvent.getData().toBytes();
    OffsetDateTime time = cloudEvent.getTime();
    if (id == null || type == null || subject == null || contentType == null || data == null || time == null) {
      throw new IllegalArgumentException("CloudEvent missing required fields: " +
              "id/type/subject/datacontenttype/data/time");
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