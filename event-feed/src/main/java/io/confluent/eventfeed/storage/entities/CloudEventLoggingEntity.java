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

import java.time.OffsetDateTime;

public class CloudEventLoggingEntity {
  private static final String PARTITION_KEY_EXT = "partitionkey";
  private static final String LSRC_EXT = "lsrc";

  private final String id;
  private final String type;
  private final String source;
  private final String subject;
  private final OffsetDateTime time;
  private final String lsrc;
  private final String partitionkey;

  public CloudEventLoggingEntity(String id, String type, String source, String subject,
                                 OffsetDateTime time, String lsrc, String partitionkey) {
    this.id = id;
    this.type = type;
    this.source = source;
    this.subject = subject;
    this.time = time;
    this.lsrc = lsrc;
    this.partitionkey = partitionkey;
  }

  public static CloudEventLoggingEntity of(CloudEvent event) {
    if (event == null) {
      return null;
    }
    String sourceStr = event.getSource() != null ? event.getSource().toString() : null;
    Object lsrcObj = event.getExtension(LSRC_EXT);
    Object partitionKeyObj = event.getExtension(PARTITION_KEY_EXT);
    return new CloudEventLoggingEntity(
            event.getId(),
            event.getType(),
            sourceStr,
            event.getSubject(),
            event.getTime(),
            lsrcObj != null ? lsrcObj.toString() : null,
            partitionKeyObj != null ? partitionKeyObj.toString() : null);
  }

  @Override
  public String toString() {
    return "id=" + id
            + ", type=" + type
            + ", source=" + source
            + ", subject=" + subject
            + ", time=" + time
            + ", lsrc=" + lsrc
            + ", partitionkey=" + partitionkey;
  }
}
