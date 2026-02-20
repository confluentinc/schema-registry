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

package io.confluent.kafka.schemaregistry.storage.garbagecollection;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.GarbageCollectionEvent;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.WireEvent;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.serde.MetadataChangeDeserializer;
import io.confluent.protobuf.events.catalog.v1.MetadataChange;
import io.confluent.protobuf.events.catalog.v1.OpType;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;

public class GarbageCollectionEventFactory {
  private MetadataChangeDeserializer deserializer;

  // headers/cloudevent fields needed for processing
  public static final String LSRC_HEADER = "_lsrc";
  public static final String LSRC_EXTENSION = "lsrc";
  private static final String PAGE_EXTENSION = "page";
  private static final String PAGE_TOTAL_EXTENSION = "total";
  private static final String LAST_PAGE_EXTENSION = "lastpage";
  private static final String SNAPSHOT_ID_EXTENSION = "snapshotid";

  // data format
  private static final String PROTOBUF = "protobuf";
  private static final String JSON = "json";

  public GarbageCollectionEventFactory(MetadataChangeDeserializer  deserializer) {
    this.deserializer = deserializer;
  }

  private String getNonNullTenant(Map<String, String> headers, CloudEvent cloudEvent) {
    Object tenantObj = cloudEvent.getExtension(LSRC_EXTENSION);
    String tenant;
    if (tenantObj instanceof String) {
      tenant = (String) tenantObj;
    } else {
      tenant = headers.get(LSRC_HEADER);
    }
    if (tenant == null) {
      throw new IllegalArgumentException("Missing lsrc header in input event headers.");
    }
    return tenant;
  }

  private long getNonNullTimestamp(CloudEvent cloudEvent) {
    OffsetDateTime time = cloudEvent.getTime();
    if (time == null) {
      throw new IllegalArgumentException("Missing time in cloud event.");
    }
    long timestampMs;
    try {
      timestampMs = time.toInstant().toEpochMilli();
    } catch (Exception e) {
      throw new IllegalArgumentException("Can't parse cloud event timestamp to long.");
    }
    return timestampMs;
  }

  private MetadataChange deserAndValidateMetadataChange(CloudEvent cloudEvent) {
    // deserialize
    CloudEventData cloudEventData = cloudEvent.getData();
    if (cloudEventData == null) {
      throw new IllegalArgumentException(
              "Failed to create Garbage Collection Event: cloudEventData is null.");
    }
    String dataContentType = cloudEvent.getDataContentType();
    if (dataContentType == null) {
      throw new IllegalArgumentException("Missing dataContentType in cloud event.");
    }
    String format = null;
    if (dataContentType.contains(PROTOBUF)) {
      format = PROTOBUF;
    } else if (dataContentType.contains(JSON)) {
      format = JSON;
    }
    if (format == null) {
      throw new IllegalArgumentException("Unrecognized dataContentType in cloud event. "
              + "Received dataContentType: " + dataContentType);
    }
    byte[] data = cloudEventData.toBytes();
    MetadataChange metadataChange = deserializer.deserialize(data, format);

    // validate
    String id = cloudEvent.getId();
    String type = cloudEvent.getType();
    String subject = cloudEvent.getSubject();

    String source = metadataChange.getSource();
    if (source.isEmpty()) {
      throw new IllegalArgumentException(
              String.format("Metadata Change source must not be null for "
                      + "cloud event id={}, type={}, subject={}", id, type, subject));
    }
    OpType opType = metadataChange.getOp();
    if (opType == OpType.UNRECOGNIZED || opType == OpType.UNSPECIFIED) {
      throw new IllegalArgumentException(
              String.format("Metadata Change op must not be null for "
                      + "cloud event id={}, type={}, subject={}", id, type, subject));
    }
    return metadataChange;
  }

  private String getNonNullSnapshotId(CloudEvent cloudEvent) {
    Object snapshotIdObj = cloudEvent.getExtension(SNAPSHOT_ID_EXTENSION);
    String snapshotId = null;
    if (snapshotIdObj != null) {
      snapshotId = String.valueOf(snapshotIdObj);
    }
    String id = cloudEvent.getId();
    String type = cloudEvent.getType();
    String subject = cloudEvent.getSubject();
    if (snapshotId == null || snapshotId.isEmpty()) {
      throw new IllegalArgumentException(
              String.format("%s must be present in the snapshot extensions for cloud event "
                      + "id=%s, subject=%s, type=%s", SNAPSHOT_ID_EXTENSION, id, subject, type));
    }
    return snapshotId;
  }

  private int getNonNullPage(CloudEvent cloudEvent) {
    Object pageObj = cloudEvent.getExtension(PAGE_EXTENSION);
    Integer page = null;
    if (pageObj != null) {
      try {
        page = Integer.valueOf(pageObj.toString());
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse "
                + PAGE_EXTENSION + " in input event extensions.");
      }
    }
    String id = cloudEvent.getId();
    String type = cloudEvent.getType();
    String subject = cloudEvent.getSubject();
    if (page == null || page < 0) {
      throw new IllegalArgumentException(
              String.format("%s must be present in the snapshot extensions for cloud event "
                      + "id=%s, subject=%s, type=%s", PAGE_EXTENSION, id, subject, type));
    }
    return Objects.requireNonNull(page);
  }

  private Integer getTotalPageIfNotNull(CloudEvent cloudEvent) {
    Object totalPagesObj = cloudEvent.getExtension(PAGE_TOTAL_EXTENSION);
    Integer totalPages = null;
    String subject = cloudEvent.getSubject();
    String type = cloudEvent.getType();
    String id = cloudEvent.getId();
    if (totalPagesObj != null) {
      try {
        totalPages = Integer.valueOf(totalPagesObj.toString());
      } catch (Exception e) {
        throw new IllegalArgumentException(
                String.format("Failed to parse %s in input event extensions "
                                + "for cloud event: id=%s, subject=%s, type=%s",
                        PAGE_TOTAL_EXTENSION, id, subject, type));
      }
    }
    return totalPages;
  }

  private Boolean getIsLastPageIfNotNull(CloudEvent cloudEvent) {
    Object lastPageObj = cloudEvent.getExtension(LAST_PAGE_EXTENSION);
    Boolean isLastPage = null;
    String subject = cloudEvent.getSubject();
    String type = cloudEvent.getType();
    String id = cloudEvent.getId();
    if (lastPageObj != null) {
      try {
        isLastPage = Boolean.valueOf(lastPageObj.toString());
      } catch (Exception e) {
        throw new IllegalArgumentException(
                String.format("Failed to parse %s in input event extensions "
                        + "for cloud event: id=%s, subject=%s, type=%s",
                        LAST_PAGE_EXTENSION, id, subject, type));
      }
    }
    return isLastPage;
  }

  public GarbageCollectionEvent createFrom(WireEvent wireEvent) {
    if (wireEvent == null) {
      throw new IllegalArgumentException(
              "Failed to create Garbage Collection Event: wireEvent is null.");
    }

    Map<String, String> headers = wireEvent.getHeaders();
    CloudEvent cloudEvent = wireEvent.getCloudEvent();

    String subject = cloudEvent.getSubject();
    String type = cloudEvent.getType();
    String id = cloudEvent.getId();
    if (subject == null || type == null || id == null) {
      throw new IllegalArgumentException("Missing subject/type/id in cloud event.");
    }
    // Get tenant
    String tenant = getNonNullTenant(headers, cloudEvent);
    // Get timestamp
    long timestampMs = getNonNullTimestamp(cloudEvent);
    // Actual data
    MetadataChange metadataChange = deserAndValidateMetadataChange(cloudEvent);

    // Snapshot event must have snapshot related extensions
    String snapshotId = null;
    Integer page = null;
    Integer totalPages = null;
    Boolean isLastPage = null;
    if (metadataChange.getOp().equals(OpType.SNAPSHOT)) {
      if (cloudEvent.getExtensionNames().isEmpty()) {
        throw new IllegalArgumentException(
                String.format("cloud event extensions must not be empty for snapshot event "
                        + "from cloud event: id=%s, subject=%s, type=%s", id, subject, type));
      }

      snapshotId = getNonNullSnapshotId(cloudEvent);
      page = getNonNullPage(cloudEvent);
      totalPages = getTotalPageIfNotNull(cloudEvent);
      isLastPage = getIsLastPageIfNotNull(cloudEvent);
    }

    return new GarbageCollectionEvent(
            subject, type, id, snapshotId, page, isLastPage,
            totalPages, tenant, timestampMs, metadataChange);
  }
}
