package io.confluent.kafka.schemaregistry.storage.garbagecollection.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.confluent.protobuf.events.catalog.v1.MetadataChange;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class GarbageCollectionEvent {
  // From cloud event
  private final String subject;
  private final String type;
  private final String id;
  private final long timestampMs;

  // From headers
  private final String snapshotId;
  private final Integer page;
  private final Boolean isLastPage;
  private final Integer totalPages;
  private final String tenant;

  private final MetadataChange metadataChange;


  public GarbageCollectionEvent(String subject,
                                String type,
                                String id,
                                String snapshotId,
                                Integer page,
                                Boolean isLastPage,
                                Integer totalPages,
                                String tenant,
                                long timestampMs,
                                MetadataChange metadataChange
                                ) {
    this.subject = subject;
    this.type = type;
    this.id = id;
    this.snapshotId = snapshotId;
    this.page = page;
    this.isLastPage = isLastPage;
    this.totalPages = totalPages;
    this.tenant = tenant;
    this.timestampMs = timestampMs;
    this.metadataChange = metadataChange;
  }

  public String getSubject() {
    return subject;
  }
  public String getType() {
    return type;
  }
  public String getId() {
    return id;
  }
  public String getTenant() {
    return tenant;
  }
  public MetadataChange getMetadataChange() {
    return metadataChange;
  }
  public long getTimestampMs() {
    return timestampMs;
  }
  public String getSnapshotId() {
    return snapshotId;
  }
  public Integer getPage() {
    return page;
  }
  public Integer getTotalPages() {
    return totalPages;
  }
  public Boolean isLastPage() {
    return isLastPage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GarbageCollectionEvent that = (GarbageCollectionEvent) o;
    return timestampMs == that.timestampMs
        && Objects.equals(subject, that.subject)
        && Objects.equals(type, that.type)
        && Objects.equals(id, that.id)
        && Objects.equals(snapshotId, that.snapshotId)
        && Objects.equals(page, that.page)
        && Objects.equals(isLastPage, that.isLastPage)
        && Objects.equals(totalPages, that.totalPages)
        && Objects.equals(tenant, that.tenant)
        && Objects.equals(metadataChange, that.metadataChange);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, type, id, timestampMs, snapshotId, page, 
        isLastPage, totalPages, tenant, metadataChange);
  }
}
