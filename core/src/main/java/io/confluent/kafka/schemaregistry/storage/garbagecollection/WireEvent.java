package io.confluent.kafka.schemaregistry.storage.garbagecollection;

import io.cloudevents.CloudEvent;

import java.util.Map;

public class WireEvent {
  private final Map<String, String> headers;
  // A list of headers in the very outer envelop
  /*
  private final int ce_page;
  private final boolean ce_lastpage; // Used by topic snapshot
  private final int ce_total; // Used by cluster snapshot
  private final String _lsrc;
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

}