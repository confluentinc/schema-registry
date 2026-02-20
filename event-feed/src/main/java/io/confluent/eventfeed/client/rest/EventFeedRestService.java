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

package io.confluent.eventfeed.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.http.HttpMessageFactory;
import io.confluent.eventfeed.client.rest.entities.SendResult;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import jakarta.ws.rs.core.UriBuilder;
import org.apache.kafka.common.Configurable;

import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class EventFeedRestService extends RestService implements Configurable {

  private static final TypeReference<SendResult> SEND_RESULT_TYPE =
          new TypeReference<SendResult>() {
          };

  private static final String PARTITION_KEY_EXTENSION = "partitionkey";
  private static final String LSRC_EXTENSION = "lsrc";

  public EventFeedRestService(UrlList baseUrls) {
    super(baseUrls);
  }

  public EventFeedRestService(List<String> baseUrls) {
    super(baseUrls);
  }

  public EventFeedRestService(String baseUrlConfig) {
    super(baseUrlConfig);
  }

  public SendResult send(CloudEvent event) throws IOException, RestClientException {
    String id = event.getId();
    if (id == null) {
      throw new IllegalArgumentException("Missing required field in cloud event: id");
    }
    String contentType = event.getDataContentType();
    if (contentType == null) {
      throw new IllegalArgumentException("Missing required field in cloud event: datacontenttype");
    }
    String subject = event.getSubject();
    if (subject == null) {
      throw new IllegalArgumentException("Missing required field in cloud event: subject");
    }
    URI source = event.getSource();
    if (source == null) {
      throw new IllegalArgumentException("Missing required field in cloud event: source");
    }
    String type = event.getType();
    if (type == null) {
      throw new IllegalArgumentException("Missing required field in cloud event: type");
    }
    OffsetDateTime time = event.getTime();
    if (time == null) {
      throw new IllegalArgumentException("Missing required field in cloud event: time");
    }
    SpecVersion specVersion = event.getSpecVersion();
    if (specVersion != SpecVersion.V1) {
      throw new IllegalArgumentException("cloud event spec version must be: " + SpecVersion.V1);
    }
    Object partitionKey = event.getExtension(PARTITION_KEY_EXTENSION);
    if (partitionKey == null) {
      throw new IllegalArgumentException(
              "Missing required field in cloud event extensions: " + PARTITION_KEY_EXTENSION);
    }
    Object lsrc = event.getExtension(LSRC_EXTENSION);
    if (lsrc == null) {
      throw new IllegalArgumentException(
              "Missing required field in cloud event extensions: " + LSRC_EXTENSION);
    }

    Map<String, String> headers = new HashMap<>();
    AtomicReference<byte[]> bodyRef = new AtomicReference<>();
    try {
      HttpMessageFactory
              .createWriter(headers::put, bodyRef::set)
              .writeBinary(event);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to convert a cloud event to an http message.", e);
    }

    UriBuilder builder = UriBuilder.fromPath("/events");
    String path = builder.build().toString();

    return httpRequest(
            path, "POST",
            bodyRef.get(),
            headers,
            SEND_RESULT_TYPE);
  }
}
