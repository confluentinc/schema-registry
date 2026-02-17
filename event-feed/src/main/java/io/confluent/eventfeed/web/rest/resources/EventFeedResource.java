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

package io.confluent.eventfeed.web.rest.resources;

import io.cloudevents.CloudEvent;
import io.confluent.eventfeed.client.rest.entities.SendResult;
import io.confluent.eventfeed.storage.EventFeedService;
import io.confluent.eventfeed.storage.exceptions.EventFeedUnsupportedCloudEventException;
import io.confluent.eventfeed.storage.exceptions.EventFeedStorageException;
import io.confluent.eventfeed.storage.exceptions.EventFeedStorageTimeoutException;
import io.confluent.eventfeed.web.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;
import io.confluent.kafka.schemaregistry.rest.resources.DocumentedName;
import io.confluent.kafka.schemaregistry.rest.resources.RequestHeaderBuilder;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.CompletionException;

@Path("/events")
@Singleton
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
        Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
        Versions.JSON_WEIGHTED})
public class EventFeedResource extends SchemaRegistryResource {
  private final EventFeedService eventFeedService;

  private static final Logger log = LoggerFactory.getLogger(EventFeedResource.class);
  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  @Inject
  public EventFeedResource(SchemaRegistry schemaRegistry, EventFeedService eventFeedService) {
    super(schemaRegistry);
    this.eventFeedService = eventFeedService;
  }

  @POST
  @Consumes({Versions.JSON, Versions.PROTOBUF})
  @Operation(summary = "Publish an external event to backing kafka for async ingestion.",
          responses = {
            @ApiResponse(responseCode="200", description = "The post event result",
            content = @Content(schema = @Schema(implementation = SendResult.class))),
  })
  @PerformanceMetric("events.publish")
  @DocumentedName("publishEvent")
  public void publishEvent(
          final @Suspended AsyncResponse asyncResponse,
          final @Context HttpHeaders headers,
          @Parameter(description = "The create request", required = true)
          @NotNull CloudEvent cloudEvent) {
    log.info("Received a request: {}", cloudEvent);
    eventFeedService.publishEventAsync(cloudEvent)
            .thenAccept(response -> {
              log.info("Event service: Event published: {}", response);
              SendResult sendResult = new SendResult();
              sendResult.setId(response.getId());
              asyncResponse.resume(sendResult);
            })
            .exceptionally(ex -> {
              Throwable cause =
                      ex instanceof CompletionException && ex.getCause() != null
                      ? ex.getCause() : ex;
              if (cause instanceof IllegalPropertyException) {
                asyncResponse.resume(Errors.invalidCloudEventException(cause.getMessage()));
              } else if (cause instanceof EventFeedUnsupportedCloudEventException) {
                asyncResponse.resume(Errors.unsupportedCloudEventException(cause.getMessage()));
              } else if (cause instanceof EventFeedStorageTimeoutException) {
                asyncResponse.resume(Errors.storeTimeoutException(cause.getMessage(), ex.getCause()));
              } else if (cause instanceof EventFeedStorageException) {
                asyncResponse.resume(Errors.storeException(cause.getMessage(), ex.getCause()));
              } else {
                asyncResponse.resume(Errors.eventFeedException(cause.getMessage(), ex.getCause()));
              }
              return null;
            });
  }


}
