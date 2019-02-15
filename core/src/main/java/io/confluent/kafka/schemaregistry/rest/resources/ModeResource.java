/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import java.util.Locale;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeGetResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownMasterException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidModeException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.Mode;

@Path("/mode")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class ModeResource {

  private static final Logger log = LoggerFactory.getLogger(ModeResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public ModeResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Path("/{subject}")
  @PUT
  public ModeUpdateRequest updateMode(
      @PathParam("subject") String subject,
      @Context HttpHeaders headers,
      @NotNull ModeUpdateRequest request
  ) {
    Mode mode;
    try {
      mode = Enum.valueOf(Mode.class, request.getMode().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new RestInvalidModeException();
    }
    try {
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(headers);
      schemaRegistry.setModeOrForward(subject, mode, headerProperties);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update mode", e);
    } catch (UnknownMasterException e) {
      throw Errors.unknownMasterException("Failed to update mode", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding update mode request"
                                                    + " to the master", e);
    }

    return request;
  }

  @Path("/{subject}")
  @GET
  public ModeGetResponse getMode(@PathParam("subject") String subject) {
    try {
      Mode mode = schemaRegistry.getMode(subject);
      if (mode == null) {
        throw Errors.subjectNotFoundException();
      }
      return new ModeGetResponse(mode.name());
    } catch (SchemaRegistryException e) {
      throw Errors.storeException("Failed to get mode", e);
    }
  }

  @PUT
  public ModeUpdateRequest updateTopLevelMode(
      @Context HttpHeaders headers,
      @NotNull ModeUpdateRequest request) {
    return updateMode(null, headers, request);
  }

  @GET
  public ModeGetResponse getTopLevelMode() {
    return getMode(null);
  }
}
