/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeGetResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownMasterException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidModeException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.Mode;
import io.confluent.kafka.schemaregistry.storage.ModeKeyAndValue;

@Path("/modes")
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
      @QueryParam("prefix") boolean prefix,
      @Context HttpHeaders headers,
      @NotNull ModeUpdateRequest request) {
    Mode mode = Enum.valueOf(Mode.class, request.getMode().toUpperCase(Locale.ROOT));
    if (mode == null) {
      throw new RestInvalidModeException();
    }
    try {
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(headers);
      schemaRegistry.setModeOrForward(subject, prefix, mode, headerProperties);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to update mode", e);
    } catch (OperationNotPermittedException e) {
      throw Errors.unknownMasterException("Failed to update mode", e);
    } catch (UnknownMasterException e) {
      throw Errors.unknownMasterException("Failed to update mode", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding update config request"
                                                    + " to the master", e);
    }

    return request;
  }

  @GET
  public List<ModeGetResponse> getModes() {
    return schemaRegistry.getModeKeyAndValues().stream()
        .map(m -> new ModeGetResponse(m.getSubject(), m.isPrefix(), m.getMode().name()))
        .collect(Collectors.toList());
  }

  @Path("/{subject}")
  @GET
  public ModeGetResponse getMode(@PathParam("subject") String subject) {
    ModeKeyAndValue modeKeyAndValue = schemaRegistry.getModeKeyAndValue(subject);
    return new ModeGetResponse(
        modeKeyAndValue.getSubject(),
        modeKeyAndValue.isPrefix(),
        modeKeyAndValue.getMode().name());
  }

  @Path("/{subject}")
  @DELETE
  public String deleteMode(
      @PathParam("subject") String subject,
      @QueryParam("prefix") boolean prefix,
      @Context HttpHeaders headers) {
    try {
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(headers);
      return schemaRegistry.deleteModeOrForward(subject, prefix, headerProperties);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Failed to delete mode", e);
    } catch (UnknownMasterException e) {
      throw Errors.unknownMasterException("Failed to delete mode", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding delete mode request"
          + " to the master", e);
    }
  }
}
