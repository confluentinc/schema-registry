/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.LookupFilter;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.tags.Tags;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/contexts")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class ContextsResource {

  public static final String apiTag = "Contexts (v1)";
  private static final Logger log = LoggerFactory.getLogger(ContextsResource.class);
  private final KafkaSchemaRegistry schemaRegistry;
  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public ContextsResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @GET
  @DocumentedName("getAllContexts")
  @Operation(summary = "List contexts",
      description = "Retrieves a list of contexts.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The contexts.", content = @Content(
            array = @ArraySchema(schema = @Schema(example = ".")))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store. ",
          content = @Content(schema = @Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  @PerformanceMetric("contexts.list")
  public List<String> listContexts(
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {
    try {
      limit = schemaRegistry.normalizeContextLimit(limit);
      List<String> contexts = schemaRegistry.listContexts();
      return contexts.stream()
        .skip(offset)
        .limit(limit)
        .collect(Collectors.toList());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Error while listing contexts", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while listing contexts", e);
    }
  }

  @DELETE
  @Path("/{context}")
  @DocumentedName("deleteContext")
  @Operation(summary = "Delete a context.",
      description = "Deletes the specified context if it is empty.",
      responses = {
          @ApiResponse(responseCode = "204", description = "No Content"),
          @ApiResponse(responseCode = "404",
              description = "Not Found. Error code 40401 indicates context not found.",
              content = @Content(schema =
              @io.swagger.v3.oas.annotations.media.Schema(implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "500",
              description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
              content = @Content(schema =
              @io.swagger.v3.oas.annotations.media.Schema(implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  @PerformanceMetric("context.delete")
  public void deleteContext(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the context", required = true)
      @PathParam("context") String delimitedContext) {
    log.debug("Deleting context {}", delimitedContext);

    delimitedContext = QualifiedSubject.normalize(schemaRegistry.tenant(), delimitedContext);

    Iterator<ExtendedSchema> schemas;
    try {
      schemas = schemaRegistry.getVersionsWithSubjectPrefix(
          delimitedContext, false, LookupFilter.INCLUDE_DELETED, false, null);
      if (schemas.hasNext()) {
        throw Errors.contextNotEmptyException(delimitedContext);
      }

      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      schemaRegistry.deleteContextOrForward(headerProperties, delimitedContext);
      asyncResponse.resume(Response.status(204).build());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting context", e);
    }
  }

}
