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

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@Path("/schemas")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SchemasResource {

  private static final Logger log = LoggerFactory.getLogger(SchemasResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  public SchemasResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @GET
  @Operation(summary = "List schemas",
      description = "Get the schemas matching the specified parameters.",
      responses = {
        @ApiResponse(responseCode = "200",
          description = "The schemas matching the specified parameters", content = @Content(
              array = @ArraySchema(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = String.class)))),
        @ApiResponse(responseCode = "500",
            description = "Error code 50001 -- Error in the backend data store\n")
      })
  @PerformanceMetric("schemas.get-schemas")
  public List<Schema> getSchemas(
      @Parameter(description = "Filters results by the respective subject prefix")
      @DefaultValue("") @QueryParam("subjectPrefix") String subjectPrefix,
      @Parameter(description = "Whether to return soft deleted schemas")
      @DefaultValue("false") @QueryParam("deleted") boolean lookupDeletedSchema,
      @Parameter(description =
          "Whether to return latest schema versions only for each matching subject")
      @DefaultValue("false") @QueryParam("latestOnly") boolean latestOnly,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {
    Iterator<Schema> schemas;
    List<Schema> filteredSchemas = new ArrayList<>();
    String errorMessage = "Error while getting schemas for prefix " + subjectPrefix;
    try {
      schemas = schemaRegistry.getVersionsWithSubjectPrefix(
          subjectPrefix, lookupDeletedSchema, latestOnly);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    int fromIndex = offset;
    int toIndex = limit > 0 ? offset + limit : Integer.MAX_VALUE;
    int index = 0;
    while (schemas.hasNext() && index < toIndex) {
      Schema schema = schemas.next();
      if (index >= offset) {
        filteredSchemas.add(schema);
      }
      index++;
    }
    return filteredSchemas;
  }

  @GET
  @Path("/ids/{id}")
  @Operation(summary = "Get schema string by ID",
      description = "Retrieves the schema string identified by the input ID.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The schema string",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                implementation = SchemaString.class))),
        @ApiResponse(responseCode = "404", description = "Error code 40403 -- Schema not found\n"),
        @ApiResponse(responseCode = "500",
            description = "Error code 50001 -- Error in the backend data store\n")
      })
  @PerformanceMetric("schemas.ids.get-schema")
  public SchemaString getSchema(
      @Parameter(description = "Globally unique identifier of the schema", required = true)
      @PathParam("id") Integer id,
      @Parameter(description = "Name of the subject")
      @QueryParam("subject") String subject,
      @Parameter(description = "Desired output format, dependent on schema type")
      @DefaultValue("") @QueryParam("format") String format,
      @Parameter(description = "Whether to fetch the maximum schema identifier that exists")
      @DefaultValue("false") @QueryParam("fetchMaxId") boolean fetchMaxId) {
    SchemaString schema;
    String errorMessage = "Error while retrieving schema with id " + id + " from the schema "
                          + "registry";
    try {
      schema = schemaRegistry.get(id, subject, format, fetchMaxId);
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    if (schema == null) {
      throw Errors.schemaNotFoundException(id);
    }
    return schema;
  }

  @GET
  @Path("/ids/{id}/subjects")
  @Operation(summary = "List subjects associated to schema ID",
      description = "Retrieves all the subjects associated with a particular schema ID.",
      responses = {
        @ApiResponse(responseCode = "200",
            description = "The subjects matching the specified parameters", content = @Content(
                array = @ArraySchema(schema = @io.swagger.v3.oas.annotations.media.Schema(
                    implementation = String.class)))),
        @ApiResponse(responseCode = "404", description = "Error code 40403 -- Schema not found\n"),
        @ApiResponse(responseCode = "500",
            description = "Error code 50001 -- Error in the backend data store\n")
      })
  public Set<String> getSubjects(
      @Parameter(description = "Globally unique identifier of the schema", required = true)
      @PathParam("id") Integer id,
      @Parameter(description = "Filters results by the respective subject")
      @QueryParam("subject") String subject,
      @Parameter(description = "Whether to include subjects where the schema was deleted")
      @QueryParam("deleted") boolean lookupDeletedSchema) {
    Set<String> subjects;
    String errorMessage = "Error while retrieving all subjects associated with schema id "
        + id + " from the schema registry";

    try {
      subjects = schemaRegistry.listSubjectsForId(id, subject, lookupDeletedSchema);
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }

    if (subjects == null) {
      throw Errors.schemaNotFoundException();
    }

    return subjects;
  }

  @GET
  @Path("/ids/{id}/versions")
  @Operation(summary = "List subject-versions associated to schema ID",
      description = "Get all the subject-version pairs associated with the input ID.",
      responses = {
        @ApiResponse(responseCode = "200",
            description = "The subject versions matching the specified parameters",
            content = @Content(array = @ArraySchema(
                schema = @io.swagger.v3.oas.annotations.media.Schema(
                    implementation = SubjectVersion.class)))),
        @ApiResponse(responseCode = "404", description = "Error code 40403 -- Schema not "
            + "found\n"),
        @ApiResponse(responseCode = "500", description = "Error code 50001 -- Error in the "
            + "backend data store\n")
      })
  public List<SubjectVersion> getVersions(
      @Parameter(description = "Globally unique identifier of the schema", required = true)
      @PathParam("id") Integer id,
      @Parameter(description = "Filters results by the respective subject")
      @QueryParam("subject") String subject,
      @Parameter(description = "Whether to include subject versions where the schema was deleted")
      @QueryParam("deleted") boolean lookupDeletedSchema) {
    List<SubjectVersion> versions;
    String errorMessage = "Error while retrieving all subjects associated with schema id "
                          + id + " from the schema registry";

    try {
      versions = schemaRegistry.listVersionsForId(id, subject, lookupDeletedSchema);
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }

    if (versions == null) {
      throw Errors.schemaNotFoundException();
    }

    return versions;
  }

  @GET
  @Path("/types")
  @Operation(summary = "List supported schema types",
      description = "Retrieve the schema types supported by this registry.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The supported schema types",
            content = @Content(array = @ArraySchema(
                schema = @io.swagger.v3.oas.annotations.media.Schema(
                    implementation = String.class)))),
        @ApiResponse(responseCode = "500",
            description = "Error code 50001 -- Error in the backend data store\n")
      })
  public Set<String> getSchemaTypes() {
    return schemaRegistry.schemaTypes();
  }
}
