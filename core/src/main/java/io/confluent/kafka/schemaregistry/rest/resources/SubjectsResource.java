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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.ReferenceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.SubjectNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.LookupFilter;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.tags.Tags;
import java.util.HashMap;
import java.util.LinkedHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Path("/subjects")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SubjectsResource {

  public static final String apiTag = "Subjects (v1)";
  private static final Logger log = LoggerFactory.getLogger(SubjectsResource.class);
  private final KafkaSchemaRegistry schemaRegistry;
  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public SubjectsResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @POST
  @DocumentedName("lookUpSchemaUnderSubject")
  @Path("/{subject}")
  @Operation(summary = "Lookup schema under subject",
      description = "Check if a schema has already been registered under the specified subject."
      + " If so, this returns the schema string along with its globally unique identifier, its "
      + "version under this subject and the subject name.",
      responses = {
        @ApiResponse(responseCode = "200", description = "The schema.", content = @Content(schema =
          @io.swagger.v3.oas.annotations.media.Schema(implementation = Schema.class))),
        @ApiResponse(responseCode = "404",
          description = "Not Found. "
                  + "Error code 40401 indicates subject not found. "
                  + "Error code 40403 indicates schema not found.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  @PerformanceMetric("subjects.get-schema")
  public void lookUpSchemaUnderSubject(
      final @Suspended AsyncResponse asyncResponse,
      @Parameter(description = "Subject under which the schema will be registered", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Whether to normalize the given schema")
      @QueryParam("normalize") boolean normalize,
      @Parameter(description = "Desired output format, dependent on schema type")
      @DefaultValue("") @QueryParam("format") String format,
      @Parameter(description = "Whether to lookup deleted schemas")
      @QueryParam("deleted") boolean lookupDeletedSchema,
      @Parameter(description = "Schema", required = true)
      @NotNull RegisterSchemaRequest request) {
    log.debug("Schema lookup under subject {}, deleted {}, type {}",
             subject, lookupDeletedSchema, request.getSchemaType());

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);
    // returns version if the schema exists. Otherwise returns 404
    Schema schema = new Schema(subject, request);
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema matchingSchema;
    try {
      if (!normalize) {
        normalize = Boolean.TRUE.equals(schemaRegistry.getConfigInScope(subject).isNormalize());
      }
      matchingSchema = schemaRegistry.lookUpSchemaUnderSubjectUsingContexts(
          subject, schema, normalize, lookupDeletedSchema);

      // If first attempt failed with normalize=false, try again with normalize=true
      if (matchingSchema == null && !normalize) {
        log.debug("No matching schema found with normalize = false,"
                + " retrying with normalize = true");
        try {
          matchingSchema = schemaRegistry.lookUpSchemaUnderSubjectUsingContexts(
                subject, schema, true, lookupDeletedSchema);
        } catch (SchemaRegistryException e) {
          throw e;
        }
      }

      if (matchingSchema == null) {
        if (!schemaRegistry.hasSubjects(subject, lookupDeletedSchema)) {
          throw Errors.subjectNotFoundException(subject);
        } else {
          throw Errors.schemaNotFoundException();
        }
      }
      if (format != null && !format.trim().isEmpty()) {
        ParsedSchema parsedSchema = schemaRegistry.parseSchema(matchingSchema, false, false);
        matchingSchema.setSchema(parsedSchema.formattedString(format));
      }
    } catch (InvalidSchemaException e) {
      throw Errors.invalidSchemaException(e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while looking up schema under subject " + subject,
                                           e);
    }
    asyncResponse.resume(matchingSchema);
  }

  @GET
  @DocumentedName("getLatestWithMetadata")
  @Path("/{subject}/metadata")
  @Operation(summary = "Retrieve the latest version with the given metadata.",
      description = "Retrieve the latest version with the given metadata.",
      responses = {
          @ApiResponse(responseCode = "200", description = "The schema", content = @Content(schema =
          @io.swagger.v3.oas.annotations.media.Schema(implementation = Schema.class))),
          @ApiResponse(responseCode = "404", description = "Error code 40401 -- Subject not found\n"
              + "Error code 40403 -- Schema not found"),
          @ApiResponse(responseCode = "500", description = "Internal server error")
      })
  @PerformanceMetric("subjects.get-latest-with-metadata")
  public void getLatestWithMetadata(
      final @Suspended AsyncResponse asyncResponse,
      @Parameter(description = "Subject under which the schema will be registered", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "The metadata key")
      @QueryParam("key") List<String> keys,
      @Parameter(description = "The metadata value")
      @QueryParam("value") List<String> values,
      @Parameter(description = "Desired output format, dependent on schema type")
      @DefaultValue("") @QueryParam("format") String format,
      @Parameter(description = "Whether to lookup deleted schemas")
      @QueryParam("deleted") boolean lookupDeletedSchema) {
    log.info("Latest with metadata under subject {}, keys {}, values {}, deleted {}",
        subject, keys, values, lookupDeletedSchema);

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    // returns version if the schema exists. Otherwise returns 404
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema matchingSchema;
    Map<String, String> metadata = new HashMap<>();
    for (int i = 0; i < Math.min(keys.size(), values.size()); i++) {
      metadata.put(keys.get(i), values.get(i));
    }
    try {
      matchingSchema = schemaRegistry.getLatestWithMetadata(
          subject, metadata, lookupDeletedSchema);
      if (matchingSchema == null) {
        if (!schemaRegistry.hasSubjects(subject, lookupDeletedSchema)) {
          throw Errors.subjectNotFoundException(subject);
        } else {
          throw Errors.schemaNotFoundException();
        }
      }
      if (format != null && !format.trim().isEmpty()) {
        ParsedSchema parsedSchema = schemaRegistry.parseSchema(matchingSchema, false, false);
        matchingSchema.setSchema(parsedSchema.formattedString(format));
      }
    } catch (InvalidSchemaException e) {
      throw Errors.invalidSchemaException(e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while looking up schema under subject " + subject,
          e);
    }
    asyncResponse.resume(matchingSchema);
  }

  @GET
  @DocumentedName("getAllSubjects")
  @Valid
  @Operation(summary = "List subjects",
      description = "Retrieves a list of registered subjects matching specified parameters.",
      responses = {
        @ApiResponse(responseCode = "200",
          description = "List of subjects matching the specified parameters.", content = @Content(
                  array = @ArraySchema(schema = @io.swagger.v3.oas.annotations.media.Schema(
                          example = Schema.SUBJECT_EXAMPLE)))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class)))
      })
  @Tags(@Tag(name = apiTag))
  @PerformanceMetric("subjects.list")
  public Set<String> list(
      @DefaultValue(QualifiedSubject.CONTEXT_WILDCARD)
      @Parameter(description = "Subject name prefix")
      @QueryParam("subjectPrefix") String subjectPrefix,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit,
      @Parameter(description = "Whether to look up deleted subjects")
      @QueryParam("deleted") boolean lookupDeletedSubjects,
      @Parameter(description = "Whether to return deleted subjects only")
      @QueryParam("deletedOnly") boolean lookupDeletedOnlySubjects
  ) {
    LookupFilter filter = LookupFilter.DEFAULT;
    // if both deleted && deletedOnly are true, return deleted only
    if (lookupDeletedOnlySubjects) {
      filter = LookupFilter.DELETED_ONLY;
    } else if (lookupDeletedSubjects) {
      filter = LookupFilter.INCLUDE_DELETED;
    }
    try {
      Set<String> subjects = schemaRegistry.listSubjectsWithPrefix(
              subjectPrefix != null ? subjectPrefix : QualifiedSubject.CONTEXT_WILDCARD, filter);
      Stream<String> stream = subjects.stream();

      limit = schemaRegistry.normalizeSubjectLimit(limit);
      return stream
        .skip(offset)
        .limit(limit)
        .collect(Collectors.toCollection(LinkedHashSet::new)); // preserve order
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Error while listing subjects", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while listing subjects", e);
    }
  }

  @DELETE
  @DocumentedName("deleteSubject")
  @Path("/{subject}")
  @Operation(summary = "Delete subject",
      description = "Deletes the specified subject and its associated compatibility level if "
        + "registered. It is recommended to use this API only when a topic needs to be recycled or "
        + "in development environment.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Operation succeeded. "
          + "Returns list of schema versions deleted", content = @Content(array = @ArraySchema(
            schema = @io.swagger.v3.oas.annotations.media.Schema(type = "integer",
                    format = "int32", example = Schema.VERSION_EXAMPLE)))),
        @ApiResponse(responseCode = "404",
          description = "Not Found. Error code 40401 indicates subject not found.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  @PerformanceMetric("subjects.delete-subject")
  public void deleteSubject(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Whether to perform a permanent delete")
      @QueryParam("permanent") boolean permanentDelete) {
    log.debug("Deleting subject {}", subject);

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    List<Integer> deletedVersions;
    try {
      if (!schemaRegistry.hasSubjects(subject, true)) {
        throw Errors.subjectNotFoundException(subject);
      }
      if (!permanentDelete && !schemaRegistry.hasSubjects(subject, false)) {
        throw Errors.subjectSoftDeletedException(subject);
      }
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      deletedVersions = schemaRegistry.deleteSubjectOrForward(headerProperties,
              subject,
              permanentDelete);
    } catch (ReferenceExistsException e) {
      throw Errors.referenceExistsException(e.getMessage());
    } catch (SubjectNotSoftDeletedException e) {
      throw Errors.subjectNotSoftDeletedException(subject);
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Delete subject operation timed out", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting the subject " + subject,
                                           e);
    }
    asyncResponse.resume(deletedVersions);
  }

}
