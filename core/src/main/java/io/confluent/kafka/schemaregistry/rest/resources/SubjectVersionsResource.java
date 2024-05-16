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
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.IdDoesNotMatchException;
import io.confluent.kafka.schemaregistry.exceptions.IncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.ReferenceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaTooLargeException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaVersionNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestInvalidRuleSetException;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.LookupFilter;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.tags.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Path("/subjects/{subject}/versions")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SubjectVersionsResource {

  public static final String apiTag = SubjectsResource.apiTag;
  private static final Logger log = LoggerFactory.getLogger(SubjectVersionsResource.class);

  private final KafkaSchemaRegistry schemaRegistry;

  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  private static final String VERSION_PARAM_DESC = "Version of the schema to be returned. "
      + "Valid values for versionId are between [1,2^31-1] or the string \"latest\". \"latest\" "
      + "returns the last registered schema under the specified subject. Note that there may be a "
      + "new latest schema that gets registered right after this request is served.";

  public SubjectVersionsResource(KafkaSchemaRegistry registry) {
    this.schemaRegistry = registry;
  }

  @GET
  @Path("/{version}")
  @DocumentedName("getSchemaByVersion")
  @PerformanceMetric("subjects.versions.get-schema")
  @Operation(summary = "Get schema by version",
      description = "Retrieves a specific version of the schema registered under this subject.",
      responses = {
          @ApiResponse(responseCode = "200", description = "The schema.",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                    implementation = Schema.class))),
          @ApiResponse(responseCode = "404",
            description = "Not Found. "
                    + "Error code 40401 indicates subject not found. "
                    + "Error code 40402 indicates version not found.",
              content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "422",
            description = "Unprocessable Entity. Error code 42202 indicates an invalid version.",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                    implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "500",
            description = "Internal Server Error. "
                    + "Error code 50001 indicates a failure in the backend data store.",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                    implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public Schema getSchemaByVersion(
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = VERSION_PARAM_DESC, required = true)
      @PathParam("version") String version,
      @Parameter(description = "Whether to include deleted schema")
      @QueryParam("deleted") boolean lookupDeletedSchema,
      @Parameter(description = "Find tagged entities for the given tags or * for all tags")
      @QueryParam("findTags") List<String> tags) {

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }
    Schema schema;
    String errorMessage = "Error while retrieving schema for subject "
          + subject
          + " with version "
          + version
          + " from the schema registry";
    try {
      schema = schemaRegistry.getUsingContexts(
          subject, versionId.getVersionId(), lookupDeletedSchema);
      if (schema == null) {
        if (!schemaRegistry.hasSubjects(subject, lookupDeletedSchema)) {
          throw Errors.subjectNotFoundException(subject);
        } else {
          throw Errors.versionNotFoundException(versionId.getVersionId());
        }
      }
      if (tags != null && !tags.isEmpty()) {
        schemaRegistry.extractSchemaTags(schema, tags);
      }
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    return schema;
  }

  @GET
  @Path("/{version}/schema")
  @DocumentedName("getVersionSchemaOnly")
  @PerformanceMetric("subjects.versions.get-schema.only")
  @Operation(summary = "Get schema string by version",
      description = "Retrieves the schema for the specified version of this subject. "
        + "Only the unescaped schema string is returned.",
      responses = {
          @ApiResponse(responseCode = "200", description = "The schema string.", content = @Content(
              schema = @io.swagger.v3.oas.annotations.media.Schema(example =
                      Schema.SCHEMA_EXAMPLE))),
          @ApiResponse(responseCode = "404",
            description = "Not Found. "
                    + "Error code 40401 indicates subject not found. "
                    + "Error code 40402 indicates version not found.",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                    implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "422",
            description = "Unprocessable Entity. Error code 42202 indicates an invalid version.",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                    implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "500",
            description = "Internal Server Error. "
                    + "Error code 50001 indicates a failure in the backend data store.",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                    ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public String getSchemaOnly(
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = VERSION_PARAM_DESC, required = true)
      @PathParam("version") String version,
      @Parameter(description = "Whether to include deleted schema")
      @QueryParam("deleted") boolean lookupDeletedSchema) {
    return getSchemaByVersion(subject, version, lookupDeletedSchema, null).getSchema();
  }

  @GET
  @Path("/{version}/referencedby")
  @DocumentedName("getReferencedBy")
  @Operation(summary = "List schemas referencing a schema",
      description = "Retrieves the IDs of schemas that reference the specified schema.",
      responses = {
        @ApiResponse(responseCode = "200",
          description = "List of IDs for schemas that reference the specified schema.",
          content = @Content(array = @ArraySchema(
              schema = @io.swagger.v3.oas.annotations.media.Schema(type = "integer",
                      format = "int32", example = Schema.ID_EXAMPLE)))),
        @ApiResponse(responseCode = "404",
          description = "Not Found. "
                  + "Error code 40401 indicates subject not found. "
                  + "Error code 40402 indicates version not found.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "422",
          description = "Unprocessable Entity. Error code 42202 indicates an invalid version.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public List<Integer> getReferencedBy(
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = VERSION_PARAM_DESC, required = true)
      @PathParam("version") String version) {

    Schema schema = getSchemaByVersion(subject, version, true, null);
    if (schema == null) {
      return new ArrayList<>();
    }
    VersionId versionId = null;
    try {
      versionId = new VersionId(schema.getVersion());
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }
    String errorMessage = "Error while retrieving schemas that reference schema with subject "
        + subject
        + " and version "
        + version
        + " from the schema registry";
    try {
      return schemaRegistry.getReferencedBy(schema.getSubject(), versionId);
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
  }

  @GET
  @DocumentedName("getAllVersions")
  @PerformanceMetric("subjects.versions.list")
  @Operation(summary = "List versions under subject",
      description = "Retrieves a list of versions registered under the specified subject.",
      responses = {
          @ApiResponse(responseCode = "200",
              description = "List of version numbers matching the specified parameters.",
              content = @Content(array = @ArraySchema(
                  schema = @io.swagger.v3.oas.annotations.media.Schema(type = "integer",
                      format = "int32", example = Schema.VERSION_EXAMPLE)))),
          @ApiResponse(responseCode = "404",
            description = "Not Found. "
                    + "Error code 40401 indicates subject not found. ",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                    implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "500",
            description = "Internal Server Error. "
                    + "Error code 50001 indicates a failure in the backend data store.",
            content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                    ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public List<Integer> listVersions(
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Whether to include deleted schemas")
      @QueryParam("deleted") boolean lookupDeletedSchema,
      @Parameter(description = "Whether to return deleted schemas only")
      @QueryParam("deletedOnly") boolean lookupDeletedOnlySchema) {

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    // check if subject exists. If not, throw 404
    Iterator<SchemaKey> resultSchemas;
    List<Integer> allVersions = new ArrayList<>();
    String errorMessage = "Error while validating that subject "
                          + subject
                          + " exists in the registry";
    try {
      if (!schemaRegistry.hasSubjects(subject, lookupDeletedSchema || lookupDeletedOnlySchema)) {
        throw Errors.subjectNotFoundException(subject);
      }
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    errorMessage = "Error while listing all versions for subject "
                   + subject;
    LookupFilter filter = LookupFilter.DEFAULT;
    // if both deleted && deletedOnly are true, return deleted only
    if (lookupDeletedOnlySchema) {
      filter = LookupFilter.DELETED_ONLY;
    } else if (lookupDeletedSchema) {
      filter = LookupFilter.INCLUDE_DELETED;
    }
    try {
      resultSchemas = schemaRegistry.getAllVersions(subject, filter);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    while (resultSchemas.hasNext()) {
      SchemaKey schema = resultSchemas.next();
      allVersions.add(schema.getVersion());
    }
    return allVersions;
  }

  @POST
  @DocumentedName("registerSchema")
  @PerformanceMetric("subjects.versions.register")
  @Operation(summary = "Register schema under a subject",
      description = "Register a new schema under the specified subject. If successfully "
        + "registered, this returns the unique identifier of this schema in the registry. The "
        + "returned identifier should be used to retrieve this schema from the schemas resource "
        + "and is different from the schema's version which is associated with the subject. If the "
        + "same schema is registered under a different subject, the same identifier will be "
        + "returned. However, the version of the schema may be different under different subjects."
        + "\nA schema should be compatible with the previously registered schema or schemas (if "
        + "there are any) as per the configured compatibility level. The configured compatibility "
        + "level can be obtained by issuing a GET http:get:: /config/(string: subject). If that "
        + "returns null, then GET http:get:: /config\n"
        + "When there are multiple instances of Schema Registry running in the same cluster, the "
        + "schema registration request will be forwarded to one of the instances designated as "
        + "the primary. If the primary is not available, the client will get an error code "
        + "indicating that the forwarding has failed.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Schema successfully registered.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  RegisterSchemaResponse.class))),
        @ApiResponse(responseCode = "409", description = "Conflict. Incompatible schema.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class))),
        @ApiResponse(responseCode = "422",
          description = "Unprocessable entity. "
                  + "Error code 42201 indicates an invalid schema or schema type. ",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store."
                  + "Error code 50002 indicates operation timed out. "
                  + "Error code 50003 indicates a failure forwarding the request to the primary.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public void register(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subjectName,
      @Parameter(description = "Whether to normalize the given schema")
      @QueryParam("normalize") boolean normalize,
      @Parameter(description = "Schema", required = true)
      @NotNull RegisterSchemaRequest request) {
    log.info("Registering new schema: subject {}, version {}, id {}, type {}, schema size {}",
             subjectName, request.getVersion(), request.getId(), request.getSchemaType(),
            request.getSchema() == null ? 0 : request.getSchema().length());

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());

    try {
      schemaRegistry.getCompositeUpdateRequestHandler().handle(
          subjectName, normalize, request, headerProperties);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while registering schema", e);
    }

    if (request.getRuleSet() != null) {
      try {
        request.getRuleSet().validate();
      } catch (RuleException e) {
        throw new RestInvalidRuleSetException(e.getMessage());
      }
    }
    if (subjectName != null
        && !QualifiedSubject.isValidSubject(schemaRegistry.tenant(), subjectName)) {
      throw Errors.invalidSubjectException(subjectName);
    }

    subjectName = QualifiedSubject.normalize(schemaRegistry.tenant(), subjectName);

    RegisterSchemaResponse registerSchemaResponse;
    try {
      if (!normalize) {
        normalize = Boolean.TRUE.equals(schemaRegistry.getConfigInScope(subjectName).isNormalize());
      }
      Schema result =
          schemaRegistry.registerOrForward(subjectName, request, normalize, headerProperties);
      registerSchemaResponse = new RegisterSchemaResponse(result);
    } catch (IdDoesNotMatchException e) {
      throw Errors.idDoesNotMatchException(e);
    } catch (InvalidSchemaException e) {
      throw Errors.invalidSchemaException(e);
    } catch (SchemaTooLargeException e) {
      throw Errors.schemaTooLargeException("Register operation failed because schema is too large");
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Register operation timed out", e);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Register schema operation failed while writing"
                                  + " to the Kafka store", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding register schema request"
                                                    + " to the leader", e);
    } catch (IncompatibleSchemaException e) {
      throw Errors.incompatibleSchemaException("Schema being registered is incompatible with"
              + " an earlier schema for subject \"" + subjectName + "\", details: "
              + e.getMessage(), e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while registering schema", e);
    }
    asyncResponse.resume(registerSchemaResponse);
  }

  @DELETE
  @Path("/{version}")
  @DocumentedName("deleteSchemaVersion")
  @PerformanceMetric("subjects.versions.deleteSchemaVersion-schema")
  @Operation(summary = "Delete schema version",
      description = "Deletes a specific version of the schema registered under this subject. "
        + "This only deletes the version and the schema ID remains intact making it still possible "
        + "to decode data using the schema ID. This API is recommended to be used only in "
        + "development environments or under extreme circumstances where-in, its required to "
        + "delete a previously registered schema for compatibility purposes or re-register "
        + "previously registered schema.",
      responses = {
        @ApiResponse(responseCode = "200", description = "Operation succeeded. "
          + "Returns the schema version.", content = @Content(schema =
            @io.swagger.v3.oas.annotations.media.Schema(type = "integer",
                    format = "int32",
                    example = Schema.VERSION_EXAMPLE))),
        @ApiResponse(responseCode = "404",
          description = "Not Found. "
                  + "Error code 40401 indicates subject not found. "
                  + "Error code 40402 indicates version not found.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "422",
          description = "Unprocessable Entity. Error code 42202 indicates an invalid version.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class))),
        @ApiResponse(responseCode = "500",
          description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
          content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(implementation =
                  ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public void deleteSchemaVersion(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = VERSION_PARAM_DESC, required = true)
      @PathParam("version") String version,
      @Parameter(description = "Whether to perform a permanent delete")
      @QueryParam("permanent") boolean permanentDelete) {
    log.debug("Deleting schema version {} from subject {}", version, subject);

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }
    Schema schema;
    String errorMessage =
            "Error while retrieving schema for subject "
                    + subject
                    + " with version "
                    + version
                    + " from the schema registry";
    try {
      if (schemaRegistry.schemaVersionExists(subject, versionId, true)) {
        if (!permanentDelete
                && !schemaRegistry.schemaVersionExists(subject,
                        versionId, false)) {
          throw Errors.schemaVersionSoftDeletedException(subject, version);
        }
      }
      schema = schemaRegistry.get(subject, versionId.getVersionId(), true);
      if (schema == null) {
        if (!schemaRegistry.hasSubjects(subject, true)) {
          throw Errors.subjectNotFoundException(subject);
        } else {
          throw Errors.versionNotFoundException(versionId.getVersionId());
        }
      }
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }

    try {
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
              headers, schemaRegistry.config().whitelistHeaders());
      schemaRegistry.deleteSchemaVersionOrForward(headerProperties, subject,
              schema, permanentDelete);
    } catch (SchemaVersionNotSoftDeletedException e) {
      throw Errors.schemaVersionNotSoftDeletedException(e.getSubject(),
              e.getVersion());
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Delete Schema Version operation timed out", e);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Delete Schema Version operation failed while writing"
                                  + " to the Kafka store", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors
          .requestForwardingFailedException("Error while forwarding delete schema version request"
                                            + " to the leader", e);
    } catch (ReferenceExistsException e) {
      throw Errors.referenceExistsException(e.getMessage());
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting Schema Version", e);
    }

    asyncResponse.resume(schema.getVersion());
  }

  @POST
  @Path("/{version}/tags")
  @DocumentedName("modifySchemaTags")
  @PerformanceMetric("subjects.versions.modify.tags")
  @Operation(summary = "Create or remove schema embedded tags for a subject version",
      description = "Create new schema subject version with the provided field or record level "
          + "schema tags embedded (or remove) in the schema string, as well as the given metadata "
          + "and ruleset.",
      responses = {
          @ApiResponse(responseCode = "200", description = "Schema successfully registered.",
              content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = RegisterSchemaResponse.class))),
          @ApiResponse(responseCode = "409", description = "Conflict. Incompatible schema.",
              content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "422",
              description = "Unprocessable entity. "
                  + "Error code 42201 indicates an invalid schema or schema type. ",
              content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "500",
              description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store."
                  + "Error code 50002 indicates operation timed out. "
                  + "Error code 50003 indicates a failure forwarding the request to the primary.",
              content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public void modifyTags(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subjectName,
      @Parameter(description = VERSION_PARAM_DESC, required = true)
      @PathParam("version") String version,
      @Parameter(description = "Tag schema request", required = true)
      @NotNull TagSchemaRequest request) {
    log.info("Modifying schema tags: subject {}, version {}, newVersion {}, "
            + "adding {} tags, removing {} tags",
        subjectName, version, request.getNewVersion(),
        request.getTagsToAdd() == null ? 0 : request.getTagsToAdd().size(),
        request.getTagsToRemove() == null ? 0 : request.getTagsToRemove().size());

    // get schema by subject version
    if (!QualifiedSubject.isValidSubject(schemaRegistry.tenant(), subjectName)) {
      throw Errors.invalidSubjectException(subjectName);
    }
    VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }
    String subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subjectName);
    Schema schema;
    try {
      schema = schemaRegistry.getUsingContexts(subject, versionId.getVersionId(), false);
      if (schema == null) {
        if (!schemaRegistry.hasSubjects(subject, false)) {
          throw Errors.subjectNotFoundException(subjectName);
        } else {
          throw Errors.versionNotFoundException(versionId.getVersionId());
        }
      }
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          String.format("Error while getting schema of subject %s version %s",
              subjectName, version), e);
    }
    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());


    RegisterSchemaResponse registerSchemaResponse;
    try {
      schemaRegistry.getCompositeUpdateRequestHandler().handle(schema, request, headerProperties);
      if (request.getRulesToMerge() != null || request.getRulesToRemove() != null) {
        if (request.getRuleSet() != null) {
          throw new RestInvalidRuleSetException(
              "ruleSet should be omitted if specifying rulesToMerge or rulesToRemove");
        }
      }

      if (request.getRulesToMerge() != null) {
        try {
          request.getRulesToMerge().validate();
        } catch (RuleException e) {
          throw new RestInvalidRuleSetException(e.getMessage());
        }
      }

      if (request.getRuleSet() != null) {
        try {
          request.getRuleSet().validate();
        } catch (RuleException e) {
          throw new RestInvalidRuleSetException(e.getMessage());
        }
      }

      Schema result =
          schemaRegistry.modifySchemaTagsOrForward(subject, schema, request, headerProperties);
      registerSchemaResponse = new RegisterSchemaResponse(result);
    } catch (InvalidSchemaException e) {
      throw Errors.invalidSchemaException(e);
    } catch (SchemaTooLargeException e) {
      throw Errors.schemaTooLargeException("Register operation failed because schema is too large");
    } catch (OperationNotPermittedException e) {
      throw Errors.operationNotPermittedException(e.getMessage());
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Register operation timed out", e);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Register schema operation failed while writing"
          + " to the Kafka store", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding register schema request"
          + " to the leader", e);
    } catch (IncompatibleSchemaException e) {
      throw Errors.incompatibleSchemaException("Schema being registered is incompatible with"
          + " an earlier schema for subject \"" + subjectName + "\", details: "
          + e.getMessage(), e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while registering schema", e);
    }
    asyncResponse.resume(registerSchemaResponse);
  }
}
