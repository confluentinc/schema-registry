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

import com.google.common.base.CharMatcher;
import io.confluent.kafka.schemaregistry.exceptions.SchemaVersionNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
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
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/subjects/{subject}/versions")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SubjectVersionsResource {

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
  @PerformanceMetric("subjects.versions.get-schema")
  @ApiOperation(value = "Get a specific version of the schema registered under this subject.")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Error code 40401 -- Subject not found\n"
          + "Error code 40402 -- Version not found"),
      @ApiResponse(code = 422, message = "Error code 42202 -- Invalid version"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store")})
  public Schema getSchemaByVersion(
      @ApiParam(value = "Name of the Subject", required = true)@PathParam("subject") String subject,
      @ApiParam(value = VERSION_PARAM_DESC, required = true)@PathParam("version") String version,
      @QueryParam("deleted") boolean lookupDeletedSchema) {

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    VersionId versionId = null;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }
    Schema schema = null;
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
  @PerformanceMetric("subjects.versions.get-schema.only")
  @ApiOperation(value = "Get the schema for the specified version of this subject. "
      + "The unescaped schema only is returned.")
  @ApiResponses(value = {@ApiResponse(code = 404, message =
      "Error code 40401 -- Subject not found\n"
          + "Error code 40402 -- Version not found"), @ApiResponse(code = 422,
      message = "Error code 42202 -- Invalid version"), @ApiResponse(code = 500,
      message = "Error code 50001 -- Error in the backend data store")})
  public String getSchemaOnly(
      @ApiParam(value = "Name of the Subject", required = true)@PathParam("subject") String subject,
      @ApiParam(value = VERSION_PARAM_DESC, required = true)@PathParam("version") String version,
      @QueryParam("deleted") boolean lookupDeletedSchema) {
    return getSchemaByVersion(subject, version, lookupDeletedSchema).getSchema();
  }

  @GET
  @Path("/{version}/referencedby")
  @ApiOperation(value = "Get the schemas that reference the specified schema.")
  @ApiResponses(value = {@ApiResponse(code = 404, message =
      "Error code 40401 -- Subject not found\n"
          + "Error code 40402 -- Version not found"), @ApiResponse(code = 422,
      message = "Error code 42202 -- Invalid version"), @ApiResponse(code = 500,
      message = "Error code 50001 -- Error in the backend data store")})
  public List<Integer> getReferencedBy(
      @ApiParam(value = "Name of the Subject", required = true)@PathParam("subject") String subject,
      @ApiParam(value = VERSION_PARAM_DESC, required = true)@PathParam("version") String version) {

    Schema schema = getSchemaByVersion(subject, version, true);
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
  @PerformanceMetric("subjects.versions.list")
  @ApiOperation(value = "Get a list of versions registered under the specified subject.")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Error code 40401 -- Subject not found"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store")})
  public List<Integer> listVersions(
      @ApiParam(value = "Name of the Subject", required = true)
        @PathParam("subject") String subject,
      @QueryParam("deleted") boolean lookupDeletedSchema) {

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    // check if subject exists. If not, throw 404
    Iterator<Schema> allSchemasForThisTopic = null;
    List<Integer> allVersions = new ArrayList<Integer>();
    String errorMessage = "Error while validating that subject "
                          + subject
                          + " exists in the registry";
    try {
      if (!schemaRegistry.hasSubjects(subject, lookupDeletedSchema)) {
        throw Errors.subjectNotFoundException(subject);
      }
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    errorMessage = "Error while listing all versions for subject "
                   + subject;
    try {
      allSchemasForThisTopic = schemaRegistry.getAllVersions(subject,
              lookupDeletedSchema);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    while (allSchemasForThisTopic.hasNext()) {
      Schema schema = allSchemasForThisTopic.next();
      allVersions.add(schema.getVersion());
    }
    return allVersions;
  }

  @POST
  @PerformanceMetric("subjects.versions.register")
  @ApiOperation(value = "Register a new schema under the specified subject. If successfully "
      + "registered, this returns the unique identifier of this schema in the registry. The "
      + "returned identifier should be used to retrieve this schema from the schemas resource and "
      + "is different from the schema's version which is associated with the subject. If the same "
      + "schema is registered under a different subject, the same identifier will be returned. "
      + "However, the version of the schema may be different under different subjects.\n"
      + "A schema should be compatible with the previously registered schema or schemas (if there "
      + "are any) as per the configured compatibility level. The configured compatibility level "
      + "can be obtained by issuing a GET http:get:: /config/(string: subject). If that returns "
      + "null, then GET http:get:: /config\n"
      + "When there are multiple instances of Schema Registry running in the same cluster, the "
      + "schema registration request will be forwarded to one of the instances designated as "
      + "the primary. If the primary is not available, the client will get an error code "
      + "indicating that the forwarding has failed.", response = RegisterSchemaResponse.class)
  @ApiResponses(value = {
      @ApiResponse(code = 409, message = "Incompatible schema"),
      @ApiResponse(code = 422, message = "Error code 42201 -- Invalid schema or schema type"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store\n"
          + "Error code 50002 -- Operation timed out\n"
          + "Error code 50003 -- Error while forwarding the request to the primary")})
  public void register(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @ApiParam(value = "Name of the Subject", required = true)
        @PathParam("subject") String subjectName,
      @QueryParam("normalize") boolean normalize,
      @ApiParam(value = "Schema", required = true)
      @NotNull RegisterSchemaRequest request) {
    log.info("Registering new schema: subject {}, version {}, id {}, type {}, schema size {}",
             subjectName, request.getVersion(), request.getId(), request.getSchemaType(),
            request.getSchema() == null ? 0 : request.getSchema().length());

    if (subjectName != null && CharMatcher.javaIsoControl().matchesAnyOf(subjectName)) {
      throw Errors.invalidSubjectException(subjectName);
    }
    subjectName = QualifiedSubject.normalize(schemaRegistry.tenant(), subjectName);

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());

    Schema schema = new Schema(
        subjectName,
        request.getVersion() != null ? request.getVersion() : 0,
        request.getId() != null ? request.getId() : -1,
        request.getSchemaType() != null ? request.getSchemaType() : AvroSchema.TYPE,
        request.getReferences(),
        request.getSchema()
    );
    int id;
    try {
      id = schemaRegistry.registerOrForward(subjectName, schema, normalize, headerProperties);
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
      throw Errors.incompatibleSchemaException("Schema being registered is incompatible with an"
                                               + " earlier schema for subject "
                                               + "\"" + subjectName + "\"", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while registering schema", e);
    }
    RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
    registerSchemaResponse.setId(id);
    asyncResponse.resume(registerSchemaResponse);
  }

  @DELETE
  @Path("/{version}")
  @PerformanceMetric("subjects.versions.deleteSchemaVersion-schema")
  @ApiOperation(value = "Deletes a specific version of the schema registered under this subject. "
      + "This only deletes the version and the schema ID remains intact making it still possible "
      + "to decode data using the schema ID. This API is recommended to be used only in "
      + "development environments or under extreme circumstances where-in, its required to delete "
      + "a previously registered schema for compatibility purposes or re-register previously "
      + "registered schema.", response = int.class)
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Error code 40401 -- Subject not found\n"
          + "Error code 40402 -- Version not found"),
      @ApiResponse(code = 422, message = "Error code 42202 -- Invalid version"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend data store")})
  public void deleteSchemaVersion(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @ApiParam(value = "Name of the Subject", required = true)
        @PathParam("subject") String subject,
      @ApiParam(value = VERSION_PARAM_DESC, required = true)
        @PathParam("version") String version,
      @QueryParam("permanent") boolean permanentDelete) {
    log.info("Deleting schema version {} from subject {}", version, subject);

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    VersionId versionId = null;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }
    Schema schema = null;
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
}
