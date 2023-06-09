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

import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.SubjectNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.DefaultValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.Valid;
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
import io.confluent.kafka.schemaregistry.exceptions.ReferenceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/subjects")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
           Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class SubjectsResource {

  private static final Logger log = LoggerFactory.getLogger(SubjectsResource.class);
  private final KafkaSchemaRegistry schemaRegistry;
  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public SubjectsResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @POST
  @Path("/{subject}")
  @ApiOperation(value = "Check if a schema has already been registered under the specified subject."
      + " If so, this returns the schema string along with its globally unique identifier, its "
      + "version under this subject and the subject name.")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Error code 40401 -- Subject not found\n"
          + "Error code 40403 -- Schema not found"),
      @ApiResponse(code = 500, message = "Internal server error", response = Schema.class),
  })
  @PerformanceMetric("subjects.get-schema")
  public void lookUpSchemaUnderSubject(
      final @Suspended AsyncResponse asyncResponse,
      @ApiParam(value = "Subject under which the schema will be registered", required = true)
        @PathParam("subject") String subject,
      @QueryParam("normalize") boolean normalize,
      @QueryParam("deleted") boolean lookupDeletedSchema,
      @ApiParam(value = "Schema", required = true)
      @NotNull RegisterSchemaRequest request) {
    log.info("Schema lookup under subject {}, deleted {}, type {}",
             subject, lookupDeletedSchema, request.getSchemaType());

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);

    // returns version if the schema exists. Otherwise returns 404
    Schema schema = new Schema(
        subject,
        0,
        -1,
        request.getSchemaType() != null ? request.getSchemaType() : AvroSchema.TYPE,
        request.getReferences(),
        request.getSchema()
    );
    io.confluent.kafka.schemaregistry.client.rest.entities.Schema matchingSchema = null;
    try {
      matchingSchema = schemaRegistry.lookUpSchemaUnderSubjectUsingContexts(
          subject, schema, normalize, lookupDeletedSchema);
      if (matchingSchema == null) {
        if (!schemaRegistry.hasSubjects(subject, lookupDeletedSchema)) {
          throw Errors.subjectNotFoundException(subject);
        } else {
          throw Errors.schemaNotFoundException();
        }
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
  @Valid
  @ApiOperation(value = "Get a list of registered subjects.")
  @ApiResponses(value = {
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend datastore")})
  @PerformanceMetric("subjects.list")
  public Set<String> list(
          @DefaultValue(QualifiedSubject.CONTEXT_WILDCARD)
          @QueryParam("subjectPrefix") String subjectPrefix,
          @QueryParam("deleted") boolean lookupDeletedSubjects
  ) {
    try {
      return schemaRegistry.listSubjectsWithPrefix(
          subjectPrefix != null ? subjectPrefix : QualifiedSubject.CONTEXT_WILDCARD,
          lookupDeletedSubjects);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Error while listing subjects", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while listing subjects", e);
    }
  }

  @DELETE
  @Path("/{subject}")
  @ApiOperation(value = "Deletes the specified subject and its associated compatibility level if "
      + "registered. It is recommended to use this API only when a topic needs to be recycled or "
      + "in development environment.", response = Integer.class, responseContainer = "List")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Error code 40401 -- Subject not found"),
      @ApiResponse(code = 500, message = "Error code 50001 -- Error in the backend datastore")
  })
  @PerformanceMetric("subjects.delete-subject")
  public void deleteSubject(
      final @Suspended AsyncResponse asyncResponse,
      @Context HttpHeaders headers,
      @ApiParam(value = "the name of the subject", required = true)
      @PathParam("subject") String subject,
      @QueryParam("permanent") boolean permanentDelete) {
    log.info("Deleting subject {}", subject);

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
