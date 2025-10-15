/*
 * Copyright 2023 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchCreateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationBatchUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;
import io.confluent.kafka.schemaregistry.exceptions.AssociationForSubjectExistsException;
import io.confluent.kafka.schemaregistry.exceptions.AssociationFrozenException;
import io.confluent.kafka.schemaregistry.exceptions.AssociationNotFoundException;
import io.confluent.kafka.schemaregistry.exceptions.IncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidAssociationException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.NoActiveSubjectVersionExistsException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.ReferenceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaTooLargeException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaVersionNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.StrongAssociationForSubjectExistsException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.exceptions.AssociationForResourceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.TooManyAssociationsException;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class AssociationsResource {

  public static final String apiTag = "Association (v1)";
  private static final Logger log = LoggerFactory.getLogger(AssociationsResource.class);
  private final SchemaRegistry schemaRegistry;

  public static final String DEFAULT_RESOURCE_TYPE = "topic";

  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public AssociationsResource(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Path("/associations/subjects/{subject}")
  @GET
  @Operation(summary = "Get a list of associations by subject.", responses = {
      @ApiResponse(responseCode = "200",
          description = "List of associations", content = @Content(
          array = @ArraySchema(schema = @Schema(implementation = Association.class)))),
  })
  @PerformanceMetric("associations.by-subject")
  @DocumentedName("getAssociationsBySubject")
  public List<Association> getAssociationsBySubject(
      @Parameter(description = "Subject")
      @PathParam("subject") String subject,
      @Parameter(description = "Resource type")
      @QueryParam("resourceType") String resourceType,
      @Parameter(description = "Association type")
      @QueryParam("associationType") List<String> associationTypes,
      @Parameter(description = "Lifecycle")
      @QueryParam("lifecycle") LifecyclePolicy lifecycle,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {
    String errorMessage = "Error while getting associations for subject " + subject;
    if (resourceType == null || resourceType.isEmpty()) {
      resourceType = DEFAULT_RESOURCE_TYPE;
    }
    try {
      limit = schemaRegistry.normalizeSchemaLimit(limit);
      List<Association> associations = schemaRegistry.getAssociationsBySubject(
          subject, resourceType, associationTypes, lifecycle);
      return associations.stream()
        .skip(offset)
        .limit(limit)
        .collect(Collectors.toList());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
  }

  @Path("/associations/resources/{resourceNamespace}/{resourceName}")
  @GET
  @Operation(summary = "Get a list of associations by resource name.", responses = {
      @ApiResponse(responseCode = "200",
          description = "List of associations", content = @Content(
          array = @ArraySchema(schema = @Schema(implementation = Association.class)))),
  })
  @PerformanceMetric("associations.by-resource-name")
  @DocumentedName("getAssociationsByResourceName")
  public List<Association> getAssociationsByResourceName(
      @Parameter(description = "Resource name")
      @PathParam("resourceName") String resourceName,
      @Parameter(description = "Resource namespace")
      @PathParam("resourceNamespace") String resourceNamespace,
      @Parameter(description = "Resource type")
      @QueryParam("resourceType") String resourceType,
      @Parameter(description = "Association type")
      @QueryParam("associationType") List<String> associationTypes,
      @Parameter(description = "Lifecycle")
      @QueryParam("lifecycle") LifecyclePolicy lifecycle,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {
    String errorMessage = "Error while getting associations for resource name " + resourceName;
    if (resourceType == null || resourceType.isEmpty()) {
      resourceType = DEFAULT_RESOURCE_TYPE;
    }
    try {
      limit = schemaRegistry.normalizeSchemaLimit(limit);
      List<Association> associations = schemaRegistry.getAssociationsByResourceName(
          resourceName, resourceNamespace, resourceType, associationTypes, lifecycle);
      return associations.stream()
          .skip(offset)
          .limit(limit)
          .collect(Collectors.toList());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
  }

  @Path("/associations/resources/{resourceId}")
  @GET
  @Operation(summary = "Get a list of associations by resource ID.", responses = {
      @ApiResponse(responseCode = "200",
          description = "List of associations", content = @Content(
          array = @ArraySchema(schema = @Schema(implementation = Association.class)))),
  })
  @PerformanceMetric("associations.by-resource-id")
  @DocumentedName("getAssociationsByResourceId")
  public List<Association> getAssociationsByResourceId(
      @Parameter(description = "Resource ID")
      @PathParam("resourceId") String resourceId,
      @Parameter(description = "Resource type")
      @QueryParam("resourceType") String resourceType,
      @Parameter(description = "Association type")
      @QueryParam("associationType") List<String> associationTypes,
      @Parameter(description = "Lifecycle")
      @QueryParam("lifecycle") LifecyclePolicy lifecycle,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {
    String errorMessage = "Error while getting associations for resource ID " + resourceId;
    if (resourceType == null || resourceType.isEmpty()) {
      resourceType = DEFAULT_RESOURCE_TYPE;
    }
    try {
      limit = schemaRegistry.normalizeSchemaLimit(limit);
      List<Association> associations = schemaRegistry.getAssociationsByResourceId(
          resourceId, resourceType, associationTypes, lifecycle);
      return associations.stream()
          .skip(offset)
          .limit(limit)
          .collect(Collectors.toList());
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
  }

  @Path("/associations")
  @POST
  @Operation(summary = "Create an association.", responses = {
      @ApiResponse(responseCode = "200", description = "The create response",
          content = @Content(schema = @Schema(implementation = AssociationResponse.class))),
      @ApiResponse(responseCode = "409", description = "Conflict. "
          + "Error code 40911 -- Association already exists. "
          + "Error code 40912 -- Too many associations."),
      @ApiResponse(responseCode = "422", description = "Error code 42212 -- Invalid association")
  })
  @PerformanceMetric("association.create")
  @DocumentedName("createAssociation")
  public void createAssociation(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      // TODO RAY
      @Parameter(description = "Context")
      @QueryParam("context") String context,
      // TODO RAY
      @Parameter(description = "Dry run")
      @QueryParam("dryRun") boolean dryRun,
      @Parameter(description = "The create request", required = true)
      @NotNull AssociationCreateRequest request) {

    log.debug("Creating association {}", request);

    try {
      request.validate();
    } catch (IllegalPropertyException e) {
      throw Errors.invalidAssociationException(e.getPropertyName(), e.getDetail());
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());

    try {
      AssociationResponse association = schemaRegistry.createAssociationOrForward(
          context, dryRun, request, headerProperties);
      asyncResponse.resume(association);
    } catch (InvalidAssociationException e) {
      throw Errors.invalidAssociationException(e.getPropertyName(), e.getDetail());
    } catch (AssociationForResourceExistsException e) {
      throw Errors.associationForResourceExistsException(e.getAssociationType(), e.getResource());
    } catch (AssociationForSubjectExistsException e) {
      throw Errors.associationForSubjectExistsException(e.getMessage());
    } catch (NoActiveSubjectVersionExistsException e) {
      throw Errors.noActiveSubjectVersionExistsException(e.getMessage());
    } catch (StrongAssociationForSubjectExistsException e) {
      throw Errors.strongAssociationExistsException(e.getMessage());
    } catch (TooManyAssociationsException e) {
      // TODO RAY max
      //throw Errors.tooManyAssociationsException(schemaRegistry.config().maxKeys());
    } catch (InvalidSchemaException e) {
      throw Errors.invalidSchemaException(e);
    } catch (SchemaTooLargeException e) {
      throw Errors.schemaTooLargeException("Register operation failed because schema is too large");
    } catch (IncompatibleSchemaException e) {
      throw Errors.incompatibleSchemaException(e.getMessage(), e);
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
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while creating association: " + e.getMessage(), e);
    }
  }

  @Path("/associations")
  @PUT
  @Operation(summary = "Update an association.", responses = {
      @ApiResponse(responseCode = "200", description = "The update response",
          content = @Content(schema = @Schema(implementation = AssociationResponse.class))),
      @ApiResponse(responseCode = "422", description = "Error code 42212 -- Invalid association")
  })
  @PerformanceMetric("association.update")
  @DocumentedName("updateAssociation")
  public void updateAssociation(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      // TODO RAY
      @Parameter(description = "Context")
      @QueryParam("context") String context,
      // TODO RAY
      @Parameter(description = "Dry run")
      @QueryParam("dryRun") boolean dryRun,
      @Parameter(description = "The update request", required = true)
      @NotNull AssociationUpdateRequest request) {

    log.debug("Updating association {}", request);

    try {
      request.validate();
    } catch (IllegalPropertyException e) {
      throw Errors.invalidAssociationException(e.getPropertyName(), e.getDetail());
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());

    try {
      List<String> associationTypes = request.getAssociations().stream()
          .map(AssociationUpdateInfo::getAssociationType)
          .collect(Collectors.toList());
      String resource;
      List<Association> oldAssociations;
      if (request.getResourceName() != null && !request.getResourceName().isEmpty()) {
        resource = request.getResourceName();
        oldAssociations = schemaRegistry.getAssociationsByResourceName(
            request.getResourceName(), request.getResourceNamespace(), request.getResourceType(),
            associationTypes, null);
      } else {
        resource = request.getResourceId();
        oldAssociations = schemaRegistry.getAssociationsByResourceId(
            request.getResourceId(), request.getResourceType(),
            associationTypes, null);
      }
      Set<String> oldAssociationTypes = oldAssociations.stream()
          .map(Association::getAssociationType)
          .collect(Collectors.toSet());
      for (String associationType : associationTypes) {
        if (!oldAssociationTypes.contains(associationType)) {
          throw Errors.associationNotFoundException(resource);
        }
      }

      AssociationResponse association = schemaRegistry.updateAssociationOrForward(context, dryRun,
          request, headerProperties);
      asyncResponse.resume(association);
    } catch (InvalidAssociationException e) {
      throw Errors.invalidAssociationException(e.getPropertyName(), e.getDetail());
    } catch (AssociationForSubjectExistsException e) {
      throw Errors.associationForSubjectExistsException(e.getMessage());
    } catch (AssociationFrozenException e) {
      throw Errors.associationFrozenException(e.getAssociationType(), e.getSubject());
    } catch (AssociationNotFoundException e) {
      throw Errors.associationNotFoundException(e.getMessage());
    } catch (TooManyAssociationsException e) {
      // TODO RAY max
      //throw Errors.tooManyAssociationsException(schemaRegistry.config().maxKeys());
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Register operation timed out", e);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Register schema operation failed while writing"
          + " to the Kafka store", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding register schema request"
          + " to the leader", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while updating association: " + e.getMessage(), e);
    }
  }

  @Path("/associations:batchCreate")
  @POST
  @Operation(summary = "Create a batch of associations.", responses = {
      @ApiResponse(responseCode = "207", description = "The create response",
          content = @Content(schema = @Schema(implementation = AssociationBatchResponse.class)))
  })
  @PerformanceMetric("associations.create")
  @DocumentedName("createAssociations")
  public void createAssociations(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      // TODO RAY
      @Parameter(description = "Context")
      @QueryParam("context") String context,
      // TODO RAY
      @Parameter(description = "Dry run")
      @QueryParam("dryRun") boolean dryRun,
      @Parameter(description = "The create requests", required = true)
      @NotNull AssociationBatchCreateRequest request) {

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());

    try {
      AssociationBatchResponse response = schemaRegistry.createAssociationsOrForward(
          context, dryRun, request, headerProperties);
      asyncResponse.resume(response);
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Register operation timed out", e);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Register schema operation failed while writing"
          + " to the Kafka store", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding register schema request"
          + " to the leader", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while creating associations: " + e.getMessage(), e);
    }
  }

  @Path("/associations:batchUpdate")
  @POST
  @Operation(summary = "Update a batch of associations.", responses = {
      @ApiResponse(responseCode = "207", description = "The update response",
          content = @Content(schema = @Schema(implementation = AssociationBatchResponse.class)))
  })
  @PerformanceMetric("associations.update")
  @DocumentedName("updateAssociations")
  public void updateAssociations(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      // TODO RAY
      @Parameter(description = "Context")
      @QueryParam("context") String context,
      // TODO RAY
      @Parameter(description = "Dry run")
      @QueryParam("dryRun") boolean dryRun,
      @Parameter(description = "The update request", required = true)
      @NotNull AssociationBatchUpdateRequest request) {

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());

    try {
      AssociationBatchResponse response = schemaRegistry.updateAssociationsOrForward(
          context, dryRun, request, headerProperties);
      asyncResponse.resume(response);
    } catch (SchemaRegistryTimeoutException e) {
      throw Errors.operationTimeoutException("Register operation timed out", e);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException("Register schema operation failed while writing"
          + " to the Kafka store", e);
    } catch (SchemaRegistryRequestForwardingException e) {
      throw Errors.requestForwardingFailedException("Error while forwarding register schema request"
          + " to the leader", e);
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while creating associations: " + e.getMessage(), e);
    }
  }

  @Path("/associations/resources/{resourceId}")
  @DELETE
  @Operation(summary = "Delete associations.", responses = {
      @ApiResponse(responseCode = "200", description = "The delete response",
          content = @Content(schema = @Schema(implementation = Association.class))),
      @ApiResponse(responseCode = "422", description = "Error code 42212 -- Invalid association")
  })
  @PerformanceMetric("associations.delete")
  @DocumentedName("deleteAssociations")
  public void deleteAssociations(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @PathParam("resourceId") String resourceId,
      @Parameter(description = "Resource type")
      @QueryParam("resourceType") String resourceType,
      @Parameter(description = "Association type")
      @QueryParam("associationType") List<String> associationTypes,
      // TODO RAY
      @Parameter(description = "Cascade lifecycle")
      @QueryParam("cascadeLifecycle") boolean cascadeLifecycle) {

    log.debug("Deleting association for resource {}", resourceId);

    if (resourceType == null || resourceType.isEmpty()) {
      resourceType = DEFAULT_RESOURCE_TYPE;
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());

    try {
      List<Association> oldAssociations = schemaRegistry.getAssociationsByResourceId(
          resourceId, resourceType, associationTypes, null);
      if (oldAssociations.isEmpty()) {
        throw Errors.associationNotFoundException(resourceId);
      }

      String subject = oldAssociations.get(0).getSubject();
      schemaRegistry.deleteAssociationsOrForward(subject,
          resourceId, resourceType, associationTypes, cascadeLifecycle,
          headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (AssociationFrozenException e) {
      throw Errors.associationFrozenException(e.getAssociationType(), e.getSubject());
    } catch (SchemaVersionNotSoftDeletedException e) {
      throw Errors.schemaVersionNotSoftDeletedException(e.getSubject(),
          e.getVersion());
    } catch (ReferenceExistsException e) {
      throw Errors.referenceExistsException(e.getMessage());
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
    } catch (UnknownLeaderException e) {
      throw Errors.unknownLeaderException("Leader not known.", e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while deleting association: " + e.getMessage(), e);
    }
  }
}
