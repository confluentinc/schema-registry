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

import com.google.common.base.CharMatcher;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpdateRequest;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.exceptions.AssociationAlreadyExistsException;
import io.confluent.kafka.schemaregistry.storage.exceptions.TooManyAssociationsException;
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

@Path("/associations")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class AssociationsResource {

  public static final String apiTag = "Association (v1)";
  private static final Logger log = LoggerFactory.getLogger(AssociationsResource.class);
  private final KafkaSchemaRegistry schemaRegistry;

  public static final String DEFAULT_RESOURCE_TYPE = "topic";
  public static final String DEFAULT_ASSOCIATION_TYPE = "value";
  public static final LifecyclePolicy DEFAULT_LIFECYCLE = LifecyclePolicy.STRONG;
  public static final int NAME_MAX_LENGTH = 256;

  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public AssociationsResource(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Path("/subjects/{subject}")
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

  @Path("/resources/{resourceNamespace}/{resourceName}")
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

  @Path("/resources/{resourceId}")
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
      // TODO
      @Parameter(description = "Context")
      @QueryParam("context") String context,
      // TODO
      @Parameter(description = "Dry run")
      @QueryParam("dryRun") boolean dryRun,
      @Parameter(description = "The create request", required = true)
      @NotNull AssociationCreateRequest request) {

    log.debug("Creating association {}", request);

    checkName(request.getResourceName(), "resourceName");
    checkName(request.getResourceNamespace(), "resourceNamespace");
    if (request.getResourceId() == null || request.getResourceId().isEmpty()) {
      throw Errors.invalidAssociation("resourceId", "cannot be null or empty");
    }
    if (request.getResourceType() != null && !request.getResourceType().isEmpty()) {
      checkName(request.getResourceType(), "resourceType");
    } else {
      request.setResourceType(DEFAULT_RESOURCE_TYPE);
    }
    for (AssociationCreateInfo info : request.getAssociations()) {
      checkSubject(info.getSubject());
      if (info.getAssociationType() != null && !info.getAssociationType().isEmpty()) {
        checkName(info.getAssociationType(), "associationType");
      } else {
        info.setAssociationType(DEFAULT_ASSOCIATION_TYPE);
      }
      if (info.getLifecycle() == null) {
        info.setLifecycle(DEFAULT_LIFECYCLE);
      }
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, schemaRegistry.config().whitelistHeaders());

    try {
      AssociationResponse association = schemaRegistry.createAssociationOrForward(
          context, dryRun, request, headerProperties);
      asyncResponse.resume(association);
    } catch (AssociationAlreadyExistsException e) {
      throw Errors.associationAlreadyExistsException(e.getMessage());
    } catch (TooManyAssociationsException e) {
      // TODO maxKeys
      //throw Errors.tooManyAssociationsException(schemaRegistry.config().maxKeys());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while creating association: " + e.getMessage(), e);
    }
  }

  @Path("/resources/{resourceNamespace}/{resourceName}")
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
      // TODO
      @Parameter(description = "Context")
      @QueryParam("context") String context,
      // TODO
      @Parameter(description = "Dry run")
      @QueryParam("dryRun") boolean dryRun,
      @Parameter(description = "The update request", required = true)
      @NotNull AssociationUpdateRequest request) {

    log.debug("Updating association {}", request);

    if (request.getResourceName() != null && !request.getResourceName().isEmpty()) {
      checkName(request.getResourceName(), "resourceName");
      checkName(request.getResourceNamespace(), "resourceNamespace");
    } else if (request.getResourceId() == null || request.getResourceId().isEmpty()) {
      throw Errors.invalidAssociation("resourceId", "cannot be null or empty");
    }
    if (request.getResourceType() != null && !request.getResourceType().isEmpty()) {
      checkName(request.getResourceType(), "resourceType");
    } else {
      request.setResourceType(DEFAULT_RESOURCE_TYPE);
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
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while updating association: " + e.getMessage(), e);
    }
  }

  @Path("/resources/{resourceId}")
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
      // TODO
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
          resourceId, resourceType, associationTypes,
          headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while deleting association: " + e.getMessage(), e);
    }
  }

  private static void checkName(String name, String propertyName) {
    if (name == null || name.isEmpty()) {
      throw Errors.invalidAssociation(propertyName, "cannot be null or empty");
    }
    if (name.length() > NAME_MAX_LENGTH) {
      throw Errors.invalidAssociation(propertyName, "exceeds max length of " + NAME_MAX_LENGTH);
    }
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      throw Errors.invalidAssociation(propertyName, "must start with a letter or underscore");
    }
    for (int i = 1; i < name.length(); i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_' || c == '-')) {
        throw Errors.invalidAssociation(propertyName, "illegal character '" + c + "'");
      }
    }
  }

  private static void checkSubject(String subject) {
    if (subject == null || subject.isEmpty()
        || CharMatcher.javaIsoControl().matchesAnyOf(subject)) {
      throw Errors.invalidAssociation("subject", "must not be empty");
    }
  }
}
