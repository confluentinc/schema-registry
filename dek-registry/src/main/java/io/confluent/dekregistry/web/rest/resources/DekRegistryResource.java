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

package io.confluent.dekregistry.web.rest.resources;

import com.google.common.base.CharMatcher;
import io.confluent.dekregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.storage.exceptions.DekGenerationException;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.client.rest.entities.UpdateKekRequest;
import io.confluent.dekregistry.storage.DataEncryptionKey;
import io.confluent.dekregistry.storage.DekRegistry;
import io.confluent.dekregistry.storage.KeyEncryptionKey;
import io.confluent.dekregistry.storage.exceptions.AlreadyExistsException;
import io.confluent.dekregistry.storage.exceptions.KeyNotSoftDeletedException;
import io.confluent.dekregistry.storage.exceptions.KeyReferenceExistsException;
import io.confluent.dekregistry.storage.exceptions.TooManyKeysException;
import io.confluent.dekregistry.web.rest.exceptions.DekRegistryErrors;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.resources.DocumentedName;
import io.confluent.kafka.schemaregistry.rest.resources.RequestHeaderBuilder;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/dek-registry/v1")
@Singleton
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class DekRegistryResource extends SchemaRegistryResource {

  private static final Logger log = LoggerFactory.getLogger(DekRegistryResource.class);

  private final DekRegistry dekRegistry;
  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  @Inject
  public DekRegistryResource(SchemaRegistry schemaRegistry, DekRegistry dekRegistry) {
    super(schemaRegistry);
    this.dekRegistry = dekRegistry;
  }

  @GET
  @Path("/keks")
  @Operation(summary = "Get a list of kek names.")
  @PerformanceMetric("keks.list")
  @DocumentedName("getKekNames")
  public List<String> getKekNames(
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted) {
    return dekRegistry.getKekNames(lookupDeleted);
  }

  @GET
  @Path("/keks/{name}")
  @Operation(summary = "Get a kek by name.", responses = {
      @ApiResponse(responseCode = "200", description = "The kek info",
          content = @Content(schema = @Schema(implementation = Kek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40450 -- Key not found")
  })
  @PerformanceMetric("keks.get")
  @DocumentedName("getKek")
  public Kek getKek(
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted) {

    checkName(name);

    KeyEncryptionKey key = dekRegistry.getKek(name, lookupDeleted);
    if (key == null) {
      throw DekRegistryErrors.keyNotFoundException(name);
    }
    return key.toKekEntity();
  }

  @GET
  @Path("/keks/{name}/deks")
  @Operation(summary = "Get a list of dek subjects.")
  @PerformanceMetric("deks.list")
  @DocumentedName("getDekSubjects")
  public List<String> getDekSubjects(
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted) {

    checkName(name);

    return dekRegistry.getDekSubjects(name, lookupDeleted);
  }

  @GET
  @Path("/keks/{name}/deks/{subject}")
  @Operation(summary = "Get a dek by subject.", responses = {
      @ApiResponse(responseCode = "200", description = "The dek info",
          content = @Content(schema = @Schema(implementation = Dek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40450 -- Key not found")
  })
  @PerformanceMetric("deks.get")
  @DocumentedName("getDek")
  public Dek getDek(
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Algorithm of the dek")
      @QueryParam("algorithm") DekFormat algorithm,
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted) {

    checkName(name);
    checkSubject(subject);

    try {
      KeyEncryptionKey kek = dekRegistry.getKek(name, lookupDeleted);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(name);
      }
      DataEncryptionKey key = dekRegistry.getDek(name, subject, algorithm, lookupDeleted);
      if (key == null) {
        throw DekRegistryErrors.keyNotFoundException(subject);
      }
      return key.toDekEntity();
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while retrieving key", e);
    }
  }

  @POST
  @Path("/keks")
  @Operation(summary = "Create a kek.", responses = {
      @ApiResponse(responseCode = "200", description = "The create response",
          content = @Content(schema = @Schema(implementation = Kek.class))),
  })
  @PerformanceMetric("keks.create")
  @DocumentedName("createKek")
  public void createKek(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "The create request", required = true)
      @NotNull CreateKekRequest request) {

    log.debug("Creating kek {}", request.getName());

    checkName(request.getName());

    if (request.getKmsKeyId() == null || request.getKmsKeyId().isEmpty()) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("kmsKeyId");
    } else if (request.getKmsType() == null) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("kmsType");
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      KeyEncryptionKey key = dekRegistry.createKekOrForward(request, headerProperties);
      Kek kek = key.toKekEntity();
      asyncResponse.resume(kek);
    } catch (AlreadyExistsException e) {
      throw DekRegistryErrors.alreadyExistsException(e.getMessage());
    } catch (TooManyKeysException e) {
      throw DekRegistryErrors.tooManyKeysException(dekRegistry.config().maxKeys());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while creating key", e);
    }
  }

  @POST
  @Path("/keks/{name}/deks")
  @Operation(summary = "Create a dek.", responses = {
      @ApiResponse(responseCode = "200", description = "The create response",
          content = @Content(schema = @Schema(implementation = Dek.class))),
  })
  @PerformanceMetric("deks.create")
  @DocumentedName("createDek")
  public void createDek(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String name,
      @Parameter(description = "The create request", required = true)
      @NotNull CreateDekRequest request) {

    log.debug("Creating dek {} for kek {}", request.getSubject(), name);

    checkName(name);
    checkSubject(request.getSubject());

    KeyEncryptionKey kek = dekRegistry.getKek(name, false);
    if (kek == null) {
      throw DekRegistryErrors.keyNotFoundException(name);
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      DataEncryptionKey key = dekRegistry.createDekOrForward(name, request, headerProperties);
      Dek dek = key.toDekEntity();
      asyncResponse.resume(dek);
    } catch (AlreadyExistsException e) {
      throw DekRegistryErrors.alreadyExistsException(e.getMessage());
    } catch (TooManyKeysException e) {
      throw DekRegistryErrors.tooManyKeysException(dekRegistry.config().maxKeys());
    } catch (DekGenerationException e) {
      throw DekRegistryErrors.dekGenerationException(request.getSubject());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while creating key", e);
    }
  }

  @PUT
  @Path("/keks/{name}")
  @Operation(summary = "Alters a kek.", responses = {
      @ApiResponse(responseCode = "200", description = "The update response",
          content = @Content(schema = @Schema(implementation = Kek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40450 -- Key not found")
  })
  @PerformanceMetric("keks.put")
  @DocumentedName("updateKek")
  public void putKek(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String name,
      @Parameter(description = "The update request", required = true)
      @NotNull UpdateKekRequest request) {

    log.debug("Updating kek {}", name);

    checkName(name);

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      KeyEncryptionKey key = dekRegistry.putKekOrForward(name, request, headerProperties);
      if (key == null) {
        throw DekRegistryErrors.keyNotFoundException(name);
      }
      Kek kek = key.toKekEntity();
      asyncResponse.resume(kek);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while creating key", e);
    }
  }

  @DELETE
  @Path("/keks/{name}")
  @Operation(summary = "Delete a kek.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Error code 40450 -- Key not found")
  })
  @PerformanceMetric("keks.delete")
  @DocumentedName("deleteKek")
  public void deleteKek(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Whether to perform a permanent delete")
      @QueryParam("permanent") boolean permanentDelete) {

    log.debug("Deleting kek {}", name);

    checkName(name);

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      dekRegistry.deleteKekOrForward(name, permanentDelete, headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (KeyNotSoftDeletedException e) {
      throw DekRegistryErrors.keyNotSoftDeletedException(e.getName());
    } catch (KeyReferenceExistsException e) {
      throw DekRegistryErrors.referenceExistsException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting key", e);
    }
  }

  @DELETE
  @Path("/keks/{name}/deks/{subject}")
  @Operation(summary = "Delete a dek.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Error code 40450 -- Key not found")
  })
  @PerformanceMetric("deks.delete")
  @DocumentedName("deleteDek")
  public void deleteDek(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Algorithm of the dek")
      @QueryParam("algorithm") DekFormat algorithm,
      @Parameter(description = "Whether to perform a permanent delete")
      @QueryParam("permanent") boolean permanentDelete) {

    log.debug("Deleting dek {} for kek {}", subject, name);

    checkName(name);
    checkSubject(subject);

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      dekRegistry.deleteDekOrForward(name, subject, algorithm, permanentDelete, headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (KeyNotSoftDeletedException e) {
      throw DekRegistryErrors.keyNotSoftDeletedException(e.getName());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting key", e);
    }
  }

  private static void checkName(String name) {
    if (name == null || name.length() == 0) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("name");
    }
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("name");
    }
    for (int i = 1; i < name.length(); i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_' || c == '-')) {
        throw DekRegistryErrors.invalidOrMissingKeyInfo("name");
      }
    }
  }

  private static void checkSubject(String subject) {
    if (subject == null || subject.length() == 0
        || CharMatcher.javaIsoControl().matchesAnyOf(subject)) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("subject");
    }
  }
}
