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

import static io.confluent.dekregistry.storage.DekRegistry.MIN_VERSION;

import com.google.common.base.CharMatcher;
import io.confluent.dekregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.storage.exceptions.DekGenerationException;
import io.confluent.dekregistry.storage.exceptions.InvalidKeyException;
import io.confluent.dekregistry.storage.exceptions.KeySoftDeletedException;
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
import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.resources.DocumentedName;
import io.confluent.kafka.schemaregistry.rest.resources.RequestHeaderBuilder;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
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

@Path("/dek-registry/v1/keks")
@Singleton
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class DekRegistryResource extends SchemaRegistryResource {

  private static final Logger log = LoggerFactory.getLogger(DekRegistryResource.class);

  public static final int NAME_MAX_LENGTH = 64;

  private final DekRegistry dekRegistry;
  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  @Inject
  public DekRegistryResource(SchemaRegistry schemaRegistry, DekRegistry dekRegistry) {
    super(schemaRegistry);
    this.dekRegistry = dekRegistry;
  }

  @GET
  @Operation(summary = "Get a list of kek names.", responses = {
      @ApiResponse(responseCode = "200",
          description = "List of kek names", content = @Content(
          array = @ArraySchema(schema = @io.swagger.v3.oas.annotations.media.Schema(
              example = "mykek")))),
  })
  @PerformanceMetric("keks.list")
  @DocumentedName("getKekNames")
  public List<String> getKekNames(
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted) {
    return dekRegistry.getKekNames(lookupDeleted);
  }

  @GET
  @Path("/{name}")
  @Operation(summary = "Get a kek by name.", responses = {
      @ApiResponse(responseCode = "200", description = "The kek info",
          content = @Content(schema = @Schema(implementation = Kek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found")
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
  @Path("/{name}/deks")
  @Operation(summary = "Get a list of dek subjects.", responses = {
      @ApiResponse(responseCode = "200",
          description = "List of dek subjects", content = @Content(
          array = @ArraySchema(schema = @io.swagger.v3.oas.annotations.media.Schema(
              example = "User")))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found")
  })
  @PerformanceMetric("deks.list")
  @DocumentedName("getDekSubjects")
  public List<String> getDekSubjects(
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted) {

    checkName(kekName);

    KeyEncryptionKey key = dekRegistry.getKek(kekName, lookupDeleted);
    if (key == null) {
      throw DekRegistryErrors.keyNotFoundException(kekName);
    }
    return dekRegistry.getDekSubjects(kekName, lookupDeleted);
  }

  @GET
  @Path("/{name}/deks/{subject}")
  @Operation(summary = "Get a dek by subject.", responses = {
      @ApiResponse(responseCode = "200", description = "The dek info",
          content = @Content(schema = @Schema(implementation = Dek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found")
  })
  @PerformanceMetric("deks.get")
  @DocumentedName("getDek")
  public Dek getDek(
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Algorithm of the dek")
      @QueryParam("algorithm") DekFormat algorithm,
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted) {

    checkName(kekName);
    checkSubject(subject);

    try {
      KeyEncryptionKey kek = dekRegistry.getKek(kekName, lookupDeleted);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(kekName);
      }
      DataEncryptionKey key = dekRegistry.getDek(
          kekName, subject, MIN_VERSION, algorithm, lookupDeleted);
      if (key == null) {
        throw DekRegistryErrors.keyNotFoundException(subject);
      }
      return key.toDekEntity();
    } catch (DekGenerationException e) {
      throw DekRegistryErrors.dekGenerationException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while retrieving key", e);
    }
  }

  @GET
  @Path("/{name}/deks/{subject}/versions")
  @Operation(summary = "List versions of dek.", responses = {
      @ApiResponse(responseCode = "200",
          description = "List of version numbers for dek",
          content = @Content(array = @ArraySchema(
              schema = @io.swagger.v3.oas.annotations.media.Schema(type = "integer",
                  format = "int32", example = "1")))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found")
  })
  @PerformanceMetric("deks.versions.list")
  @DocumentedName("getAllDekVersions")
  public List<Integer> getDekVersions(
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Algorithm of the dek")
      @QueryParam("algorithm") DekFormat algorithm,
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted) {

    checkName(kekName);
    checkSubject(subject);

    KeyEncryptionKey kek = dekRegistry.getKek(kekName, lookupDeleted);
    if (kek == null) {
      throw DekRegistryErrors.keyNotFoundException(kekName);
    }
    return dekRegistry.getDekVersions(kekName, subject, algorithm, lookupDeleted);
  }

  @GET
  @Path("/{name}/deks/{subject}/versions/{version}")
  @Operation(summary = "Get a dek by subject and version.", responses = {
      @ApiResponse(responseCode = "200", description = "The dek info",
          content = @Content(schema = @Schema(implementation = Dek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "422", description = "Error code 42202 -- Invalid version")
  })
  @PerformanceMetric("deks.versions.get")
  @DocumentedName("getDekByVersion")
  public Dek getDekByVersion(
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Version of the dek", required = true)
      @PathParam("version") String version,
      @Parameter(description = "Algorithm of the dek")
      @QueryParam("algorithm") DekFormat algorithm,
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted) {

    checkName(kekName);
    checkSubject(subject);
    VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }

    try {
      KeyEncryptionKey kek = dekRegistry.getKek(kekName, lookupDeleted);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(kekName);
      }
      DataEncryptionKey key = dekRegistry.getDek(
          kekName, subject, versionId.getVersionId(), algorithm, lookupDeleted);
      if (key == null) {
        throw DekRegistryErrors.keyNotFoundException(subject);
      }
      return key.toDekEntity();
    } catch (DekGenerationException e) {
      throw DekRegistryErrors.dekGenerationException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while retrieving key", e);
    }
  }

  @POST
  @Operation(summary = "Create a kek.", responses = {
      @ApiResponse(responseCode = "200", description = "The create response",
          content = @Content(schema = @Schema(implementation = Kek.class))),
  })
  @PerformanceMetric("keks.create")
  @DocumentedName("registerKek")
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
      Kek kek = dekRegistry.createKekOrForward(request, headerProperties);
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
  @Path("/{name}/deks")
  @Operation(summary = "Create a dek.", responses = {
      @ApiResponse(responseCode = "200", description = "The create response",
          content = @Content(schema = @Schema(implementation = Dek.class))),
  })
  @PerformanceMetric("deks.create")
  @DocumentedName("registerDek")
  public void createDek(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "The create request", required = true)
      @NotNull CreateDekRequest request) {

    log.debug("Creating dek {} for kek {}", request.getSubject(), kekName);

    checkName(kekName);
    checkSubject(request.getSubject());

    KeyEncryptionKey kek = dekRegistry.getKek(kekName, request.isDeleted());
    if (kek == null) {
      throw DekRegistryErrors.keyNotFoundException(kekName);
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      Dek dek = dekRegistry.createDekOrForward(kekName, request, headerProperties);
      asyncResponse.resume(dek);
    } catch (AlreadyExistsException e) {
      throw DekRegistryErrors.alreadyExistsException(e.getMessage());
    } catch (DekGenerationException e) {
      throw DekRegistryErrors.dekGenerationException(e.getMessage());
    } catch (InvalidKeyException e) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo(e.getMessage());
    } catch (TooManyKeysException e) {
      throw DekRegistryErrors.tooManyKeysException(dekRegistry.config().maxKeys());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while creating key", e);
    }
  }

  @PUT
  @Path("/{name}")
  @Operation(summary = "Alters a kek.", responses = {
      @ApiResponse(responseCode = "200", description = "The update response",
          content = @Content(schema = @Schema(implementation = Kek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found")
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
      Kek kek = dekRegistry.putKekOrForward(name, request, headerProperties);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(name);
      }
      asyncResponse.resume(kek);
    } catch (AlreadyExistsException e) {
      throw DekRegistryErrors.alreadyExistsException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while creating key", e);
    }
  }

  @DELETE
  @Path("/{name}")
  @Operation(summary = "Delete a kek.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found")
  })
  @PerformanceMetric("keks.delete")
  @DocumentedName("deregisterKek")
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
      KeyEncryptionKey kek = dekRegistry.getKek(name, true);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(name);
      }

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
  @Path("/{name}/deks/{subject}")
  @Operation(summary = "Delete all versions of a dek.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found")
  })
  @PerformanceMetric("deks.delete")
  @DocumentedName("deregisterDekVersions")
  public void deleteDekVersions(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Algorithm of the dek")
      @QueryParam("algorithm") DekFormat algorithm,
      @Parameter(description = "Whether to perform a permanent delete")
      @QueryParam("permanent") boolean permanentDelete) {

    log.debug("Deleting dek {} for kek {}", subject, kekName);

    checkName(kekName);
    checkSubject(subject);

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      KeyEncryptionKey kek = dekRegistry.getKek(kekName, true);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(kekName);
      }
      DataEncryptionKey key = dekRegistry.getLatestDek(kekName, subject, algorithm, true);
      if (key == null) {
        throw DekRegistryErrors.keyNotFoundException(subject);
      }

      dekRegistry.deleteDekOrForward(
          kekName, subject, algorithm, permanentDelete, headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (KeyNotSoftDeletedException e) {
      throw DekRegistryErrors.keyNotSoftDeletedException(e.getName());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting key", e);
    }
  }

  @DELETE
  @Path("/{name}/deks/{subject}/versions/{version}")
  @Operation(summary = "Delete a dek version.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "422", description = "Error code 42202 -- Invalid version")
  })
  @PerformanceMetric("deks.versions.delete")
  @DocumentedName("deregisterDekVersion")
  public void deleteDekVersion(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Version of the dek", required = true)
      @PathParam("version") String version,
      @Parameter(description = "Algorithm of the dek")
      @QueryParam("algorithm") DekFormat algorithm,
      @Parameter(description = "Whether to perform a permanent delete")
      @QueryParam("permanent") boolean permanentDelete) {

    log.debug("Deleting dek {} for kek {}", subject, kekName);

    checkName(kekName);
    checkSubject(subject);
    VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      KeyEncryptionKey kek = dekRegistry.getKek(kekName, true);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(kekName);
      }
      DataEncryptionKey key = dekRegistry.getDek(
          kekName, subject, versionId.getVersionId(), algorithm, true);
      if (key == null) {
        throw DekRegistryErrors.keyNotFoundException(subject);
      }

      dekRegistry.deleteDekVersionOrForward(
          kekName, subject, versionId.getVersionId(), algorithm, permanentDelete, headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (KeyNotSoftDeletedException e) {
      throw DekRegistryErrors.keyNotSoftDeletedException(e.getName());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting key", e);
    }
  }

  @POST
  @Path("/{name}/undelete")
  @Operation(summary = "Undelete a kek.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found")
  })
  @PerformanceMetric("keks.undelete")
  @DocumentedName("undeleteKek")
  public void undeleteKek(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String name) {

    log.debug("Undeleting kek {}", name);

    checkName(name);

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      KeyEncryptionKey kek = dekRegistry.getKek(name, true);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(name);
      }

      dekRegistry.undeleteKekOrForward(name, headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (KeyReferenceExistsException e) {
      throw DekRegistryErrors.referenceExistsException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while undeleting key", e);
    }
  }

  @POST
  @Path("/{name}/deks/{subject}/undelete")
  @Operation(summary = "Undelete all versions of a dek.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found")
  })
  @PerformanceMetric("deks.undelete")
  @DocumentedName("undeleteDekVersions")
  public void undeleteDekVersions(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Algorithm of the dek")
      @QueryParam("algorithm") DekFormat algorithm) {

    log.debug("Undeleting dek {} for kek {}", subject, kekName);

    checkName(kekName);
    checkSubject(subject);

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      KeyEncryptionKey kek = dekRegistry.getKek(kekName, true);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(kekName);
      }
      DataEncryptionKey key = dekRegistry.getLatestDek(kekName, subject, algorithm, true);
      if (key == null) {
        throw DekRegistryErrors.keyNotFoundException(subject);
      }

      dekRegistry.undeleteDekOrForward(kekName, subject, algorithm, headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (KeySoftDeletedException e) {
      throw DekRegistryErrors.keySoftDeletedException(e.getName());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while undeleting key", e);
    }
  }

  @POST
  @Path("/{name}/deks/{subject}/versions/{version}/undelete")
  @Operation(summary = "Undelete a dek version.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "422", description = "Error code 42202 -- Invalid version")
  })
  @PerformanceMetric("deks.versions.undelete")
  @DocumentedName("undeleteDekVersion")
  public void undeleteDekVersion(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "Version of the dek", required = true)
      @PathParam("version") String version,
      @Parameter(description = "Algorithm of the dek")
      @QueryParam("algorithm") DekFormat algorithm) {

    log.debug("Undeleting dek {} for kek {}", subject, kekName);

    checkName(kekName);
    checkSubject(subject);
    VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      KeyEncryptionKey kek = dekRegistry.getKek(kekName, true);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(kekName);
      }
      DataEncryptionKey key = dekRegistry.getDek(
          kekName, subject, versionId.getVersionId(), algorithm, true);
      if (key == null) {
        throw DekRegistryErrors.keyNotFoundException(subject);
      }

      dekRegistry.undeleteDekVersionOrForward(
          kekName, subject, versionId.getVersionId(), algorithm, headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (KeySoftDeletedException e) {
      throw DekRegistryErrors.keySoftDeletedException(e.getName());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while undeleting key", e);
    }
  }

  private static void checkName(String name) {
    if (name == null || name.isEmpty()) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("name");
    }
    if (name.length() > NAME_MAX_LENGTH) {
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
    if (subject == null || subject.isEmpty()
        || CharMatcher.javaIsoControl().matchesAnyOf(subject)) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("subject");
    }
  }
}
