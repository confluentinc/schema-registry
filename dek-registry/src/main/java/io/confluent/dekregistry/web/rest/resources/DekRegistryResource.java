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
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
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

  public static final int NAME_MAX_LENGTH = 256;

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
      @Parameter(description = "Subject name prefix")
      @QueryParam("subjectPrefix") List<String> subjectPrefix,
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {
    limit = dekRegistry.normalizeKekLimit(limit);
    List<String> kekNames = dekRegistry.getKekNames(subjectPrefix, lookupDeleted);
    return kekNames.stream()
      .skip(offset)
      .limit(limit)
      .collect(Collectors.toList());
  }

  @GET
  @Path("/{name}")
  @Operation(summary = "Get a kek by name.", responses = {
      @ApiResponse(responseCode = "200", description = "The kek info",
          content = @Content(schema = @Schema(implementation = Kek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key")
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
    return dekRegistry.toKekEntity(key);
  }

  @GET
  @Path("/{name}/deks")
  @Operation(summary = "Get a list of dek subjects.", responses = {
      @ApiResponse(responseCode = "200",
          description = "List of dek subjects", content = @Content(
          array = @ArraySchema(schema = @io.swagger.v3.oas.annotations.media.Schema(
              example = "User")))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key")
  })
  @PerformanceMetric("deks.list")
  @DocumentedName("getDekSubjects")
  public List<String> getDekSubjects(
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Whether to include deleted keys")
      @QueryParam("deleted") boolean lookupDeleted,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {

    checkName(kekName);

    KeyEncryptionKey key = dekRegistry.getKek(kekName, lookupDeleted);
    if (key == null) {
      throw DekRegistryErrors.keyNotFoundException(kekName);
    }
    limit = dekRegistry.normalizeDekSubjectLimit(limit);
    List<String> dekSubjects = dekRegistry.getDekSubjects(kekName, lookupDeleted);
    return dekSubjects.stream()
      .skip(offset)
      .limit(limit)
      .collect(Collectors.toList());
  }

  @GET
  @Path("/{name}/deks/{subject}")
  @Operation(summary = "Get a dek by subject.", responses = {
      @ApiResponse(responseCode = "200", description = "The dek info",
          content = @Content(schema = @Schema(implementation = Dek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key"),
      @ApiResponse(responseCode = "500", description = "Error code 50070 -- Dek generation error")
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
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key")
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
      @QueryParam("deleted") boolean lookupDeleted,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {

    checkName(kekName);
    checkSubject(subject);

    KeyEncryptionKey kek = dekRegistry.getKek(kekName, lookupDeleted);
    if (kek == null) {
      throw DekRegistryErrors.keyNotFoundException(kekName);
    }
    limit = dekRegistry.normalizeDekVersionLimit(limit);
    List<Integer> dekVersions = dekRegistry.getDekVersions(
            kekName, subject, algorithm, lookupDeleted);
    return dekVersions.stream()
      .skip(offset)
      .limit(limit)
      .collect(Collectors.toList());
  }

  @GET
  @Path("/{name}/deks/{subject}/versions/{version}")
  @Operation(summary = "Get a dek by subject and version.", responses = {
      @ApiResponse(responseCode = "200", description = "The dek info",
          content = @Content(schema = @Schema(implementation = Dek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "422", description = "Unprocessable entity. "
          + "Error code 42202 -- Invalid version. "
          + "Error code 42271 -- Invalid key."),
      @ApiResponse(responseCode = "500", description = "Error code 50070 -- Dek generation error")
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
      @ApiResponse(responseCode = "409", description = "Conflict. "
          + "Error code 40971 -- Key already exists. "
          + "Error code 40972 -- Too many keys."),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key")
  })
  @PerformanceMetric("keks.create")
  @DocumentedName("registerKek")
  public void createKek(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Whether to test kek sharing")
      @QueryParam("testSharing") boolean testSharing,
      @Parameter(description = "The create request", required = true)
      @NotNull CreateKekRequest request) {

    log.debug("Creating kek {}", request.getName());

    checkName(request.getName());

    if (request.getKmsKeyId() == null || request.getKmsKeyId().isEmpty()) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("kmsKeyId");
    } else if (request.getKmsType() == null) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("kmsType");
    }

    try {
      request.validate();
    } catch (Exception e) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("kmsKeyId");
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      if (request.isShared() && testSharing) {
        KeyEncryptionKey kek = new KeyEncryptionKey(request.getName(), request.getKmsType(),
            request.getKmsKeyId(), new TreeMap<>(request.getKmsProps()), null, true, false);
        dekRegistry.testKek(kek);
      }

      Kek kek = dekRegistry.createKekOrForward(request, headerProperties);
      asyncResponse.resume(kek);
    } catch (AlreadyExistsException e) {
      throw DekRegistryErrors.alreadyExistsException(e.getMessage());
    } catch (TooManyKeysException e) {
      throw DekRegistryErrors.tooManyKeysException(dekRegistry.config().maxKeys());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while creating key: " + e.getMessage(), e);
    }
  }

  @POST
  @Path("/{name}/test")
  @Operation(summary = "Test a kek.", responses = {
      @ApiResponse(responseCode = "200", description = "The test response",
          content = @Content(schema = @Schema(implementation = Kek.class))),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key"),
      @ApiResponse(responseCode = "500", description = "Error code 50070 -- Dek generation error")
  })
  @PerformanceMetric("keks.test")
  @DocumentedName("testKek")
  public void testKek(
      final @Suspended AsyncResponse asyncResponse,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName) {

    log.debug("Testing kek {}", kekName);

    checkName(kekName);

    KeyEncryptionKey kek = dekRegistry.getKek(kekName, false);
    if (kek == null) {
      throw DekRegistryErrors.keyNotFoundException(kekName);
    }

    try {
      dekRegistry.testKek(kek);
      asyncResponse.resume(kek);
    } catch (DekGenerationException e) {
      throw DekRegistryErrors.dekGenerationException(e.getMessage());
    } catch (InvalidKeyException e) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while testing key", e);
    }
  }

  // The following method is deprecated in favor of the API that takes subject as a path parameter.
  @Deprecated
  @POST
  @Path("/{name}/deks")
  @Operation(summary = "Create a dek.", responses = {
      @ApiResponse(responseCode = "200", description = "The create response",
          content = @Content(schema = @Schema(implementation = Dek.class))),
      @ApiResponse(responseCode = "409", description = "Conflict. "
          + "Error code 40971 -- Key already exists. "
          + "Error code 40972 -- Too many keys."),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key"),
      @ApiResponse(responseCode = "500", description = "Error code 50070 -- Dek generation error")
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
    createDekWithSubject(asyncResponse, headers, kekName, request.getSubject(), request);
  }

  @POST
  @Path("/{name}/deks/{subject}")
  @Operation(summary = "Create a dek.", responses = {
      @ApiResponse(responseCode = "200", description = "The create response",
          content = @Content(schema = @Schema(implementation = Dek.class))),
      @ApiResponse(responseCode = "409", description = "Conflict. "
          + "Error code 40971 -- Key already exists. "
          + "Error code 40972 -- Too many keys."),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key"),
      @ApiResponse(responseCode = "500", description = "Error code 50070 -- Dek generation error")
  })
  @PerformanceMetric("deks.create")
  @DocumentedName("registerDekWithSubject")
  public void createDekWithSubject(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String kekName,
      @Parameter(description = "Subject of the dek", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "The create request", required = true)
      @NotNull CreateDekRequest request) {


    log.debug("Creating dek {} for kek {}", subject, kekName);

    if (request.getSubject() != null
        && !request.getSubject().isEmpty()
        && !subject.equals(request.getSubject())) {
      throw DekRegistryErrors.invalidOrMissingKeyInfo("subject");
    }

    checkName(kekName);
    checkSubject(subject);

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
      throw Errors.schemaRegistryException("Error while creating key: " + e.getMessage(), e);
    }
  }

  @PUT
  @Path("/{name}")
  @Operation(summary = "Alters a kek.", responses = {
      @ApiResponse(responseCode = "200", description = "The update response",
          content = @Content(schema = @Schema(implementation = Kek.class))),
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "409", description = "Error code 40971 -- Key already exists"),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key")
  })
  @PerformanceMetric("keks.put")
  @DocumentedName("updateKek")
  public void putKek(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "Name of the kek", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Whether to test kek sharing")
      @QueryParam("testSharing") boolean testSharing,
      @Parameter(description = "The update request", required = true)
      @NotNull UpdateKekRequest request) {

    log.debug("Updating kek {}", name);

    checkName(name);

    KeyEncryptionKey oldKek = dekRegistry.getKek(name, false);
    if (oldKek == null) {
      throw DekRegistryErrors.keyNotFoundException(name);
    }
    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      boolean shared = request.isShared() != null ? request.isShared() : oldKek.isShared();
      if (shared && testSharing) {
        SortedMap<String, String> kmsProps = request.getKmsProps() != null
            ? new TreeMap<>(request.getKmsProps())
            : oldKek.getKmsProps();
        KeyEncryptionKey newKek = new KeyEncryptionKey(name, oldKek.getKmsType(),
            oldKek.getKmsKeyId(), kmsProps, null, true, false);
        dekRegistry.testKek(newKek);
      }

      Kek kek = dekRegistry.putKekOrForward(name, request, headerProperties);
      if (kek == null) {
        throw DekRegistryErrors.keyNotFoundException(name);
      }
      asyncResponse.resume(kek);
    } catch (AlreadyExistsException e) {
      throw DekRegistryErrors.alreadyExistsException(e.getMessage());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while creating key: " + e.getMessage(), e);
    }
  }

  @DELETE
  @Path("/{name}")
  @Operation(summary = "Delete a kek.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Not found. "
          + "Error code 40470 -- Key not found. "
          + "Error code 40471 -- Key not soft-deleted."),
      @ApiResponse(responseCode = "422", description = "Unprocessable entity. "
          + "Error code 42271 -- Invalid key. "
          + "Error code 42272 -- References to key exist."),
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
      @ApiResponse(responseCode = "404", description = "Not found. "
          + "Error code 40470 -- Key not found. "
          + "Error code 40471 -- Key not soft-deleted."),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key")
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
      @ApiResponse(responseCode = "404", description = "Not found. "
          + "Error code 40470 -- Key not found. "
          + "Error code 40471 -- Key not soft-deleted."),
      @ApiResponse(responseCode = "422", description = "Unprocessable entity. "
          + "Error code 42202 -- Invalid version. "
          + "Error code 42271 -- Invalid key."),
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
      @ApiResponse(responseCode = "404", description = "Error code 40470 -- Key not found"),
      @ApiResponse(responseCode = "422", description = "Unprocessable entity. "
          + "Error code 42271 -- Invalid key. "
          + "Error code 42272 -- References to key exist."),
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
      @ApiResponse(responseCode = "404", description = "Not found. "
          + "Error code 40470 -- Key not found. "
          + "Error code 40472 -- Key must be undeleted."),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid key")
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
      @ApiResponse(responseCode = "404", description = "Not found. "
          + "Error code 40470 -- Key not found. "
          + "Error code 40472 -- Key must be undeleted."),
      @ApiResponse(responseCode = "422", description = "Unprocessable entity. "
          + "Error code 42202 -- Invalid version. "
          + "Error code 42271 -- Invalid key."),
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
