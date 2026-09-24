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

import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceAlgorithm;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import java.util.OptionalInt;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.provenance.RecursiveTypeException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.AssociationForSubjectExistsException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.ReferenceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.SubjectNotFoundException;
import io.confluent.kafka.schemaregistry.exceptions.SubjectNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.SubjectSoftDeletedException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.LookupFilter;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
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

import jakarta.inject.Inject;
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
import java.util.Objects;
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
  private final SchemaRegistry schemaRegistry;
  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  /** Provenance ranges retained. Each holds a report over many versions, so the bound is low. */
  private static final int MAX_CACHED_PROVENANCE_HISTORIES = 100;

  /**
   * Provenance over one requested range, keyed by the subject, the mode, and every
   * (version, schema id) in the range, soft-deleted versions included. It is computed over the
   * range alone — its ends and everything between — so a version outside it, however old or
   * however broken, has no effect. Any registration or deletion inside the range changes the key,
   * so nothing needs invalidating, and many readers asking for the same range collapse into one
   * computation per node.
   */
  private final Cache<List<Object>, SchemaProvenance> provenanceCache =
      Caffeine.newBuilder().maximumSize(MAX_CACHED_PROVENANCE_HISTORIES).build();

  @Inject
  public SubjectsResource(SchemaRegistry schemaRegistry) {
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

    io.confluent.kafka.schemaregistry.client.rest.entities.Schema matchingSchema;
    try {
      // returns version if the schema exists. Otherwise returns 404
      Schema schema = new Schema(subject, request);
      if (!normalize) {
        normalize = Boolean.TRUE.equals(schemaRegistry.getConfigInScope(subject).isNormalize());
      }
      matchingSchema = schemaRegistry.lookUpSchemaUnderSubjectUsingContexts(
          subject, schema, normalize, lookupDeletedSchema);
      if (matchingSchema == null) {
        if (!schemaRegistry.hasSubjects(subject, lookupDeletedSchema)) {
          throw Errors.subjectNotFoundException(subject);
        } else {
          throw Errors.schemaNotFoundException();
        }
      }
      if (format != null && !format.trim().isEmpty()) {
        // Schema.setSchema(...) nulls the guid, and getGuid() then recomputes it as an MD5 of the
        // new schema string -- so capture the real guid up front and restore it after rendering,
        // whether the body is logical DDL or a native-formatter output.
        String originalGuid = matchingSchema.getGuid();
        if (LogicalFormat.isLogical(format)) {
          matchingSchema.setSchema(LogicalFormat.convertToLogical(schemaRegistry, matchingSchema));
        } else {
          ParsedSchema parsedSchema = schemaRegistry.parseSchema(matchingSchema, false, false);
          matchingSchema.setSchema(parsedSchema.formattedString(format));
        }
        matchingSchema.setGuid(originalGuid);
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
        // Schema.setSchema(...) nulls the guid, and getGuid() then recomputes it as an MD5 of the
        // new schema string -- so capture the real guid up front and restore it after rendering,
        // whether the body is logical DDL or a native-formatter output.
        String originalGuid = matchingSchema.getGuid();
        if (LogicalFormat.isLogical(format)) {
          matchingSchema.setSchema(LogicalFormat.convertToLogical(schemaRegistry, matchingSchema));
        } else {
          ParsedSchema parsedSchema = schemaRegistry.parseSchema(matchingSchema, false, false);
          matchingSchema.setSchema(parsedSchema.formattedString(format));
        }
        matchingSchema.setGuid(originalGuid);
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

  @GET
  @Path("/{subject}/provenance")
  @DocumentedName("getProvenance")
  @PerformanceMetric("subjects.provenance.get")
  @Operation(summary = "Get column provenance between two versions",
      description = "Retrieves, for each version in the range, every column's inlined path and a "
          + "provenance id that is stable across renames. Address the range either by version, "
          + "with fromVersion and toVersion, or by schema id, with fromId and toId.",
      responses = {
          @ApiResponse(responseCode = "200", description = "The provenance.",
              content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = SchemaProvenance.class))),
          @ApiResponse(responseCode = "404",
              description = "Not Found. Error code 40401 indicates subject not found. "
                  + "Error code 40402 indicates version not found. Error code 40411 indicates "
                  + "a schema id with no version under the subject.",
              content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "422",
              description = "Unprocessable Entity. Error code 42201 indicates a schema with no "
                  + "logical form. Error code 42202 indicates an invalid version. Error code "
                  + "42213 indicates a recursive schema. Error code 42214 indicates a schema "
                  + "that could not be parsed. Error code 42215 indicates an invalid range. "
                  + "Error code 42216 indicates an unknown algorithm.",
              content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class))),
          @ApiResponse(responseCode = "500",
              description = "Internal Server Error. "
                  + "Error code 50001 indicates a failure in the backend data store.",
              content = @Content(schema = @io.swagger.v3.oas.annotations.media.Schema(
                  implementation = ErrorMessage.class)))})
  @Tags(@Tag(name = apiTag))
  public SchemaProvenance getProvenance(
      @Parameter(description = "Name of the subject", required = true)
      @PathParam("subject") String subject,
      @Parameter(description = "First version of the range, or \"latest\"")
      @QueryParam("fromVersion") String fromVersion,
      @Parameter(description = "Last version of the range, or \"latest\"")
      @QueryParam("toVersion") String toVersion,
      @Parameter(description = "Schema id at one end of the range")
      @QueryParam("fromId") Integer fromId,
      @Parameter(description = "Schema id at the other end of the range")
      @QueryParam("toId") Integer toId,
      @Parameter(description = "Whether to return every version in the range, not only its ends")
      @DefaultValue("false") @QueryParam("includeInterior") boolean includeInterior,
      @Parameter(description = "Whether to root each Protobuf version at a struct over all its "
          + "top-level messages; ignored for other formats")
      @DefaultValue("false") @QueryParam("includeMultipleMessages")
      boolean includeMultipleMessages,
      @Parameter(description = "Version of the provenance algorithm, such as v1; the latest when "
          + "omitted")
      @QueryParam("algorithm") String algorithm) {

    subject = QualifiedSubject.normalize(schemaRegistry.tenant(), subject);
    boolean byVersion = fromVersion != null || toVersion != null;
    boolean byId = fromId != null || toId != null;
    if (byVersion == byId) {
      throw Errors.invalidProvenanceRequestException(
          "Name the range either by fromVersion and toVersion or by fromId and toId.");
    }
    if (byVersion ? fromVersion == null || toVersion == null : fromId == null || toId == null) {
      throw Errors.invalidProvenanceRequestException("Name both ends of the range.");
    }
    ProvenanceAlgorithm version;
    try {
      version = ProvenanceAlgorithm.of(algorithm);
    } catch (IllegalArgumentException e) {
      throw Errors.unknownProvenanceAlgorithmException(e.getMessage());
    }

    String errorMessage = "Error while computing provenance for subject " + subject;
    try {
      return byVersion
          ? provenanceByVersion(subject, fromVersion, toVersion, includeInterior,
              includeMultipleMessages, version)
          : provenanceById(subject, fromId, toId, includeInterior,
              includeMultipleMessages, version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    } catch (SchemaRegistryStoreException e) {
      log.debug(errorMessage, e);
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
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
      Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
          headers, schemaRegistry.config().whitelistHeaders());
      deletedVersions = schemaRegistry.deleteSubjectOrForward(headerProperties,
              subject,
              permanentDelete);
    } catch (AssociationForSubjectExistsException e) {
      throw Errors.associationForSubjectExistsException(e.getMessage());
    } catch (ReferenceExistsException e) {
      throw Errors.referenceExistsException(e.getMessage());
    } catch (SubjectNotSoftDeletedException e) {
      throw Errors.subjectNotSoftDeletedException(subject);
    } catch (SubjectNotFoundException e) {
      throw Errors.subjectNotFoundException(subject);
    } catch (SubjectSoftDeletedException e) {
      throw Errors.subjectSoftDeletedException(subject);
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


  /**
   * The range between two versions, each a version number or {@code "latest"}.
   */
  private SchemaProvenance provenanceByVersion(String subject, String fromVersion, String toVersion,
      boolean includeInterior, boolean includeMultipleMessages,
      ProvenanceAlgorithm algorithm) throws SchemaRegistryException, InvalidVersionException {
    List<Schema> history = nonEmptyProvenanceHistory(subject);
    List<ProvenanceHistory.Entry> entries = provenanceEntries(history);
    return provenanceOf(subject, history, versionNamed(fromVersion, entries),
        versionNamed(toVersion, entries), includeInterior, includeMultipleMessages,
        algorithm);
  }

  /**
   * The range between the versions carrying two schema ids, in either order.
   */
  private SchemaProvenance provenanceById(String subject, int fromId, int toId,
      boolean includeInterior, boolean includeMultipleMessages,
      ProvenanceAlgorithm algorithm) throws SchemaRegistryException {
    List<Schema> history = nonEmptyProvenanceHistory(subject);
    List<ProvenanceHistory.Entry> entries = provenanceEntries(history);
    return provenanceOf(subject, history, versionOfId(fromId, subject, entries),
        versionOfId(toId, subject, entries), includeInterior, includeMultipleMessages,
        algorithm);
  }

  private List<Schema> nonEmptyProvenanceHistory(String subject) throws SchemaRegistryException {
    List<Schema> history = provenanceHistory(subject);
    if (history.isEmpty()) {
      throw Errors.subjectNotFoundException(subject);
    }
    return history;
  }

  private SchemaProvenance provenanceOf(String subject, List<Schema> history, int from, int to,
      boolean includeInterior, boolean includeMultipleMessages,
      ProvenanceAlgorithm algorithm) {
    int low = Math.min(from, to);
    int high = Math.max(from, to);
    List<Schema> range = history.stream()
        .filter(s -> s.getVersion() >= low && s.getVersion() <= high)
        .collect(Collectors.toList());
    // computeProvenance never returns null, so neither does the cache.
    SchemaProvenance whole = Objects.requireNonNull(provenanceCache.get(
        provenanceKey(subject, range, includeMultipleMessages, algorithm),
        k -> computeProvenance(subject, range, includeMultipleMessages, algorithm)));
    return ProvenanceHistory.slice(whole, from, to, includeInterior);
  }

  /**
   * Every version of {@code subject}, soft-deleted ones included, in version order.
   */
  private List<Schema> provenanceHistory(String subject) throws SchemaRegistryException {
    List<Schema> history = new ArrayList<>();
    Iterator<SchemaKey> keys = schemaRegistry.getAllVersions(subject, LookupFilter.INCLUDE_DELETED);
    while (keys.hasNext()) {
      Schema schema = schemaRegistry.get(subject, keys.next().getVersion(), true);
      if (schema != null) {
        history.add(schema);
      }
    }
    history.sort(Comparator.comparing(Schema::getVersion));
    return history;
  }

  private static List<ProvenanceHistory.Entry> provenanceEntries(List<Schema> history) {
    return history.stream()
        .map(s -> new ProvenanceHistory.Entry(
            s.getVersion(), s.getId(), Boolean.TRUE.equals(s.getDeleted())))
        .collect(Collectors.toList());
  }

  /**
   * A version number or {@code "latest"}, which means the latest version not soft-deleted.
   */
  private static int versionNamed(String version, List<ProvenanceHistory.Entry> entries)
      throws InvalidVersionException {
    VersionId id = new VersionId(version);
    OptionalInt resolved = id.isLatest()
        ? ProvenanceHistory.latestVersion(entries)
        : ProvenanceHistory.version(entries, id.getVersionId());
    if (!resolved.isPresent()) {
      throw Errors.versionNotFoundException(id.getVersionId());
    }
    return resolved.getAsInt();
  }

  private static int versionOfId(int id, String subject, List<ProvenanceHistory.Entry> entries) {
    OptionalInt version = ProvenanceHistory.versionCarrying(entries, id);
    if (!version.isPresent()) {
      throw Errors.schemaIdNotInSubjectException(id, subject);
    }
    return version.getAsInt();
  }

  private static List<Object> provenanceKey(String subject, List<Schema> history,
      boolean includeMultipleMessages, ProvenanceAlgorithm algorithm) {
    // The mode and the algorithm are part of the key: each answers differently.
    List<Object> key = new ArrayList<>(3 + 2 * history.size());
    key.add(subject);
    key.add(includeMultipleMessages);
    key.add(algorithm);
    for (Schema schema : history) {
      key.add(schema.getVersion());
      key.add(schema.getId());
    }
    return key;
  }

  private SchemaProvenance computeProvenance(String subject, List<Schema> history,
      boolean includeMultipleMessages, ProvenanceAlgorithm algorithm) {
    List<ParsedSchema> parsed = new ArrayList<>(history.size());
    for (Schema schema : history) {
      try {
        parsed.add(schemaRegistry.parseSchema(schema, false, false));
      } catch (InvalidSchemaException e) {
        throw Errors.unresolvableReferenceException("Version " + schema.getVersion()
            + " of subject " + subject + " could not be parsed: " + e.getMessage());
      }
    }
    try {
      return ProvenanceHistory.compute(
          subject, provenanceEntries(history), parsed, includeMultipleMessages, algorithm);
    } catch (RecursiveTypeException e) {
      throw Errors.recursiveSchemaException(e.getMessage());
    } catch (ValidationException e) {
      throw Errors.invalidSchemaException(e);
    } catch (RuntimeException e) {
      // Any other failure is this schema's, not the server's: a 4xx, so a client falls back and
      // caches that instead of retrying every record.
      throw Errors.invalidSchemaException(e);
    }
  }
}
