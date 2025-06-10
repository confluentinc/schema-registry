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

package io.confluent.dpregistry.web.rest.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.dpregistry.client.rest.entities.DataProduct;
import io.confluent.dpregistry.storage.DataProductKey;
import io.confluent.dpregistry.storage.exceptions.DataProductNotSoftDeletedException;
import io.confluent.dpregistry.web.rest.exceptions.DataProductRegistryErrors;
import io.confluent.dpregistry.client.rest.entities.RegisteredDataProduct;
import io.confluent.dpregistry.storage.DataProductRegistry;
import io.confluent.dpregistry.storage.DataProductValue;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.builtin.NativeSchema;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.rest.resources.DocumentedName;
import io.confluent.kafka.schemaregistry.rest.resources.RequestHeaderBuilder;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;
import io.kcache.KeyValue;
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
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;

@Path("/dataproduct-registry/v1/environments/{env}/clusters/{cluster}/dataproducts")
@Singleton
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class DataProductRegistryResource extends SchemaRegistryResource {

  private static final Logger log = LoggerFactory.getLogger(DataProductRegistryResource.class);

  public static final int NAME_MAX_LENGTH = 256;

  public static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  private final DataProductRegistry dataProductRegistry;
  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  @Inject
  public DataProductRegistryResource(SchemaRegistry schemaRegistry,
      DataProductRegistry dataProductRegistry) {
    super(schemaRegistry);
    this.dataProductRegistry = dataProductRegistry;
  }

  @GET
  @Operation(summary = "Get a list of data product names.", responses = {
      @ApiResponse(responseCode = "200",
          description = "List of data product names", content = @Content(
          array = @ArraySchema(schema = @Schema(
              example = "my-data-product")))),
  })
  @PerformanceMetric("dataproducts.list")
  @DocumentedName("getDataProductNames")
  public List<String> getDataProductNames(
      @Parameter(description = "The environment", required = true)
      @PathParam("env") String env,
      @Parameter(description = "The cluster", required = true)
      @PathParam("cluster") String cluster,
      @Parameter(description = "Whether to include deleted data products")
      @QueryParam("deleted") boolean lookupDeleted,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {

    log.info("get data product for cluster {}", cluster);

    limit = dataProductRegistry.normalizeNameSearchLimit(limit);
    List<String> dataProductNames = dataProductRegistry.getDataProductNames(
        env, cluster, lookupDeleted);
    return dataProductNames.stream()
      .skip(offset)
      .limit(limit)
      .collect(Collectors.toList());
  }

  @GET
  @Path("/{name}/versions")
  @Operation(summary = "List versions of data product.", responses = {
      @ApiResponse(responseCode = "200",
          description = "List of version numbers for data product",
          content = @Content(array = @ArraySchema(
              schema = @Schema(type = "integer",
                  format = "int32", example = "1")))),
      @ApiResponse(responseCode = "404",
          description = "Error code 40470 -- Data product not found"),
      @ApiResponse(responseCode = "422", description = "Error code 42271 -- Invalid data product")
  })
  @PerformanceMetric("dataproducts.versions.list")
  @DocumentedName("getDataProductVersions")
  public List<Integer> getDataProductVersions(
      @Parameter(description = "The environment", required = true)
      @PathParam("env") String env,
      @Parameter(description = "The cluster", required = true)
      @PathParam("cluster") String cluster,
      @Parameter(description = "Name of the data product", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Whether to include deleted data products")
      @QueryParam("deleted") boolean lookupDeleted,
      @Parameter(description = "Pagination offset for results")
      @DefaultValue("0") @QueryParam("offset") int offset,
      @Parameter(description = "Pagination size for results. Ignored if negative")
      @DefaultValue("-1") @QueryParam("limit") int limit) {

    checkName(name);

    limit = dataProductRegistry.normalizeVersionSearchLimit(limit);
    List<Integer> versions = dataProductRegistry.getDataProductVersions(
        env, cluster, name, lookupDeleted);
    return versions.stream()
      .skip(offset)
      .limit(limit)
      .collect(Collectors.toList());
  }

  @GET
  @Path("/{name}/versions/{version}")
  @Operation(summary = "Get a data product by name and version.", responses = {
      @ApiResponse(responseCode = "200", description = "The data product info",
          content = @Content(schema = @Schema(implementation = RegisteredDataProduct.class))),
      @ApiResponse(responseCode = "404",
          description = "Error code 40470 -- Data product not found")
  })
  @PerformanceMetric("dataproducts.versions.get")
  @DocumentedName("getDataProductVersion")
  public RegisteredDataProduct getDataProductByVersion(
      @Parameter(description = "The environment", required = true)
      @PathParam("env") String env,
      @Parameter(description = "The cluster", required = true)
      @PathParam("cluster") String cluster,
      @Parameter(description = "Name of the data product", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Version of the dek", required = true)
      @PathParam("version") String version,
      @Parameter(description = "Desired output format, dependent on schema type")
      @DefaultValue("") @QueryParam("format") String format,
      @Parameter(description = "Whether to include deleted data products")
      @QueryParam("deleted") boolean lookupDeleted) {

    checkName(name);
    VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }

    try {
      DataProductValue value = dataProductRegistry.getDataProduct(
          env, cluster, name, versionId.getVersionId(), lookupDeleted);
      if (value == null) {
        throw DataProductRegistryErrors.dataProductNotFoundException(name);
      }
      RegisteredDataProduct product = value.toEntity();
      if (product.getSchemas() != null && format != null && !format.trim().isEmpty()) {
        if (format.equalsIgnoreCase("flink")) {
          formatFlink(product, format);
        } else {
          formatKeyAndValue(product, format);
        }
      }
      return product;
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while retrieving data product", e);
    }
  }

  private void formatFlink(RegisteredDataProduct product, String format)
      throws InvalidSchemaException {
    try {
      ObjectNode root = MAPPER.createObjectNode();
      ObjectNode schemaNode = root.putObject("schema");
      ArrayNode fieldsNode = schemaNode.putArray("fields");
      // For the demo, assume key and value schemas are native schema structs if they exist
      if (product.getSchemas().getKey() != null) {
        ParsedSchema keySchema = ((KafkaSchemaRegistry) getSchemaRegistry()).parseSchema(
            product.getSchemas().getKey(), false, false);
        formatFlinkFields(keySchema, fieldsNode);
      }
      if (product.getSchemas().getValue() != null) {
        ParsedSchema valueSchema = ((KafkaSchemaRegistry) getSchemaRegistry()).parseSchema(
            product.getSchemas().getValue(), false, false);
        formatFlinkFields(valueSchema, fieldsNode);
      }
      ObjectNode configsNode = root.putObject("options");
      if (product.getConfigs() != null) {
        product.getConfigs().forEach(configsNode::put);
      }
      product.getSchemas().setKey(null);
      product.getSchemas().getValue().setSchema(MAPPER.writeValueAsString(root));
    } catch (Exception e) {
      throw new InvalidSchemaException(
          "Error formatting data product for Flink: " + e.getMessage(), e);
    }
  }

  private static void formatFlinkFields(ParsedSchema schema, ArrayNode fieldsNode) {
    if (schema instanceof NativeSchema) {
      io.confluent.kafka.schemaregistry.builtin.Schema s =
          (io.confluent.kafka.schemaregistry.builtin.Schema) schema.rawSchema();
      if (s.getType() == io.confluent.kafka.schemaregistry.builtin.Schema.Type.STRUCT) {
        s.getFields().forEach(field -> {
          NativeSchema fs = new NativeSchema(field.schema());
          fieldsNode.addObject()
              .put("name", field.name())
              .put("type", fs.formattedString("flink"));
        });
      }
    }
  }

  private void formatKeyAndValue(RegisteredDataProduct product, String format)
      throws InvalidSchemaException {
    if (product.getSchemas().getKey() != null) {
      ParsedSchema keySchema = ((KafkaSchemaRegistry) getSchemaRegistry()).parseSchema(
          product.getSchemas().getKey(), false, false);
      String formattedKey = keySchema.formattedString(format);
      product.getSchemas().getKey().setSchema(formattedKey);
    }
    if (product.getSchemas().getValue() != null) {
      ParsedSchema valueSchema = ((KafkaSchemaRegistry) getSchemaRegistry()).parseSchema(
          product.getSchemas().getValue(), false, false);
      String formattedValue = valueSchema.formattedString(format);
      product.getSchemas().getValue().setSchema(formattedValue);
    }
  }

  @POST
  @Operation(summary = "Create a data product.", responses = {
      @ApiResponse(responseCode = "200", description = "The create response",
          content = @Content(schema = @Schema(implementation = RegisteredDataProduct.class))),
      @ApiResponse(responseCode = "409", description = "Conflict. "
          + "Error code 40971 -- Data product already exists.")
  })
  @PerformanceMetric("dataproducts.create")
  @DocumentedName("registerDataProduct")
  public void createDataProduct(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "The environment", required = true)
      @PathParam("env") String env,
      @Parameter(description = "The cluster", required = true)
      @PathParam("cluster") String cluster,
      @Parameter(description = "The create request", required = true)
      @NotNull DataProduct request) {

    log.debug("Creating data product {}", request.getInfo().getName());

    checkName(request.getInfo().getName());

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      RegisteredDataProduct product = dataProductRegistry.createDataProductOrForward(
          env, cluster, request, headerProperties);

      createTopicIfNotExists(env, cluster, request);

      asyncResponse.resume(product);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(
          "Error while creating data product: " + e.getMessage(), e);
    }
  }

  private void createTopicIfNotExists(String env, String cluster, DataProduct product) {
    Properties props = new Properties();
    product.getConfigs().forEach((key, value) -> {
      if (key.startsWith("properties.")) {
        String propKey = key.substring("properties.".length());
        props.put(propKey, value);
      }
    });

    //S et the security protocol and password.
    props.put(SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    props.put("sasl.mechanism", "PLAIN"); // Or "SCRAM-SHA-256", etc.
    String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"owner\" " +
            "password=\"owner_password\";";
    props.put("sasl.jaas.config", jaasConfig);

    try (AdminClient admin = AdminClient.create(props)) {
      String topicName = product.getInfo().getName();
      // TODO RAY make configurable
      int numPartitions = 1;
      short replicationFactor = 1;
      int timeoutMs = 30000;
      NewTopic topicRequest = new NewTopic(topicName, numPartitions, replicationFactor);
      if (!admin.listTopics().names().get().contains(topicName)) {
        // Create topic with default settings
        admin.createTopics(Collections.singleton(topicRequest)).all().get(
            timeoutMs, TimeUnit.MILLISECONDS);
        log.info("Created topic {}", topicName);
      } else {
        log.info("Topic {} already exists", topicName);
      }
    } catch (Exception e) {
      throw Errors.schemaRegistryException("Error while creating topic for data product", e);
    }
  }

  @DELETE
  @Path("/{name}")
  @Operation(summary = "Delete all versions of a data product.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Not found. "
          + "Error code 40470 -- Data product not found. "
          + "Error code 40471 -- Data product not soft-deleted.")
  })
  @PerformanceMetric("dataproducts.delete")
  @DocumentedName("deleteDataProduct")
  public void deleteDataProduct(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "The environment", required = true)
      @PathParam("env") String env,
      @Parameter(description = "The cluster", required = true)
      @PathParam("cluster") String cluster,
      @Parameter(description = "Name of the data product", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Whether to perform a permanent delete")
      @QueryParam("permanent") boolean permanentDelete) {

    log.debug("Deleting data product {}", name);

    checkName(name);

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      KeyValue<DataProductKey, DataProductValue> product =
          dataProductRegistry.getLatestDataProduct(env, cluster, name);
      if (product == null) {
        throw DataProductRegistryErrors.dataProductNotFoundException(name);
      }

      dataProductRegistry.deleteDataProductOrForward(
          cluster, env, name, permanentDelete, headerProperties);

      //deleteTopic(env, cluster, product);

      asyncResponse.resume(Response.status(204).build());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting data product", e);
    }
  }

  private void deleteTopic(String env, String cluster, DataProductValue product) {
    Properties props = new Properties();
    product.getConfigs().forEach((key, value) -> {
      if (key.startsWith("properties.")) {
        String propKey = key.substring("properties.".length());
        props.put(propKey, value);
      }
    });
    try (AdminClient admin = AdminClient.create(props)) {
      String topicName = product.getInfo().getName();
      // delete topic if it exists
      if (admin.listTopics().names().get().contains(topicName)) {
        // Delete topic with default settings
        admin.deleteTopics(Collections.singleton(topicName)).all().get(
            30000, TimeUnit.MILLISECONDS);
        log.info("Deleted topic {}", topicName);
      } else {
        log.info("Topic {} does not exist", topicName);
      }
    } catch (Exception e) {
      throw Errors.schemaRegistryException("Error while deleting topic for data product", e);
    }
  }

  @DELETE
  @Path("/{name}/versions/{version}")
  @Operation(summary = "Delete a data product version.", responses = {
      @ApiResponse(responseCode = "204", description = "No Content"),
      @ApiResponse(responseCode = "404", description = "Not found. "
          + "Error code 40470 -- Data product not found. "
          + "Error code 40471 -- Data product not soft-deleted.")
  })
  @PerformanceMetric("dataproducts.versions.delete")
  @DocumentedName("deleteDataProductVersion")
  public void deleteDataProductVersion(
      final @Suspended AsyncResponse asyncResponse,
      final @Context HttpHeaders headers,
      @Parameter(description = "The environment", required = true)
      @PathParam("env") String env,
      @Parameter(description = "The cluster", required = true)
      @PathParam("cluster") String cluster,
      @Parameter(description = "Name of the data product", required = true)
      @PathParam("name") String name,
      @Parameter(description = "Version of the data product", required = true)
      @PathParam("version") String version,
      @Parameter(description = "Whether to perform a permanent delete")
      @QueryParam("permanent") boolean permanentDelete) {

    log.debug("Deleting data product {}, version {}", name, version);

    checkName(name);
    VersionId versionId;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException(e.getMessage());
    }

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(
        headers, getSchemaRegistry().config().whitelistHeaders());

    try {
      DataProductValue product = dataProductRegistry.getDataProduct(
          env, cluster, name, versionId.getVersionId(), true);
      if (product == null) {
        throw DataProductRegistryErrors.dataProductNotFoundException(name);
      }

      dataProductRegistry.deleteDataProductVersionOrForward(
          env, cluster, name, versionId.getVersionId(), permanentDelete, headerProperties);
      asyncResponse.resume(Response.status(204).build());
    } catch (DataProductNotSoftDeletedException e) {
      throw DataProductRegistryErrors.dataProductNotSoftDeletedException(e.getName());
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException("Error while deleting data product", e);
    }
  }

  private static void checkName(String name) {
    if (name == null || name.isEmpty()) {
      throw DataProductRegistryErrors.invalidOrMissingDataProduct("name");
    }
    if (name.length() > NAME_MAX_LENGTH) {
      throw DataProductRegistryErrors.invalidOrMissingDataProduct("name");
    }
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      throw DataProductRegistryErrors.invalidOrMissingDataProduct("name");
    }
    for (int i = 1; i < name.length(); i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_' || c == '-')) {
        throw DataProductRegistryErrors.invalidOrMissingDataProduct("name");
      }
    }
  }
}
