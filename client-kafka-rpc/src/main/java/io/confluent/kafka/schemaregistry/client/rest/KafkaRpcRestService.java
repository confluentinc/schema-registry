/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.schema.avro.AvroSerdeConfig;
import org.apache.kafka.schema.avro.KafkaRpcSchemaRegistryClient;
import org.apache.kafka.schema.avro.SchemaRegistryRpcClient;
import org.apache.kafka.server.schema.IncompatibleSchemaException;
import org.apache.kafka.server.schema.InvalidSchemaException;
import org.apache.kafka.server.schema.SchemaNotFoundException;

/**
 * A {@link RestService} that answers from Kafka RPCs instead of HTTP, so that
 * {@code KafkaAvroSerializer} and the rest of the existing serde stack work against a broker-hosted
 * registry with no separate endpoint configured.
 *
 * <p>Selected by the {@code kafka://} scheme on {@code schema.registry.url}; see
 * {@code io.confluent.kafka.schemaregistry.client.KafkaRpcUrls}. Because
 * {@link io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient} is
 * constructed around this class unchanged, its caching, subject-name strategies and
 * error handling all continue to apply.
 *
 * <p><b>Why the single {@code httpRequest} override.</b> Every REST call in
 * {@link RestService} funnels through {@link #httpRequest}, so overriding it intercepts all
 * of them. Overriding the individual semantic methods instead would be type-safe but would
 * leave any method that was missed falling through to real HTTP, failing with a connection
 * error against an endpoint that does not exist. Here an unhandled path fails immediately.
 *
 * <p>Responses are synthesised as JSON and deserialised through the caller's {@link TypeReference},
 * rather than by constructing entity objects directly. That keeps this class independent of the
 * entities' constructors and of fields it does not populate.
 */
public class KafkaRpcRestService extends RestService {

  /**
   * Never contacted. {@link #httpRequest} is overridden, so no HTTP request is issued.
   */
  private static final String UNUSED_BASE_URL = "http://kafka-rpc.invalid";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Pattern SUBJECT_VERSIONS =
      Pattern.compile("/subjects/([^/]+)/versions/?");
  private static final Pattern SUBJECT_VERSION_LATEST =
      Pattern.compile("/subjects/([^/]+)/versions/latest/?");
  private static final Pattern SUBJECT_VERSION_NUMBER =
      Pattern.compile("/subjects/([^/]+)/versions/(-?\\d+)/?");
  private static final Pattern SUBJECT_LOOKUP =
      Pattern.compile("/subjects/([^/]+)/?");
  private static final Pattern SCHEMA_BY_ID =
      Pattern.compile("/schemas/ids/(\\d+)/?");
  private static final Pattern SCHEMA_BY_GUID =
      Pattern.compile("/schemas/guids/([^/]+)/?");
  private static final Pattern ASSOCIATIONS_BY_RESOURCE_NAME =
      Pattern.compile("/associations/resources/([^/]+)/([^/]+)/?");

  private static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;
  private static final int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;
  private static final int INVALID_SCHEMA_ERROR_CODE = 42201;
  private static final int INCOMPATIBLE_SCHEMA_ERROR_CODE = 409;

  private final SchemaRegistryRpcClient rpcClient;

  /**
   * @param bootstrapServers brokers to send schema requests to
   * @param configs          the client's own properties, so that security settings —
   *                         SASL, SSL — are those the producer or consumer already uses
   */
  public KafkaRpcRestService(List<String> bootstrapServers, Map<String, ?> configs) {
    this(new KafkaRpcSchemaRegistryClient(
        new AvroSerdeConfig(rpcConfigs(bootstrapServers, configs)), Time.SYSTEM));
  }

  /**
   * For tests, where the transport is supplied directly.
   */
  public KafkaRpcRestService(SchemaRegistryRpcClient rpcClient) {
    super(UNUSED_BASE_URL);
    this.rpcClient = rpcClient;
  }

  private static Map<String, Object> rpcConfigs(
      List<String> bootstrapServers, Map<String, ?> configs) {
    Map<String, Object> merged = new HashMap<>();
    if (configs != null) {
      merged.putAll(configs);
    }
    // The scheme is meaningless to the transport, and bootstrap.servers is authoritative
    // here because it may have come from the scheme rather than from the client's config.
    merged.remove("schema.registry.url");
    merged.put("bootstrap.servers", String.join(",", bootstrapServers));
    return merged;
  }

  @Override
  public <T> T httpRequest(String path,
                           String method,
                           byte[] requestBodyData,
                           Map<String, String> requestProperties,
                           TypeReference<T> responseFormat)
      throws IOException, RestClientException {
    String route = stripQuery(path);
    try {
      Matcher byResourceName = ASSOCIATIONS_BY_RESOURCE_NAME.matcher(route);
      if ("GET".equalsIgnoreCase(method) && byResourceName.matches()) {
        return deserializeArray(
            associationsForTopic(decode(byResourceName.group(2)), queryOf(path)), responseFormat);
      }
      return deserialize(dispatch(route, method, requestBodyData), responseFormat);
    } catch (SchemaNotFoundException e) {
      throw new RestClientException(e.getMessage(), 404, notFoundCodeFor(route));
    } catch (IncompatibleSchemaException e) {
      throw new RestClientException(e.getMessage(), 409, INCOMPATIBLE_SCHEMA_ERROR_CODE);
    } catch (InvalidSchemaException e) {
      throw new RestClientException(e.getMessage(), 422, INVALID_SCHEMA_ERROR_CODE);
    }
  }

  private ObjectNode dispatch(String route, String method, byte[] body)
      throws IOException, RestClientException {
    Matcher matcher;

    if ("POST".equalsIgnoreCase(method)) {
      matcher = SUBJECT_VERSIONS.matcher(route);
      if (matcher.matches()) {
        return register(decode(matcher.group(1)), body);
      }
      matcher = SUBJECT_LOOKUP.matcher(route);
      if (matcher.matches()) {
        return lookUpBySchema(decode(matcher.group(1)), body);
      }
    } else if ("GET".equalsIgnoreCase(method)) {
      matcher = SUBJECT_VERSION_LATEST.matcher(route);
      if (matcher.matches()) {
        return schemaNode(rpcClient.lookupLatest(decode(matcher.group(1))), true);
      }
      matcher = SCHEMA_BY_ID.matcher(route);
      if (matcher.matches()) {
        return schemaStringNode(rpcClient.lookupById(Integer.parseInt(matcher.group(1))));
      }
      matcher = SCHEMA_BY_GUID.matcher(route);
      if (matcher.matches()) {
        return schemaStringNode(rpcClient.lookupByGuid(parseGuid(decode(matcher.group(1)))));
      }
      if (SUBJECT_VERSION_NUMBER.matcher(route).matches()) {
        // The RPC supports lookup by subject and version, but the transport in use does not yet
        // expose it. Fail explicitly rather than appearing to succeed with the latest version.
        throw unsupported(route, method,
            "lookup by explicit version is not yet implemented over Kafka RPCs; "
                + "use the latest version instead");
      }
    }

    throw unsupported(route, method, null);
  }

  private ObjectNode register(String subject, byte[] body) throws IOException {
    ObjectNode request = readTree(body);
    String schemaType = request.hasNonNull("schemaType") ? request.get("schemaType").asText()
        : "AVRO";
    String schemaText = request.hasNonNull("schema") ? request.get("schema").asText() : null;
    SchemaRegistryRpcClient.RegisteredSchema registered =
        rpcClient.register(subject, schemaType, schemaText);

    ObjectNode node = MAPPER.createObjectNode();
    node.put("id", registered.id());
    node.put("guid", guidText(registered.guid()));
    node.put("version", registered.version());
    node.put("schemaType", registered.schemaType());
    node.put("schema", registered.schemaText());
    return node;
  }

  private ObjectNode lookUpBySchema(String subject, byte[] body) throws IOException {
    ObjectNode request = readTree(body);
    String schemaType = request.hasNonNull("schemaType") ? request.get("schemaType").asText()
        : "AVRO";
    String schemaText = request.hasNonNull("schema") ? request.get("schema").asText() : null;
    return schemaNode(rpcClient.lookupBySchema(subject, schemaType, schemaText), true);
  }

  /**
   * The shape returned for {@code Schema}: subject, version and identity together.
   */
  private ObjectNode schemaNode(SchemaRegistryRpcClient.RegisteredSchema schema,
                                boolean includeSubject) {
    ObjectNode node = MAPPER.createObjectNode();
    if (includeSubject) {
      node.put("subject", schema.subject());
    }
    node.put("version", schema.version());
    node.put("id", schema.id());
    node.put("guid", guidText(schema.guid()));
    node.put("schemaType", schema.schemaType());
    node.put("schema", schema.schemaText());
    return node;
  }

  /**
   * The shape returned for {@code SchemaString}, which names the schema text differently.
   */
  private ObjectNode schemaStringNode(SchemaRegistryRpcClient.RegisteredSchema schema) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("schema", schema.schemaText());
    node.put("schemaType", schema.schemaType());
    node.put("guid", guidText(schema.guid()));
    if (schema.subject() != null) {
      node.put("subject", schema.subject());
    }
    node.put("version", schema.version());
    return node;
  }

  /**
   * Answers the association lookup the default subject-name strategy performs before deriving a
   * subject name. The registry addresses the topic as a resource named within a cluster; the RPC
   * addresses it by topic name, which is the last path segment.
   *
   * <p>An empty array means the topic has no association, which the strategy treats as a signal to
   * fall back to its conventional {@code topic-key} / {@code topic-value} naming.
   */
  private ArrayNode associationsForTopic(String topic, String query) {
    ArrayNode associations = MAPPER.createArrayNode();
    for (boolean isKey : requestedBindings(query)) {
      rpcClient.subjectForTopic(topic, isKey).ifPresent(subject -> {
        ObjectNode association = MAPPER.createObjectNode();
        association.put("subject", subject);
        association.put("resourceName", topic);
        association.put("resourceType", "topic");
        association.put("associationType", isKey ? "key" : "value");
        associations.add(association);
      });
    }
    return associations;
  }

  /**
   * The bindings the caller asked about, from repeated {@code associationType} parameters. Absent
   * means both, matching the registry's behaviour of not filtering when none is given.
   */
  private static List<Boolean> requestedBindings(String query) {
    List<Boolean> bindings = new ArrayList<>();
    if (query != null) {
      for (String param : query.split("&")) {
        int eq = param.indexOf('=');
        if (eq > 0 && "associationType".equals(param.substring(0, eq))) {
          String value = decode(param.substring(eq + 1));
          if ("key".equalsIgnoreCase(value)) {
            bindings.add(Boolean.TRUE);
          } else if ("value".equalsIgnoreCase(value)) {
            bindings.add(Boolean.FALSE);
          }
        }
      }
    }
    return bindings.isEmpty() ? List.of(Boolean.TRUE, Boolean.FALSE) : bindings;
  }

  /**
   * Kafka renders a {@link Uuid} in base64, but the serde parses a schema GUID with
   * {@link java.util.UUID#fromString}. The two agree on the underlying 128 bits, so convert the
   * text rather than the value.
   */
  private static String guidText(Uuid guid) {
    return new java.util.UUID(guid.getMostSignificantBits(), guid.getLeastSignificantBits())
        .toString();
  }

  /** Accepts either rendering, since a GUID may arrive from the serde or from a Kafka response. */
  private static Uuid parseGuid(String text) {
    try {
      java.util.UUID parsed = java.util.UUID.fromString(text);
      return new Uuid(parsed.getMostSignificantBits(), parsed.getLeastSignificantBits());
    } catch (IllegalArgumentException e) {
      return Uuid.fromString(text);
    }
  }

  private <T> T deserializeArray(ArrayNode node, TypeReference<T> responseFormat)
      throws IOException {
    if (responseFormat == null) {
      return null;
    }
    return MAPPER.readValue(MAPPER.writeValueAsBytes(node), responseFormat);
  }

  private <T> T deserialize(ObjectNode node, TypeReference<T> responseFormat) throws IOException {
    if (responseFormat == null) {
      return null;
    }
    return MAPPER.readValue(MAPPER.writeValueAsBytes(node), responseFormat);
  }

  private static ObjectNode readTree(byte[] body) throws IOException {
    if (body == null || body.length == 0) {
      return MAPPER.createObjectNode();
    }
    return (ObjectNode) MAPPER.readTree(body);
  }

  private static int notFoundCodeFor(String route) {
    return route.startsWith("/schemas/") ? SCHEMA_NOT_FOUND_ERROR_CODE
        : SUBJECT_NOT_FOUND_ERROR_CODE;
  }

  private static RestClientException unsupported(String route, String method, String detail) {
    return new RestClientException(
        method + " " + route + " is not supported over Kafka RPCs"
            + (detail == null ? "" : ": " + detail),
        501, 50001);
  }

  private static String stripQuery(String path) {
    int query = path.indexOf('?');
    return query < 0 ? path : path.substring(0, query);
  }

  private static String queryOf(String path) {
    int query = path.indexOf('?');
    return query < 0 ? null : path.substring(query + 1);
  }

  private static String decode(String segment) {
    return URLDecoder.decode(segment, StandardCharsets.UTF_8);
  }

  @Override
  public void close() throws IOException {
    try {
      rpcClient.close();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    } finally {
      super.close();
    }
  }
}
