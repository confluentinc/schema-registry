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

package io.confluent.kafka.schemaregistry.client;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_TENANT;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Non-blocking counterpart to {@link SchemaRegistryClient}.
 *
 * <p>Every method returns immediately. The returned future either completes with the result, or
 * completes exceptionally with a {@link RestClientException} (the registry returned an error
 * response) or an {@link IOException} (network or parsing failure). Callers that use
 * {@code join()} will see these wrapped in a {@link java.util.concurrent.CompletionException}.
 *
 * <p>Futures may be completed on the HTTP client's I/O threads. Dependent stages must not block;
 * use the {@code *Async} variants of {@link CompletableFuture} with your own executor for
 * expensive work.
 *
 * <p>Implementations must be thread-safe. {@link #close()} releases the underlying I/O threads;
 * futures still pending at that point complete exceptionally.
 */
public interface AsyncSchemaRegistryClient extends Closeable {

  default String tenant() {
    return DEFAULT_TENANT;
  }

  // Parsing. Returns a future because resolving schema references may require registry lookups.

  CompletableFuture<Optional<ParsedSchema>> parseSchema(Schema schema);

  // Read path, as used by deserializers. Results are cached.

  CompletableFuture<ParsedSchema> getSchemaBySubjectAndId(String subject, int id);

  default CompletableFuture<ParsedSchema> getSchemaById(int id) {
    return getSchemaBySubjectAndId(null, id);
  }

  CompletableFuture<Schema> getSchemaEntityBySubjectAndId(String subject, int id);

  CompletableFuture<ParsedSchema> getSchemaByGuid(String guid, String format);

  CompletableFuture<SchemaMetadata> getSchemaMetadata(
      String subject, int version, boolean lookupDeletedSchema);

  default CompletableFuture<SchemaMetadata> getSchemaMetadata(String subject, int version) {
    return getSchemaMetadata(subject, version, false);
  }

  CompletableFuture<SchemaMetadata> getLatestSchemaMetadata(String subject);

  CompletableFuture<SchemaMetadata> getLatestWithMetadata(
      String subject, Map<String, String> metadata, boolean lookupDeletedSchema);

  // Write path, as used by serializers. Results are cached.

  CompletableFuture<RegisterSchemaResponse> registerWithResponse(
      String subject, ParsedSchema schema, boolean normalize, boolean propagateSchemaTags);

  default CompletableFuture<Integer> register(
      String subject, ParsedSchema schema, boolean normalize) {
    return registerWithResponse(subject, schema, normalize, false)
        .thenApply(RegisterSchemaResponse::getId);
  }

  default CompletableFuture<Integer> register(String subject, ParsedSchema schema) {
    return register(subject, schema, false);
  }

  /**
   * Looks up the ID of an already-registered schema without registering it.
   */
  CompletableFuture<RegisterSchemaResponse> getIdWithResponse(
      String subject, ParsedSchema schema, boolean normalize);

  default CompletableFuture<Integer> getId(
      String subject, ParsedSchema schema, boolean normalize) {
    return getIdWithResponse(subject, schema, normalize)
        .thenApply(RegisterSchemaResponse::getId);
  }

  default CompletableFuture<Integer> getId(String subject, ParsedSchema schema) {
    return getId(subject, schema, false);
  }

  CompletableFuture<Integer> getVersion(String subject, ParsedSchema schema, boolean normalize);

  default CompletableFuture<Integer> getVersion(String subject, ParsedSchema schema) {
    return getVersion(subject, schema, false);
  }

  // Compatibility and config. Not cached.

  /**
   * Returns an empty list if the schema is compatible, otherwise the reasons it is not.
   */
  CompletableFuture<List<String>> testCompatibilityVerbose(
      String subject, ParsedSchema schema, boolean normalize);

  default CompletableFuture<Boolean> testCompatibility(String subject, ParsedSchema schema) {
    return testCompatibilityVerbose(subject, schema, false).thenApply(List::isEmpty);
  }

  CompletableFuture<Config> getConfig(String subject, boolean defaultToGlobal);

  // Lifecycle

  /**
   * Clears all caches. Does not affect in-flight requests.
   */
  void reset();

  @Override
  void close() throws IOException;
}
