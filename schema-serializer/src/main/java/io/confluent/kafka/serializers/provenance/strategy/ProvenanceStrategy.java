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

package io.confluent.kafka.serializers.provenance.strategy;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth.exceptions.SchemaRegistryOauthTokenRetrieverException;
import io.confluent.kafka.serializers.provenance.ProvenanceRejectedException;
import io.confluent.kafka.serializers.provenance.ProvenanceRetriableException;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import io.confluent.kafka.serializers.provenance.ProvenanceUnknownWriterException;
import java.io.Closeable;
import java.io.UncheckedIOException;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;

/**
 * Where a deserializer reading by provenance gets the provenance pairing a writer schema with a
 * reader schema. The default, {@link ClientProvenanceStrategy}, asks Schema Registry.
 *
 * <p>The response must be the one the registry's endpoint would give: each version's locations
 * with the {@code path}, {@code names} and {@code kind} Schema Registry's converters produce, and
 * the {@code kind} of each version's root, as a reader finds its fields and tells a field from a
 * union branch by them, and a pid per location that is equal exactly where the location continues.
 *
 * <p>What a failure is thrown as decides what the record gets:
 * <ul>
 *   <li>a {@link ProvenanceRetriableException}, an {@link UncheckedIOException}, Kafka's
 *       {@link RetriableException} or {@link InterruptException}, a
 *       {@link SchemaRegistryOauthTokenRetrieverException}, or a raw {@code KafkaException} as
 *       the client's token retrievers and SSL factory throw it: the source is unreachable,
 *       overloaded or timed out, or a credential cannot be had now; the record fails, and the
 *       next asks again;
 *   <li>an {@link AuthenticationException} or {@link AuthorizationException}: the record fails
 *       with it, and the next asks again;
 *   <li>a {@link ProvenanceRejectedException}: the request itself is wrong, as for an unknown
 *       algorithm; every record of the writer fails until the outcome expires;
 *   <li>a {@link ProvenanceUnknownWriterException}: a schema id asked about, most often the
 *       writer's, is no version of the subject; the writer is matched to one by structure,
 *       through the deserializer's client, and asked about again, or else read without it;
 *   <li>a {@link ProvenanceUnavailableException}: no provenance for the pair; the writer is read
 *       without it, with one warning, until the outcome expires;
 *   <li>anything else, or a null response: the strategy broke this contract; every record of the
 *       writer fails until the outcome expires.
 * </ul>
 *
 * <p>A strategy is called from any thread reading a record, possibly several at once, so it must
 * be thread-safe. It is closed when its deserializer is closed or reconfigured: a record being
 * read while its deserializer is reconfigured may find it closed, and fail.
 */
public interface ProvenanceStrategy extends Configurable, Closeable {

  /**
   * Releases what the strategy holds.
   */
  @Override
  default void close() {
  }

  /**
   * The provenance of {@code subject} between the versions carrying schema ids {@code fromId}
   * and {@code toId}, as {@code GET /subjects/{subject}/provenance} answers it.
   *
   * @param client the deserializer's Schema Registry client
   * @param includeMultipleMessages whether each Protobuf version is rooted at all its top-level
   *     messages
   * @param algorithm the provenance algorithm asked for; null for the default, v1
   */
  SchemaProvenance provenance(SchemaRegistryClient client, String subject, int fromId, int toId,
      boolean includeMultipleMessages, String algorithm);

  /**
   * The provenance of {@code subject} from the version carrying schema id {@code fromId} to
   * version {@code toVersion}, as {@code GET /subjects/{subject}/provenance} answers it, for a
   * reader pinned to a version. A strategy that cannot pin one leaves this as is: its records
   * fail, rather than be read against whichever version carries the reader's schema id.
   *
   * @param client the deserializer's Schema Registry client
   * @param includeMultipleMessages whether each Protobuf version is rooted at all its top-level
   *     messages
   * @param algorithm the provenance algorithm asked for; null for the default, v1
   */
  default SchemaProvenance provenanceToVersion(SchemaRegistryClient client, String subject,
      int fromId, int toVersion, boolean includeMultipleMessages, String algorithm) {
    throw new ProvenanceRejectedException(
        "The provenance strategy cannot pin a reader to a version");
  }
}
