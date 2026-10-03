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
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.provenance.ProvenanceRejectedException;
import io.confluent.kafka.serializers.provenance.ProvenanceRetriableException;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import io.confluent.kafka.serializers.provenance.ProvenanceUnknownWriterException;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;

/**
 * Asks Schema Registry, through the deserializer's client, for the provenance, and turns the
 * registry's statuses and error codes into what {@link ProvenanceStrategy} says they mean.
 */
public class ClientProvenanceStrategy implements ProvenanceStrategy {

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public SchemaProvenance provenance(SchemaRegistryClient client, String subject, int fromId,
      int toId, boolean includeInterior, boolean includeMultipleMessages, String algorithm) {
    try {
      return client.getProvenanceById(
          subject, fromId, toId, includeInterior, includeMultipleMessages, algorithm);
    } catch (IOException e) {
      throw new ProvenanceRetriableException("Could not reach Schema Registry", e);
    } catch (RestClientException e) {
      throw translate(e, "schema ids " + fromId + " and " + toId + " of subject " + subject);
    } catch (UnsupportedOperationException e) {
      throw new ProvenanceUnavailableException(
          "The Schema Registry client does not support provenance", e);
    }
  }

  @Override
  public SchemaProvenance provenanceToVersion(SchemaRegistryClient client, String subject,
      int fromId, int toVersion, boolean includeInterior, boolean includeMultipleMessages,
      String algorithm) {
    try {
      return client.getProvenanceToVersion(
          subject, fromId, toVersion, includeInterior, includeMultipleMessages, algorithm);
    } catch (IOException e) {
      throw new ProvenanceRetriableException("Could not reach Schema Registry", e);
    } catch (RestClientException e) {
      throw translate(e, "schema id " + fromId + " and version " + toVersion + " of subject "
          + subject);
    } catch (UnsupportedOperationException e) {
      throw new ProvenanceRejectedException(
          "The Schema Registry client cannot pin a reader to a version", e);
    }
  }

  private static RuntimeException translate(RestClientException e, String pair) {
    if (isTransient(e.getStatus())) {
      return new ProvenanceRetriableException(
          "Schema Registry could not serve provenance: " + e.getMessage(), e);
    }
    if (e.getStatus() == 401) {
      return new AuthenticationException(
          "Not authenticated to Schema Registry for the provenance of " + pair, e);
    }
    if (e.getStatus() == 403) {
      return new AuthorizationException(
          "Not authorized by Schema Registry for the provenance of " + pair, e);
    }
    switch (e.getErrorCode()) {
      // A bad version, request or range, or an algorithm the registry does not know.
      case 42202:
      case 42215:
      case 40402:
      case 42216:
        return new ProvenanceRejectedException(e.getMessage(), e);
      case 40411:
        return new ProvenanceUnknownWriterException(e.getMessage(), e);
      default:
        return new ProvenanceUnavailableException(e.getMessage(), e);
    }
  }

  /**
   * Whether a response with {@code status} may succeed if asked again.
   */
  public static boolean isTransient(int status) {
    return status >= 500 || status == 408 || status == 429;
  }
}
