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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.provenance.ProvenanceRejectedException;
import io.confluent.kafka.serializers.provenance.ProvenanceRetriableException;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import io.confluent.kafka.serializers.provenance.ProvenanceUnknownWriterException;
import java.io.IOException;
import java.util.Collections;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.junit.Test;

/** The registry's statuses and error codes, as the strategy contract types them. */
public class ClientProvenanceStrategyTest {

  @Test
  public void aResponseIsReturnedAsIs() {
    SchemaProvenance response = new SchemaProvenance("s", Collections.emptyList());
    assertSame(response, ask(new FailingClient(null, null, response)));
  }

  @Test
  public void anUnreachableOrOverloadedRegistryIsRetriable() {
    assertFails(ProvenanceRetriableException.class, new IOException("down"));
    for (int status : new int[] {500, 503, 408, 429}) {
      assertFails(ProvenanceRetriableException.class,
          new RestClientException("busy", status, status * 100));
    }
  }

  @Test
  public void anAuthFailureIsKafkasOwn() {
    assertFails(AuthenticationException.class, new RestClientException("who", 401, 40101));
    assertFails(AuthorizationException.class, new RestClientException("no", 403, 40301));
  }

  @Test
  public void aRequestTheRegistryRefusesIsRejected() {
    for (int[] code : new int[][] {{422, 42202}, {422, 42215}, {404, 40402}, {422, 42216}}) {
      assertFails(ProvenanceRejectedException.class,
          new RestClientException("rejected", code[0], code[1]));
    }
  }

  @Test
  public void aWriterIdUnderNoVersionIsUnknown() {
    assertFails(ProvenanceUnknownWriterException.class,
        new RestClientException("not a version", 404, 40411));
  }

  @Test
  public void anyOtherErrorOrAClientWithoutProvenanceIsUnavailable() {
    assertFails(ProvenanceUnavailableException.class,
        new RestClientException("recursive", 422, 42220));
    assertFails(ProvenanceUnavailableException.class,
        new UnsupportedOperationException("not implemented"));
  }

  @Test
  public void aPinnedVersionIsAskedForByVersionAndFailsAlike() {
    SchemaProvenance response = new SchemaProvenance("s", Collections.emptyList());
    assertSame(response, askToVersion(new FailingClient(null, null, response)));
    assertSame(IOException.class, assertThrows(ProvenanceRetriableException.class,
        () -> askToVersion(new FailingClient(new IOException("down"), null, null)))
        .getCause().getClass());
    assertThrows(ProvenanceRejectedException.class, () -> askToVersion(new FailingClient(
        new RestClientException("Version 9 not found.", 404, 40402), null, null)));
    // A client that cannot pin a version fails the records, never reads another version.
    assertThrows(ProvenanceRejectedException.class, () -> askToVersion(
        new FailingClient(null, new UnsupportedOperationException("not implemented"), null)));
  }

  @Test
  public void transientStatusesAreServerErrorsTimeoutsAndThrottling() {
    assertTrue(ClientProvenanceStrategy.isTransient(500));
    assertTrue(ClientProvenanceStrategy.isTransient(408));
    assertTrue(ClientProvenanceStrategy.isTransient(429));
    assertFalse(ClientProvenanceStrategy.isTransient(404));
  }

  private static void assertFails(Class<? extends RuntimeException> expected, Exception failure) {
    RuntimeException e = assertThrows(expected, () -> ask(new FailingClient(
        failure instanceof IOException || failure instanceof RestClientException ? failure : null,
        failure instanceof RuntimeException ? (RuntimeException) failure : null, null)));
    assertSame(failure, e.getCause());
  }

  private static SchemaProvenance ask(FailingClient client) {
    return new ClientProvenanceStrategy().provenance(client, "s", 1, 2, false, false, "v1");
  }

  private static SchemaProvenance askToVersion(FailingClient client) {
    return new ClientProvenanceStrategy().provenanceToVersion(
        client, "s", 1, 2, false, false, "v1");
  }

  // Answers the provenance request with a response, or fails as set.
  private static final class FailingClient extends MockSchemaRegistryClient {

    private final Exception checked;
    private final RuntimeException unchecked;
    private final SchemaProvenance response;

    FailingClient(Exception checked, RuntimeException unchecked, SchemaProvenance response) {
      this.checked = checked;
      this.unchecked = unchecked;
      this.response = response;
    }

    @Override
    public SchemaProvenance getProvenanceById(String subject, int fromId, int toId,
        boolean includeInterior, boolean includeMultipleMessages, String algorithm)
        throws IOException, RestClientException {
      return answer();
    }

    @Override
    public SchemaProvenance getProvenanceToVersion(String subject, int fromId, int toVersion,
        boolean includeInterior, boolean includeMultipleMessages, String algorithm)
        throws IOException, RestClientException {
      return answer();
    }

    private SchemaProvenance answer() throws IOException, RestClientException {
      if (checked instanceof IOException) {
        throw (IOException) checked;
      }
      if (checked instanceof RestClientException) {
        throw (RestClientException) checked;
      }
      if (unchecked != null) {
        throw unchecked;
      }
      return response;
    }
  }
}
