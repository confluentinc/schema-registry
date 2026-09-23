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

package io.confluent.kafka.serializers.provenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.util.Optional;
import org.junit.Test;

public class ProvenanceProjectorTest {

  private static final String SUBJECT = "s-value";

  @Test
  public void anUnavailableOutcomeIsCachedWithoutATtl() throws Exception {
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    ask(projector, client);
    ask(projector, client);
    assertEquals(1, client.asked);
  }

  @Test
  public void anExpiredOutcomeIsWorkedOutAfresh() throws Exception {
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, 0);
    ask(projector, client);
    ask(projector, client);
    assertEquals(2, client.asked);
  }

  @Test
  public void theCacheHoldsNoMoreThanItsSize() throws Exception {
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 1, -1);
    ask(projector, client, client.writer);
    ask(projector, client, client.otherWriter);
    ask(projector, client, client.writer);
    assertEquals(3, client.asked);
  }

  private static void ask(ProvenanceProjector<String> projector, CountingClient client)
      throws Exception {
    ask(projector, client, client.writer);
  }

  private static void ask(ProvenanceProjector<String> projector, CountingClient client,
      int writerId) throws Exception {
    SchemaId id = new SchemaId(AvroSchema.TYPE, writerId, (String) null);
    Optional<String> built = projector.project(SUBJECT, id, client.writerSchema, client.reader,
        false, mapping -> "built");
    assertFalse(built.isPresent());
  }

  // Answers every provenance request as unavailable, counting how often it is asked.
  private static final class CountingClient extends MockSchemaRegistryClient {
    final ParsedSchema writerSchema = new AvroSchema("\"int\"");
    final ParsedSchema reader = new AvroSchema("\"long\"");
    final int writer;
    final int otherWriter;
    int asked;

    CountingClient() throws Exception {
      writer = register(SUBJECT, writerSchema);
      otherWriter = register(SUBJECT, new AvroSchema("\"string\""));
      register(SUBJECT, reader);
    }

    @Override
    public SchemaProvenance getProvenanceById(String subject, int fromId, int toId,
        boolean includeInterior, boolean includeMultipleMessages,
        String algorithm) {
      asked++;
      throw new UnsupportedOperationException("no provenance here");
    }
  }
}
