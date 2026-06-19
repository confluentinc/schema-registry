/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryInitializationException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.rest.RestConfigException;
import io.confluent.srj.spire.SpireX509Client;
import io.confluent.srj.spire.exceptions.SpiffeX509SourceProviderException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyChar;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaSchemaRegistrySpireTest {

  private static SchemaRegistryConfig config(Properties props) throws RestConfigException {
    return new SchemaRegistryConfig(props);
  }

  @Test
  public void createSpireX509ClientAppliesAgentUrlAndPatternsWhenProvided() throws Exception {
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.INTER_INSTANCE_SPIRE_AGENT_URL_CONFIG,
        "unix:///tmp/spire-agent/abc/api.sock");
    props.setProperty(SchemaRegistryConfig.INTER_INSTANCE_SPIRE_AUTHORIZED_ID_PATTERNS_CONFIG,
        "spiffe://example.org/sr,spiffe://example.org/sr2");

    SpireX509Client.SpireX509ClientBuilder builder =
        mock(SpireX509Client.SpireX509ClientBuilder.class);
    SpireX509Client built = mock(SpireX509Client.class);
    when(builder.build()).thenReturn(built);

    SpireX509Client result =
        KafkaSchemaRegistry.createSpireX509Client(config(props), builder);

    assertSame(built, result, "Should return the client produced by the builder");
    verify(builder).setSpireAgentURL("unix:///tmp/spire-agent/abc/api.sock");
    verify(builder).setAuthorizedSpiffeIdPatterns(
        "spiffe://example.org/sr,spiffe://example.org/sr2", ',');
    verify(builder).build();
  }

  @Test
  public void createSpireX509ClientSkipsAgentUrlAndPatternsWhenEmpty() throws Exception {
    // Defaults leave both the agent URL and the authorized-id patterns empty, which means
    // "fall back to SPIFFE_ENDPOINT_SOCKET" and "accept any SPIFFE ID" respectively.
    SpireX509Client.SpireX509ClientBuilder builder =
        mock(SpireX509Client.SpireX509ClientBuilder.class);
    SpireX509Client built = mock(SpireX509Client.class);
    when(builder.build()).thenReturn(built);

    SpireX509Client result =
        KafkaSchemaRegistry.createSpireX509Client(config(new Properties()), builder);

    assertSame(built, result);
    verify(builder, never()).setSpireAgentURL(anyString());
    verify(builder, never()).setAuthorizedSpiffeIdPatterns(anyString(), anyChar());
    verify(builder).build();
  }

  @Test
  public void createSpireX509ClientDoesNotWrapUncheckedExceptions() throws Exception {
    // Only SpiffeX509SourceProviderException should be wrapped; unexpected unchecked exceptions
    // must propagate as-is rather than being masked as an initialization failure.
    SpireX509Client.SpireX509ClientBuilder builder =
        mock(SpireX509Client.SpireX509ClientBuilder.class);
    RuntimeException cause = new IllegalStateException("boom");
    when(builder.build()).thenThrow(cause);

    RuntimeException thrown = assertThrows(IllegalStateException.class,
        () -> KafkaSchemaRegistry.createSpireX509Client(config(new Properties()), builder));

    assertSame(cause, thrown, "Unchecked exceptions should propagate unwrapped");
  }
}
