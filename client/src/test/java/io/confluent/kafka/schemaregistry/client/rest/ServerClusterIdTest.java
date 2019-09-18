package io.confluent.kafka.schemaregistry.client.rest;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.entities.ServerClusterId;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ServerClusterIdTest {

  @Test
  public void buildServerClusterId() {
    ServerClusterId serverClusterId = ServerClusterId.of("kafka1", "sr1");

    final String id = serverClusterId.getId();
    final Map<String, String> scope = serverClusterId.getScope();

    assertEquals("", id);
    assertEquals(
        ImmutableMap.of(
           "kafka-cluster", "kafka1",
           "schema-registry-cluster", "sr1"
        ), scope
    );
  }
}
