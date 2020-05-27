package io.confluent.kafka.schemaregistry.metrics;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class CustomSchemaProviderMetricTest extends ClusterTestHarness {

  public CustomSchemaProviderMetricTest() { super(1, true); }

  @Override
  protected Properties getSchemaRegistryProperties() {
    Properties props = new Properties();
    props.setProperty(SchemaRegistryConfig.SCHEMA_PROVIDERS_CONFIG,
                      CustomSchemaProvider.class.getName());
    return props;
  }

  @Test
  public void testCustomSchemaProviderMetricCount() {
    MetricsContainer container = restApp.restApp.schemaRegistry().getMetricsContainer();
    assertEquals(1, container.getCustomSchemaProviderCount().get());
  }

  public static class CustomSchemaProvider implements SchemaProvider {

    @Override
    public String schemaType() {
      return "CUSTOM_PROVIDER";
    }

    @Override
    public Optional<ParsedSchema> parseSchema(String schemaString,
                                              List<SchemaReference> references) {
      return Optional.empty();
    }
  }
}
