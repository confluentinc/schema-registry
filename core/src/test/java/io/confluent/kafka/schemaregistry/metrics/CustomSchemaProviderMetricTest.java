/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.metrics;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

import static io.confluent.kafka.schemaregistry.metrics.MetricsContainer.METRIC_NAME_CUSTOM_SCHEMA_PROVIDER;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
  public void testCustomSchemaProviderMetricCount() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName customSchemaProviderCount =
            new ObjectName("kafka.schema.registry:type=" + METRIC_NAME_CUSTOM_SCHEMA_PROVIDER);
    assertEquals(1.0, mBeanServer.getAttribute(customSchemaProviderCount, METRIC_NAME_CUSTOM_SCHEMA_PROVIDER));
  }

  public static class CustomSchemaProvider implements SchemaProvider {

    @Override
    public String schemaType() {
      return "CUSTOM_PROVIDER";
    }

    @Override
    public Optional<ParsedSchema> parseSchema(String schemaString,
                                              List<SchemaReference> references,
                                              boolean isNew,
                                              boolean normalize) {
      return Optional.empty();
    }

    @Override
    public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew, boolean normalize) {
      return null;
    }
  }
}
