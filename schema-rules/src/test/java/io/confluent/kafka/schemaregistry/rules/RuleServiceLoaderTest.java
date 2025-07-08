/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules;

import static com.google.common.collect.ImmutableList.*;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rules.cel.CelExecutor;
import io.confluent.kafka.schemaregistry.rules.cel.CelFieldExecutor;
import io.confluent.kafka.schemaregistry.rules.jsonata.JsonataExecutor;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RuleServiceLoaderTest {

  private SchemaRegistryClient schemaRegistry;
  private KafkaAvroSerializer avroSerializer;
  private String topic;

  @Before
  public void setup() throws IOException {
    topic = "test";
    schemaRegistry = new MockSchemaRegistryClient(of(
        new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()));
    Map<String, Object> defaultConfig = new HashMap<>();
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, defaultConfig);
  }

  @Test
  public void testSuccess() {
    assertInstance(avroSerializer.getRuleExecutors().get("CEL").get(RuleBase.DEFAULT_NAME),
        CelExecutor.class);
    assertInstance(avroSerializer.getRuleExecutors().get("CEL_FIELD").get(RuleBase.DEFAULT_NAME),
        CelFieldExecutor.class);
    assertInstance(avroSerializer.getRuleExecutors().get("JSONATA").get(RuleBase.DEFAULT_NAME),
        JsonataExecutor.class);
  }

  public void assertInstance(RuleBase instance,
                             Class<? extends RuleBase> klass) {
    Assert.assertNotNull(instance);
    Assert.assertEquals(klass, instance.getClass());
  }
}
