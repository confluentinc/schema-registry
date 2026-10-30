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

package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

// DGS-25191: a generic serde running a domain rule does not initialize a class the schema names
public class CelSchemaNamedClassTest {

  static final Set<String> initialized = ConcurrentHashMap.newKeySet();

  public static class SchemaNamed {
    static {
      initialized.add("SchemaNamed");
    }
  }

  @Test
  public void testDomainRule() throws Exception {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    config.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    config.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, "cel");
    config.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".cel.class",
        CelExecutor.class.getName());
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry, config);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistry, config);

    // Resolved as a nested class of this test
    Schema schema = Schema.createRecord("SchemaNamed", null,
        CelSchemaNamedClassTest.class.getName(), false, Collections.singletonList(
            new Schema.Field("f", Schema.create(Schema.Type.STRING))));
    Rule rule = new Rule("myRule", null, RuleKind.CONDITION, RuleMode.WRITEREAD,
        CelExecutor.TYPE, null, null, "message.f == 'value'", null, null, false);
    AvroSchema avroSchema = new AvroSchema(schema).copy(null,
        new RuleSet(Collections.emptyList(), Collections.singletonList(rule)));
    schemaRegistry.register("test-value", avroSchema);

    GenericRecord value = new GenericData.Record(schema);
    value.put("f", "value");
    byte[] bytes = serializer.serialize("test", value);
    assertEquals(value, deserializer.deserialize("test", bytes));
    assertFalse(initialized.contains("SchemaNamed"));
  }
}
