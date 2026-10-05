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

package io.confluent.kafka.schemaregistry.type.logical.json.v1;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reader-side coverage gaps not present in the existing
 * {@code JsonToLogicalTypeConverterTest}, ported from Flink's
 * {@code JsonToFlinkSchemaConverterTest#testTypeMapping} matrix:
 * MAP/MULTISET shapes (non-string-keyed via array-of-{key,value}, and
 * string-keyed via additionalProperties).
 */
class JsonReaderCoverageTest {

  // ---------- MAP — string key ----------

  @Test
  void testMapStringKey() {
    Map<String, Object> props = new HashMap<>();
    props.put("connect.type", "map");
    org.everit.json.schema.Schema jsonSchema = ObjectSchema.builder()
        .schemaOfAdditionalProperties(NumberSchema.builder()
            .unprocessedProperties(Map.of("connect.type", "int64"))
            .build())
        .unprocessedProperties(props)
        .build();

    Schema lt = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertThat(lt.getType()).isEqualTo(Schema.Type.MAP);
    assertThat(lt.getKeyType().getType()).isEqualTo(Schema.Type.VARCHAR);
    assertThat(lt.getValueType().getType()).isEqualTo(Schema.Type.BIGINT);
  }

  // ---------- MAP — non-string key ----------

  @Test
  void testMapNonStringKey() {
    Map<String, Object> props = new HashMap<>();
    props.put("connect.type", "map");
    org.everit.json.schema.Schema entrySchema = ObjectSchema.builder()
        .addPropertySchema("key", NumberSchema.builder()
            .unprocessedProperties(Map.of("connect.type", "int32"))
            .build())
        .addPropertySchema("value", NumberSchema.builder()
            .unprocessedProperties(Map.of("connect.type", "int64"))
            .build())
        .additionalProperties(false)
        .build();
    org.everit.json.schema.Schema jsonSchema = ArraySchema.builder()
        .allItemSchema(entrySchema)
        .unprocessedProperties(props)
        .build();

    Schema lt = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertThat(lt.getType()).isEqualTo(Schema.Type.MAP);
    assertThat(lt.getKeyType().getType()).isEqualTo(Schema.Type.INT);
    assertThat(lt.getValueType().getType()).isEqualTo(Schema.Type.BIGINT);
  }

  // ---------- MULTISET — string key (additionalProperties path) ----------

  @Test
  void testMultisetStringKey() {
    Map<String, Object> props = new HashMap<>();
    props.put("connect.type", "map");
    props.put("flink.type", "multiset");
    props.put("flink.version", "1");
    org.everit.json.schema.Schema jsonSchema = ObjectSchema.builder()
        .schemaOfAdditionalProperties(NumberSchema.builder()
            .unprocessedProperties(Map.of("connect.type", "int32"))
            .build())
        .unprocessedProperties(props)
        .build();

    Schema lt = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertThat(lt.getType()).isEqualTo(Schema.Type.MULTISET);
    assertThat(lt.getElementType().getType()).isEqualTo(Schema.Type.VARCHAR);
  }

  // ---------- MULTISET — non-string key (array-of-{k,v} path) ----------

  @Test
  void testMultisetNonStringKey() {
    Map<String, Object> props = new HashMap<>();
    props.put("connect.type", "map");
    props.put("flink.type", "multiset");
    props.put("flink.version", "1");
    org.everit.json.schema.Schema entrySchema = ObjectSchema.builder()
        .addPropertySchema("key", NumberSchema.builder()
            .unprocessedProperties(Map.of("connect.type", "int64"))
            .build())
        .addPropertySchema("value", NumberSchema.builder()
            .unprocessedProperties(Map.of("connect.type", "int32"))
            .build())
        .additionalProperties(false)
        .build();
    org.everit.json.schema.Schema jsonSchema = ArraySchema.builder()
        .allItemSchema(entrySchema)
        .unprocessedProperties(props)
        .build();

    Schema lt = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertThat(lt.getType()).isEqualTo(Schema.Type.MULTISET);
    assertThat(lt.getElementType().getType()).isEqualTo(Schema.Type.BIGINT);
  }

  // ---------- ARRAY of various element types ----------

  @Test
  void testArrayOfNumber() {
    org.everit.json.schema.Schema jsonSchema = ArraySchema.builder()
        .allItemSchema(NumberSchema.builder()
            .unprocessedProperties(Map.of("connect.type", "int64"))
            .build())
        .build();
    Schema lt = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertThat(lt.getType()).isEqualTo(Schema.Type.ARRAY);
    assertThat(lt.getElementType().getType()).isEqualTo(Schema.Type.BIGINT);
  }

  @Test
  void testArrayOfObject() {
    org.everit.json.schema.Schema item = ObjectSchema.builder()
        .addPropertySchema("name", StringSchema.builder().build())
        .additionalProperties(false)
        .build();
    org.everit.json.schema.Schema jsonSchema = ArraySchema.builder()
        .allItemSchema(item)
        .build();
    Schema lt = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertThat(lt.getType()).isEqualTo(Schema.Type.ARRAY);
    assertThat(lt.getElementType().getType()).isEqualTo(Schema.Type.STRUCT);
    assertThat(lt.getElementType().getFields()).hasSize(1);
    assertThat(lt.getElementType().getField("name").getSchema().getType())
        .isEqualTo(Schema.Type.VARCHAR);
  }
}
