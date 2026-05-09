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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.avro.type.VariantConversion;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantBuilder;
import io.confluent.kafka.schemaregistry.type.VariantObjectBuilder;
import java.util.Collections;
import java.util.List;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

/**
 * Avro counterpart to {@link CelValidatorVariantTest}. Uses Avro {@code variant}
 * logical type as the payload field; verifies the same accessor surface
 * ({@code to_variant} + {@code variant_get_field} + {@code variant_get_string}).
 */
public class CelValidatorAvroVariantTest {

  private static final VariantConversion CONVERSION = new VariantConversion();

  /** Outer Event schema with a variant-typed payload field carrying the rule. */
  private static final Schema SCHEMA = buildSchema();

  private static Schema buildSchema() {
    Schema variantSchema = CONVERSION.getRecommendedSchema();
    Schema.Field payload = new Schema.Field(
        "payload", variantSchema, null, (Object) null);
    payload.addProp("confluent:rules", List.of(java.util.Map.of(
        "name", "nameIsAlice",
        "expr",
        "variant_get_string("
            + "variant_get_field(to_variant(this), \"name\")) == \"alice\"")));

    return Schema.createRecord("Event", null, "test", false,
        Collections.singletonList(payload));
  }

  /** Build a Variant payload {"name": value}. */
  private static Variant variantOf(String name) {
    VariantBuilder vb = new VariantBuilder();
    VariantObjectBuilder ob = vb.startObject();
    ob.appendKey("name");
    ob.appendString(name);
    vb.endObject();
    return vb.build();
  }

  /** Build the outer Event with a variant payload encoded as IndexedRecord. */
  private static GenericRecord eventOf(Variant v) {
    Schema variantSchema = SCHEMA.getField("payload").schema();
    IndexedRecord rec = CONVERSION.toRecord(v, variantSchema, variantSchema.getLogicalType());
    GenericRecord r = new GenericData.Record(SCHEMA);
    r.put("payload", rec);
    return r;
  }

  @Test
  void aliceName_satisfiesRule() {
    AvroSchema avro = new AvroSchema(SCHEMA);
    GenericRecord rec = eventOf(variantOf("alice"));
    List<ValidationRuleError> errors =
        avro.validateMessage(new CelValidator(), rec);
    if (!errors.isEmpty() && errors.get(0).getCause() != null) {
      throw new AssertionError(errors.get(0).getCause().getMessage(),
          errors.get(0).getCause());
    }
    assertTrue(errors.isEmpty(),
        "name=alice should satisfy the rule, got: " + errors);
  }

  @Test
  void otherName_violatesRule() {
    AvroSchema avro = new AvroSchema(SCHEMA);
    GenericRecord rec = eventOf(variantOf("bob"));
    List<ValidationRuleError> errors =
        avro.validateMessage(new CelValidator(), rec);
    assertEquals(1, errors.size(),
        "name=bob should fail the rule, got: " + errors);
    assertEquals("nameIsAlice", errors.get(0).getRule().getName());
    assertEquals("payload", errors.get(0).getFieldPath());
  }

  // Suppresses "unused" — JsonProperties is only used implicitly via Schema.Field.
  @SuppressWarnings("unused")
  private static final JsonProperties.Null UNUSED = JsonProperties.NULL_VALUE;
}
