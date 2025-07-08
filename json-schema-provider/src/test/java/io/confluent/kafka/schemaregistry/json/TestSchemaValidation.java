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

package io.confluent.kafka.schemaregistry.json;


import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.SchemaValidator;
import io.confluent.kafka.schemaregistry.SchemaValidatorBuilder;
import io.confluent.kafka.schemaregistry.json.TestSchemas.ReaderWriter;

import static io.confluent.kafka.schemaregistry.json.TestSchemas.A_DINT_B_DINT_RECORD1;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.A_INT_B_DINT_RECORD1;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.A_INT_B_DINT_REQUIRED_RECORD1;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.A_INT_B_INT_RECORD1;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.A_INT_B_INT_REQUIRED_RECORD1;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.A_INT_OPEN_RECORD1;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.A_INT_RECORD1;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.BOOLEAN_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.EMPTY_RECORD1;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.ENUM1_ABC_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.ENUM1_AB_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.ENUM1_BC_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.INT_ARRAY_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.INT_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.NUMBER_ARRAY_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.NUMBER_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.STRING_ARRAY_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.STRING_INT_UNION_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.STRING_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.STRING_UNION_SCHEMA;
import static io.confluent.kafka.schemaregistry.json.TestSchemas.list;

public class TestSchemaValidation {

  public static final List<ReaderWriter> COMPATIBLE_READER_WRITER_TEST_CASES =
      list(new ReaderWriter(NUMBER_SCHEMA,
          INT_SCHEMA
      ),

      new ReaderWriter(NUMBER_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),

      new ReaderWriter(ENUM1_ABC_SCHEMA, ENUM1_AB_SCHEMA),

      new ReaderWriter(STRING_INT_UNION_SCHEMA, STRING_UNION_SCHEMA),
      new ReaderWriter(STRING_INT_UNION_SCHEMA, STRING_SCHEMA),

      // Special case of singleton unions
      new ReaderWriter(STRING_UNION_SCHEMA, STRING_SCHEMA),
      new ReaderWriter(STRING_SCHEMA, STRING_UNION_SCHEMA),

      new ReaderWriter(A_INT_RECORD1, EMPTY_RECORD1),
      new ReaderWriter(A_INT_B_DINT_RECORD1, A_INT_RECORD1),
      new ReaderWriter(A_DINT_B_DINT_RECORD1, EMPTY_RECORD1),
      new ReaderWriter(A_DINT_B_DINT_RECORD1, A_INT_RECORD1),

      new ReaderWriter(A_INT_OPEN_RECORD1, A_INT_B_INT_RECORD1),
      new ReaderWriter(A_INT_B_INT_RECORD1, A_INT_RECORD1),
      new ReaderWriter(A_INT_B_DINT_REQUIRED_RECORD1, A_INT_RECORD1)
  );

  public static final List<ReaderWriter> INCOMPATIBLE_READER_WRITER_TEST_CASES =
      list(new ReaderWriter(BOOLEAN_SCHEMA,
          INT_SCHEMA
      ),

      new ReaderWriter(INT_SCHEMA, BOOLEAN_SCHEMA),
      new ReaderWriter(INT_SCHEMA, NUMBER_SCHEMA),

      new ReaderWriter(STRING_SCHEMA, BOOLEAN_SCHEMA),
      new ReaderWriter(STRING_SCHEMA, INT_SCHEMA),

      new ReaderWriter(INT_ARRAY_SCHEMA, NUMBER_ARRAY_SCHEMA),
      new ReaderWriter(INT_ARRAY_SCHEMA, STRING_ARRAY_SCHEMA),

      new ReaderWriter(ENUM1_AB_SCHEMA, ENUM1_ABC_SCHEMA),
      new ReaderWriter(ENUM1_BC_SCHEMA, ENUM1_ABC_SCHEMA),

      new ReaderWriter(INT_SCHEMA, ENUM1_AB_SCHEMA),
      new ReaderWriter(ENUM1_AB_SCHEMA, INT_SCHEMA),

      new ReaderWriter(STRING_UNION_SCHEMA, STRING_INT_UNION_SCHEMA),
      new ReaderWriter(INT_SCHEMA, STRING_INT_UNION_SCHEMA),

      new ReaderWriter(A_INT_B_INT_REQUIRED_RECORD1, A_INT_RECORD1)
  );

  SchemaValidatorBuilder builder = new SchemaValidatorBuilder();

  Schema rec = ObjectSchema.builder()
      .addPropertySchema("a", NumberSchema.builder().requiresInteger(true).defaultValue(1).build())
      .addPropertySchema("b", NumberSchema.builder().requiresNumber(true).build())
      .additionalProperties(false)
      .build();
  Schema rec2 = ObjectSchema.builder()
      .addPropertySchema("a", NumberSchema.builder().requiresInteger(true).defaultValue(1).build())
      .addPropertySchema("b", NumberSchema.builder().requiresNumber(true).build())
      .addPropertySchema("c", NumberSchema.builder().requiresInteger(true).defaultValue(0).build())
      .additionalProperties(false)
      .build();
  Schema rec3 = ObjectSchema.builder()
      .addPropertySchema("b", NumberSchema.builder().requiresNumber(true).build())
      .addPropertySchema("c", NumberSchema.builder().requiresInteger(true).defaultValue(0).build())
      .additionalProperties(true)
      .build();
  Schema rec4 = ObjectSchema.builder()
      .addPropertySchema("b", NumberSchema.builder().requiresNumber(true).build())
      .addPropertySchema("c", NumberSchema.builder().requiresInteger(true).build())
      .additionalProperties(false)
      .build();

  @Test
  public void testAllTypes() {
    Schema s = ObjectSchema.builder()
        .addPropertySchema("boolF", BooleanSchema.builder().build())
        .addRequiredProperty("boolF")
        .addPropertySchema("intF", NumberSchema.builder().requiresInteger(true).build())
        .addRequiredProperty("intF")
        .addPropertySchema("numberF", NumberSchema.builder().requiresNumber(true).build())
        .addRequiredProperty("numberF")
        .addPropertySchema("stringF", StringSchema.builder().build())
        .addRequiredProperty("stringF")
        .addPropertySchema("enumF", EnumSchema.builder().possibleValue("S").build())
        .addRequiredProperty("enumF")
        .addPropertySchema(
            "arrayF",
            ArraySchema.builder().allItemSchema(StringSchema.builder().build()).build()
        )
        .addRequiredProperty("arrayF")
        .addPropertySchema(
            "recordF",
            ObjectSchema.builder().addPropertySchema("f", NumberSchema.builder().build()).build()
        )
        .addRequiredProperty("recordF")
        .addPropertySchema("bool0", BooleanSchema.builder().build())
        .build();
    testValidatorPasses(builder.mutualReadStrategy().validateLatest(), s, s);
  }

  @Test
  public void testReadOnePrior() {
    testValidatorPasses(builder.canReadStrategy().validateLatest(), rec3, rec);
    testValidatorFails(builder.canReadStrategy().validateLatest(), rec4, rec);
  }

  @Test
  public void testReadAllPrior() {
    testValidatorPasses(builder.canReadStrategy().validateAll(), rec3, rec, rec2);
    testValidatorFails(builder.canReadStrategy().validateAll(), rec4, rec, rec2, rec3);
  }

  @Test
  public void testOnePriorCanRead() {
    testValidatorPasses(builder.canBeReadStrategy().validateLatest(), rec, rec3);
    testValidatorFails(builder.canBeReadStrategy().validateLatest(), rec, rec4);
  }

  @Test
  public void testAllPriorCanRead() {
    testValidatorPasses(builder.canBeReadStrategy().validateAll(), rec, rec3, rec2);
    testValidatorFails(builder.canBeReadStrategy().validateAll(), rec, rec4, rec3, rec2);
  }

  @Test
  public void testUnionWithIncompatibleElements() {
    Schema union1 = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(ArraySchema.builder().allItemSchema(rec).build())
        .build();
    Schema union2 = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(ArraySchema.builder().allItemSchema(rec4).build())
        .build();
    testValidatorFails(builder.canReadStrategy().validateAll(), union2, union1);
  }

  @Test
  public void testUnionWithCompatibleElements() {
    Schema union1 = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(ArraySchema.builder().allItemSchema(rec).build())
        .build();
    Schema union2 = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(ArraySchema.builder().allItemSchema(rec3).build())
        .build();
    testValidatorPasses(builder.canReadStrategy().validateAll(), union2, union1);
  }

  @Test
  public void testSchemaCompatibilitySuccesses() {
    for (ReaderWriter tc : COMPATIBLE_READER_WRITER_TEST_CASES) {
      testValidatorPasses(builder.canReadStrategy().validateAll(), tc.getReader(), tc.getWriter());
    }
  }

  @Test
  public void testSchemaCompatibilityFailures() {
    for (ReaderWriter tc : INCOMPATIBLE_READER_WRITER_TEST_CASES) {
      Schema reader = tc.getReader();
      Schema writer = tc.getWriter();
      SchemaValidator validator = builder.canReadStrategy().validateAll();
      List<String> valid = validator.validate(
          new JsonSchema(reader),
          Collections.singleton(new SimpleParsedSchemaHolder(new JsonSchema(writer)))
      );
      Assert.assertFalse(valid.isEmpty());
    }
  }

  private void testValidatorPasses(SchemaValidator validator, Schema schema, Schema... prev) {
    ArrayList<ParsedSchemaHolder> prior = new ArrayList<>();
    for (int i = prev.length - 1; i >= 0; i--) {
      prior.add(new SimpleParsedSchemaHolder(new JsonSchema(prev[i])));
    }
    validator.validate(new JsonSchema(schema), prior);
  }

  private void testValidatorFails(SchemaValidator validator, Schema schemaFails, Schema... prev) {
    ArrayList<ParsedSchemaHolder> prior = new ArrayList<>();
    for (int i = prev.length - 1; i >= 0; i--) {
      prior.add(new SimpleParsedSchemaHolder(new JsonSchema(prev[i])));
    }
    List<String> valid = validator.validate(new JsonSchema(schemaFails), prior);
    Assert.assertFalse(valid.isEmpty());
  }
}
