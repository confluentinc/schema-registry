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

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Schemas used by other tests in this package. Therefore package protected.
 */
public class TestSchemas {

  static final Schema BOOLEAN_SCHEMA = BooleanSchema.builder().build();
  static final Schema INT_SCHEMA = NumberSchema.builder().requiresInteger(true).build();
  static final Schema NUMBER_SCHEMA = NumberSchema.builder().requiresNumber(true).build();
  static final Schema STRING_SCHEMA = StringSchema.builder().build();

  static final Schema INT_ARRAY_SCHEMA = ArraySchema.builder().allItemSchema(INT_SCHEMA).build();
  static final Schema NUMBER_ARRAY_SCHEMA = ArraySchema.builder()
      .allItemSchema(NUMBER_SCHEMA)
      .build();
  static final Schema STRING_ARRAY_SCHEMA = ArraySchema.builder()
      .allItemSchema(STRING_SCHEMA)
      .build();

  static final Schema ENUM1_AB_SCHEMA = EnumSchema.builder()
      .possibleValue("A")
      .possibleValue("B")
      .build();
  static final Schema ENUM1_ABC_SCHEMA = EnumSchema.builder()
      .possibleValue("A")
      .possibleValue("B")
      .possibleValue("C")
      .build();
  static final Schema ENUM1_BC_SCHEMA = EnumSchema.builder()
      .possibleValue("B")
      .possibleValue("C")
      .build();
  static final Schema STRING_UNION_SCHEMA = CombinedSchema.builder()
      .criterion(CombinedSchema.ONE_CRITERION)
      .subschema(STRING_SCHEMA)
      .build();
  static final Schema STRING_INT_UNION_SCHEMA = CombinedSchema.builder()
      .criterion(CombinedSchema.ONE_CRITERION)
      .subschema(STRING_SCHEMA)
      .subschema(INT_SCHEMA)
      .build();

  static final Schema EMPTY_RECORD1 = ObjectSchema.builder().additionalProperties(false).build();
  static final Schema A_INT_RECORD1 = ObjectSchema.builder()
      .addPropertySchema("a", NumberSchema.builder().requiresInteger(true).build())
      .additionalProperties(false)
      .build();
  static final Schema A_INT_OPEN_RECORD1 = ObjectSchema.builder()
      .addPropertySchema("a", NumberSchema.builder().requiresInteger(true).build())
      .additionalProperties(true)
      .build();
  static final Schema A_INT_B_INT_RECORD1 = ObjectSchema.builder()
      .addPropertySchema("a", NumberSchema.builder().requiresInteger(true).build())
      .addPropertySchema("b", NumberSchema.builder().requiresInteger(true).build())
      .additionalProperties(false)
      .build();
  static final Schema A_INT_B_INT_REQUIRED_RECORD1 = ObjectSchema.builder()
      .addPropertySchema("a", NumberSchema.builder().requiresInteger(true).build())
      .addPropertySchema("b", NumberSchema.builder().requiresInteger(true).build())
      .addRequiredProperty("b")
      .additionalProperties(false)
      .build();
  static final Schema A_INT_B_DINT_RECORD1 = ObjectSchema.builder()
      .addPropertySchema("a", NumberSchema.builder().requiresInteger(true).build())
      .addPropertySchema("b", NumberSchema.builder().requiresInteger(true).defaultValue(0).build())
      .additionalProperties(false)
      .build();
  static final Schema A_INT_B_DINT_REQUIRED_RECORD1 = ObjectSchema.builder()
      .addPropertySchema("a", NumberSchema.builder().requiresInteger(true).build())
      .addPropertySchema("b", NumberSchema.builder().requiresInteger(true).defaultValue(0).build())
      .addRequiredProperty("b")
      .additionalProperties(false)
      .build();
  static final Schema A_DINT_B_DINT_RECORD1 = ObjectSchema.builder()
      .addPropertySchema("a", NumberSchema.builder().requiresInteger(true).defaultValue(0).build())
      .addPropertySchema("b", NumberSchema.builder().requiresInteger(true).defaultValue(0).build())
      .additionalProperties(false)
      .build();

  static final class ReaderWriter {
    private final Schema mReader;
    private final Schema mWriter;

    public ReaderWriter(final Schema reader, final Schema writer) {
      mReader = reader;
      mWriter = writer;
    }

    public Schema getReader() {
      return mReader;
    }

    public Schema getWriter() {
      return mWriter;
    }
  }

  /**
   * Borrowed from the Guava library.
   */
  @SafeVarargs
  static <E> ArrayList<E> list(E... elements) {
    final ArrayList<E> list = new ArrayList<>();
    Collections.addAll(list, elements);
    return list;
  }
}
