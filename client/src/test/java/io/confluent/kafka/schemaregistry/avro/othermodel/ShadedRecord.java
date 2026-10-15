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

package io.confluent.kafka.schemaregistry.avro.othermodel;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecordBase;

// A specific record relocated by a shading plugin: its SCHEMA$ keeps the original namespace,
// which Avro replaces with the class's package when deriving its schema
public class ShadedRecord extends SpecificRecordBase {
  public static final Schema SCHEMA$ = SchemaBuilder.record("ShadedRecord")
      .namespace("com.original.model").fields()
      .name("child").type(SchemaBuilder.record("ShadedChild").namespace("com.original.model")
          .fields().requiredInt("x").endRecord()).noDefault()
      .endRecord();

  @Override
  public Schema getSchema() {
    return SCHEMA$;
  }

  @Override
  public Object get(int field) {
    return null;
  }

  @Override
  public void put(int field, Object value) {
  }
}
