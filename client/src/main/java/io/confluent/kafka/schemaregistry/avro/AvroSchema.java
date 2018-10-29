/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.avro;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import java.util.Collections;
import java.util.Objects;

public class AvroSchema implements ParsedSchema {

  public static final String AVRO = "AVRO";

  private static final SchemaValidator BACKWARD_VALIDATOR =
      new SchemaValidatorBuilder().canReadStrategy().validateLatest();

  public final Schema schemaObj;
  private final String canonicalString;

  public AvroSchema(Schema schemaObj) {
    this.schemaObj = schemaObj;
    this.canonicalString = schemaObj.toString();
  }

  public AvroSchema(String schemaString) {
    Schema.Parser parser = new Schema.Parser();
    parser.setValidateDefaults(false);
    Schema schemaObj = parser.parse(schemaString);
    
    this.schemaObj = schemaObj;
    this.canonicalString = schemaObj.toString();
  }

  @Override
  public String schemaType() {
    return AVRO;
  }

  @Override
  public String canonicalString() {
    return canonicalString;
  }

  @Override
  public Integer version() {
    // The version is set directly on the Schema instead.
    return null;
  }

  @Override
  public boolean isBackwardCompatible(ParsedSchema previousSchema) {
    if (!schemaType().equals(previousSchema.schemaType())) {
      return false;
    }
    try {
      BACKWARD_VALIDATOR.validate(this.schemaObj,
          Collections.singleton(((AvroSchema) previousSchema).schemaObj));
      return true;
    } catch (SchemaValidationException e) {
      return false;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AvroSchema that = (AvroSchema) o;
    return Objects.equals(schemaObj, that.schemaObj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaObj);
  }

  @Override
  public String toString() {
    return schemaObj.toString();
  }
}
