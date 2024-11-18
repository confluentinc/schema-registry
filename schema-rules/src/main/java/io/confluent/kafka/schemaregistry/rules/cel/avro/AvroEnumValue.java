/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules.cel.avro;

import static org.projectnessie.cel.common.types.StringT.stringOf;

import org.apache.avro.generic.GenericEnumSymbol;
import org.projectnessie.cel.common.types.ref.Val;

public final class AvroEnumValue {

  private final AvroEnumDescription enumType;
  private final Val stringValue;

  AvroEnumValue(AvroEnumDescription enumType, String enumValue) {
    this.enumType = enumType;
    this.stringValue = stringOf(enumValue);
  }

  static String fullyQualifiedName(GenericEnumSymbol<?> value) {
    return value.getSchema().getFullName() + '.' + value;
  }

  String fullyQualifiedName() {
    return enumType.fullName() + "." + stringValue.value();
  }

  Val stringValue() {
    return stringValue;
  }
}
