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

import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.projectnessie.cel.common.types.pb.Checked;

public final class AvroEnumDescription {

  private final String fullName;
  private final com.google.api.expr.v1alpha1.Type pbType;
  private final List<String> enumValues;

  AvroEnumDescription(Schema schema) {
    this.fullName = schema.getFullName();
    this.enumValues = schema.getEnumSymbols();
    this.pbType = Checked.checkedString;
  }

  String fullName() {
    return fullName;
  }

  com.google.api.expr.v1alpha1.Type pbType() {
    return pbType;
  }

  Stream<AvroEnumValue> buildValues() {
    return enumValues.stream().map(v -> new AvroEnumValue(this, v));
  }
}
