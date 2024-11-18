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

import com.google.api.expr.v1alpha1.Type;
import org.apache.avro.Schema;
import org.projectnessie.cel.common.types.ref.FieldGetter;
import org.projectnessie.cel.common.types.ref.FieldTester;
import org.projectnessie.cel.common.types.ref.FieldType;

public final class AvroFieldType extends FieldType {

  private final Schema schema;

  AvroFieldType(
      Type type, FieldTester isSet, FieldGetter getFrom, Schema schema) {
    super(type, isSet, getFrom);
    this.schema = schema;
  }

  Schema schema() {
    return schema;
  }
}
