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

package io.confluent.avro.type;

import io.confluent.kafka.schemaregistry.type.Variant;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;

public class VariantConversion extends Conversion<Variant> {
  static final String RECORD_NAME = "confluent.type.Variant";

  @Override
  public Class<Variant> getConvertedType() {
    return Variant.class;
  }

  @Override
  public Schema getRecommendedSchema() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("metadata", Schema.create(Schema.Type.BYTES)));
    fields.add(new Schema.Field("value", Schema.create(Schema.Type.BYTES)));
    Schema schema =
        Schema.createRecord(
            RECORD_NAME,
            null,
            null,
            false,
            fields);
    return VariantLogicalType.get().addToSchema(schema);
  }

  @Override
  public String getLogicalTypeName() {
    return VariantLogicalType.NAME;
  }

  @Override
  public Variant fromRecord(IndexedRecord record, Schema schema, LogicalType type) {
    int metadataPos = schema.getField("metadata").pos();
    int valuePos = schema.getField("value").pos();
    ByteBuffer metadata = (ByteBuffer) record.get(metadataPos);
    ByteBuffer value = (ByteBuffer) record.get(valuePos);
    return new Variant(value, metadata);
  }

  @Override
  public IndexedRecord toRecord(Variant variant, Schema schema, LogicalType type) {
    int metadataPos = schema.getField("metadata").pos();
    int valuePos = schema.getField("value").pos();
    GenericData.Record record = new GenericData.Record(schema);
    record.put(metadataPos, variant.getMetadataBuffer());
    record.put(valuePos, variant.getValueBuffer());
    return record;
  }
}
