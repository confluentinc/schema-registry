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

package io.confluent.kafka.converters;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

public class TBaseToGenericRecord {
  public static GenericRecord convert(TBase message, Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    int index = 1;
    TFieldIdEnum field = message.fieldForId(index);
    while (field != null) {
      String fieldName = field.getFieldName();
      Object value = message.getFieldValue(field);
      Schema fieldSchema = schema.getField(fieldName).schema();
      setField(record, fieldName, value, fieldSchema);
      index = index + 1;
      field = message.fieldForId(index);
    }
    return record;
  }

  private static void setField(GenericRecord record, String fieldName, Object value,
                               Schema fieldSchema) {
    Schema.Type type = fieldSchema.getType();
    // TODO - handling nested types of maps and array
    switch (type) {
      case UNION:
        Schema fschema = fieldSchema.getTypes().get(1); // TODO - iterate all types
        setField(record, fieldName, value, fschema);
        break;
      case RECORD:
        record.put(fieldName, TBaseToGenericRecord.convert(
                (TBase) value, fieldSchema));
        break;
      default:
        record.put(fieldName, value);
    }
  }
}
