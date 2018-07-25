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

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public class GeneratedMessageToGenericRecord {
  public static GenericRecord convert(GeneratedMessage message, Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      String fieldName = entry.getKey().getName();
      Object value = entry.getValue();
      Schema fieldSchema = schema.getField(fieldName).schema();
      setField(record, fieldName, value, fieldSchema);
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
        record.put(fieldName, GeneratedMessageToGenericRecord.convert(
                (GeneratedMessage) value, fieldSchema));
        break;
      default:
        record.put(fieldName, value);
    }
  }
}
