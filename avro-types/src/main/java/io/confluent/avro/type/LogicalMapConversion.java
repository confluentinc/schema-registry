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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;

public class LogicalMapConversion extends Conversion<Map> {

  @Override
  @SuppressWarnings("rawtypes")
  public Class<Map> getConvertedType() {
    return Map.class;
  }

  @Override
  public String getLogicalTypeName() {
    return LogicalMap.NAME;
  }

  @Override
  public Map<Object, Object> fromArray(
      Collection<?> value, Schema schema, LogicalType type) {
    Map<Object, Object> map = new LinkedHashMap<>(value.size());
    for (Object entry : value) {
      IndexedRecord record = (IndexedRecord) entry;
      map.put(record.get(0), record.get(1));
    }
    return map;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Collection<IndexedRecord> toArray(
      Map value, Schema schema, LogicalType type) {
    Schema entrySchema = schema.getElementType();
    List<IndexedRecord> records = new ArrayList<>(value.size());
    for (Object entryObj : value.entrySet()) {
      Map.Entry<?, ?> entry = (Map.Entry<?, ?>) entryObj;
      GenericData.Record record = new GenericData.Record(entrySchema);
      record.put(0, entry.getKey());
      record.put(1, entry.getValue());
      records.add(record);
    }
    return records;
  }

}
