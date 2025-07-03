/*
 * Copyright 2014-2022 Confluent Inc.
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

package io.confluent.kafka.serializers;

import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;

public class AvroData {

  private static final GenericData INSTANCE = new GenericData();

  static {
    addLogicalTypeConversion(INSTANCE);
  }

  public static GenericData getGenericData() {
    return INSTANCE;
  }

  public static void addLogicalTypeConversion(GenericData avroData) {
    avroData.addLogicalTypeConversion(new Conversions.DecimalConversion());
    avroData.addLogicalTypeConversion(new Conversions.UUIDConversion());

    avroData.addLogicalTypeConversion(new TimeConversions.DateConversion());

    avroData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
    avroData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());

    avroData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    avroData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());

    avroData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
    avroData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
  }
}
