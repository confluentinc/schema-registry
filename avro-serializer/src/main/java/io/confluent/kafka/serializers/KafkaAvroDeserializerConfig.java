/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafka.serializers;

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Type;
import io.confluent.common.config.ConfigDef.Importance;

import java.util.Map;

public class KafkaAvroDeserializerConfig extends AbstractKafkaAvroSerDeConfig {

  public static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";
  public static final boolean SPECIFIC_AVRO_READER_DEFAULT = false;
  public static final String SPECIFIC_AVRO_READER_DOC =
      "If true, tries to look up the SpecificRecord class ";
  
  public static final String AVRO_FORCE_NEW_SPECIFIC_DATA_CONFIG = "force.new.specific.avro.instance";
  public static final boolean AVRO_FORCE_NEW_SPECIFIC_DATA_DEFAULT = false;
  public static final String AVRO_FORCE_NEW_SPECIFIC_DATA_DOC = "If true, it passes a new instace of SpecificData to SpecificDatumReader ";

  private static ConfigDef config;

  static {
    config = baseConfigDef()
        .define(SPECIFIC_AVRO_READER_CONFIG, Type.BOOLEAN, SPECIFIC_AVRO_READER_DEFAULT,
                Importance.LOW, SPECIFIC_AVRO_READER_DOC)
        .define(AVRO_FORCE_NEW_SPECIFIC_DATA_CONFIG, Type.BOOLEAN, AVRO_FORCE_NEW_SPECIFIC_DATA_DEFAULT,
                Importance.LOW, AVRO_FORCE_NEW_SPECIFIC_DATA_DOC);
  }

  public KafkaAvroDeserializerConfig(Map<?, ?> props) {
    super(config, props);
  }
}
