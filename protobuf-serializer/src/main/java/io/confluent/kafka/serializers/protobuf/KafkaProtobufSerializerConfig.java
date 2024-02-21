/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package io.confluent.kafka.serializers.protobuf;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;

public class KafkaProtobufSerializerConfig extends AbstractKafkaSchemaSerDeConfig {

  public static final String REFERENCE_LOOKUP_ONLY_CONFIG =
      "reference.lookup.only";
  public static final String REFERENCE_LOOKUP_ONLY_DOC =
      "Regardless of whether auto.register.schemas or use.latest.version is true, only look up "
          + "the ID for references by using the schema.";
  public static final String SKIP_KNOWN_TYPES_CONFIG =
      "skip.known.types";
  public static final String SKIP_KNOWN_TYPES_DOC =
      "Whether to skip known types when resolving schema dependencies.";

  public static final String REFERENCE_SUBJECT_NAME_STRATEGY_CONFIG =
      "reference.subject.name.strategy";
  public static final String REFERENCE_SUBJECT_NAME_STRATEGY_DOC =
      "Determines how to construct the subject name for referenced schemas. "
          + "By default, the reference name is used as subject.";

  private static final ConfigDef config;

  static {
    config = baseConfigDef()
        .define(REFERENCE_LOOKUP_ONLY_CONFIG, ConfigDef.Type.BOOLEAN,
            false, ConfigDef.Importance.MEDIUM,
            REFERENCE_LOOKUP_ONLY_DOC)
        .define(SKIP_KNOWN_TYPES_CONFIG, ConfigDef.Type.BOOLEAN,
            true, ConfigDef.Importance.LOW,
            SKIP_KNOWN_TYPES_DOC)
        .define(REFERENCE_SUBJECT_NAME_STRATEGY_CONFIG, ConfigDef.Type.CLASS,
            DefaultReferenceSubjectNameStrategy.class, ConfigDef.Importance.LOW,
            REFERENCE_SUBJECT_NAME_STRATEGY_DOC);
  }

  public KafkaProtobufSerializerConfig(Map<?, ?> props) {
    super(config, props);
  }

  public boolean onlyLookupReferencesBySchema() {
    return this.getBoolean(REFERENCE_LOOKUP_ONLY_CONFIG);
  }

  public boolean skipKnownTypes() {
    return getBoolean(SKIP_KNOWN_TYPES_CONFIG);
  }

  public ReferenceSubjectNameStrategy referenceSubjectNameStrategyInstance() {
    return this.getConfiguredInstance(REFERENCE_SUBJECT_NAME_STRATEGY_CONFIG,
        ReferenceSubjectNameStrategy.class);
  }
}
