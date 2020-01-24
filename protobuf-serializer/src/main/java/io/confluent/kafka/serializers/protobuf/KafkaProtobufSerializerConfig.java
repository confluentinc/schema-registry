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

import io.confluent.common.config.ConfigDef;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;

public class KafkaProtobufSerializerConfig extends AbstractKafkaSchemaSerDeConfig {

  public static final String REFERENCE_SUBJECT_NAME_STRATEGY_CONFIG =
      "reference.subject.name.strategy";
  public static final String REFERENCE_SUBJECT_NAME_STRATEGY_DOC =
      "Determines how to construct the subject name for referenced schemas. "
          + "By default, the reference name is used as subject.";

  private static final ConfigDef config;

  static {
    config = baseConfigDef().define(
        REFERENCE_SUBJECT_NAME_STRATEGY_CONFIG,
        ConfigDef.Type.CLASS,
        DefaultReferenceSubjectNameStrategy.class,
        ConfigDef.Importance.LOW,
        REFERENCE_SUBJECT_NAME_STRATEGY_DOC
    );
  }

  public KafkaProtobufSerializerConfig(Map<?, ?> props) {
    super(config, props);
  }

  public ReferenceSubjectNameStrategy referenceSubjectNameStrategyInstance() {
    return this.getConfiguredInstance(REFERENCE_SUBJECT_NAME_STRATEGY_CONFIG,
        ReferenceSubjectNameStrategy.class);
  }
}
