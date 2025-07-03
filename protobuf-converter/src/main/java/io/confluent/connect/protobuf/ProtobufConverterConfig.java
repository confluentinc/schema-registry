/*
 * Copyright 2020-2025 Confluent Inc.
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
 */

package io.confluent.connect.protobuf;

import java.util.Map;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public class ProtobufConverterConfig extends AbstractKafkaSchemaSerDeConfig {

  public ProtobufConverterConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }
}
