/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafka.serializers.context.strategy;

import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * A {@link ContextNameStrategy} is used by a serializer or deserializer to determine
 * the context name used with the schema registry.
 */
public interface ContextNameStrategy extends Configurable {

  @Override
  default void configure(Map<String, ?> var1) {
  }

  /**
   * For a given topic, returns the context name to use.
   *
   * @param topic The Kafka topic name.
   * @return The context name to use
   */
  String contextName(String topic);
}
