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

package io.confluent.kafka.serializers.context;

import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;

import java.util.Map;

/**
 * A {@link NullContextNameStrategy} that returns null (indicating no specific context).
 */
public class NullContextNameStrategy implements ContextNameStrategy {

  public NullContextNameStrategy() {
  }

  @Override
  public void configure(Map<String, ?> config) {
  }

  /**
   * For a given topic, returns the context name to use.
   *
   * @param topic The Kafka topic name.
   * @return The context name to use
   */
  public String contextName(String topic) {
    return null;
  }
}
