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

package io.confluent.kafka.schemaregistry.rules.cel;

import com.google.protobuf.Timestamp;
import dev.cel.runtime.CelVariableResolver;
import java.time.Instant;
import java.util.Optional;

final class NowVariable implements CelVariableResolver {

  static final String NOW_NAME = "now";

  private Timestamp now;

  @Override
  public Optional<Object> find(String name) {
    if (!NOW_NAME.equals(name)) {
      return Optional.empty();
    }
    if (now == null) {
      Instant instant = Instant.now();
      now = Timestamp.newBuilder()
          .setSeconds(instant.getEpochSecond())
          .setNanos(instant.getNano())
          .build();
    }
    return Optional.of(now);
  }
}
