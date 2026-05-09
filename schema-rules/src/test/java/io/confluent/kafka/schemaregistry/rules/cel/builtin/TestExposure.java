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

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

import com.google.protobuf.Timestamp;
import java.math.BigDecimal;
import java.util.List;

/**
 * Test-only re-export of package-private helpers used by edge-case tests in
 * the parent {@code rules.cel} package. Keeps the production helpers
 * package-private (the package boundary is the API surface) while allowing
 * focused unit tests to call them directly.
 */
public final class TestExposure {

  private TestExposure() {
  }

  public static BigDecimal toBigDecimal(Object o) {
    return DecimalUtils.toBigDecimal(o);
  }

  public static BigDecimal toBigDecimal(byte[] bytes, int scale) {
    return DecimalUtils.toBigDecimal(bytes, scale);
  }

  public static Timestamp toTimestamp(Object o) {
    return TimestampUtils.toTimestamp(o);
  }

  public static Timestamp toTimestamp(long value, String unit) {
    return TimestampUtils.fromEpoch(value, unit);
  }

  public static List<VariantPath.Segment> parsePath(String path) {
    return VariantPath.parse(path);
  }
}
