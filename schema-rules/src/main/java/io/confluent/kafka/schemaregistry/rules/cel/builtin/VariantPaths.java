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

/**
 * Checks a variant path as the variants functions parse it, so a caller can refuse a malformed
 * one before it fails at evaluation.
 */
public final class VariantPaths {

  private VariantPaths() {
  }

  /**
   * Throws {@link IllegalArgumentException}, with the parser's message, if {@code path} is not
   * a valid variant path.
   */
  public static void check(String path) {
    VariantPath.parse(path);
  }
}
