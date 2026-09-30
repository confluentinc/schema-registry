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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

/**
 * A version with more locations than provenance computes. Each use of a named type is a location
 * of its own, so a type used more than once at each of several levels multiplies them with depth;
 * such a schema has no provenance report, and a consumer reads it without provenance.
 */
public class TooManyLocationsException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public TooManyLocationsException(int version, int limit) {
    super("Version " + version + " has more than " + limit + " locations, too many to compute "
        + "provenance for");
  }
}
