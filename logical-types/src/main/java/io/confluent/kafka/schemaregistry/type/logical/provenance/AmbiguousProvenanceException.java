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
 * A history whose names and aliases do not determine one identity per location — two fields
 * aliasing one old name, a type aliasing two old types, an alias that is its own name. A property
 * of the registered schemas, so a caller cannot have provenance for it and should stop asking.
 */
public class AmbiguousProvenanceException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  // The version it names, or -1, and the message around it: the computer counts versions from 0,
  // and a caller knowing them by number names the version again with withVersion.
  private final int version;
  private final String before;
  private final String after;

  public AmbiguousProvenanceException(String message) {
    this(message, -1, "");
  }

  AmbiguousProvenanceException(String before, int version, String after) {
    super(before + (version >= 0 ? String.valueOf(version) : "") + after);
    this.version = version;
    this.before = before;
    this.after = after;
  }

  // The version it names, as its thrower counted it; -1 if none.
  int version() {
    return version;
  }

  // As this, naming the version number instead.
  AmbiguousProvenanceException withVersion(int number) {
    return version < 0 ? this : new AmbiguousProvenanceException(before, number, after);
  }
}
