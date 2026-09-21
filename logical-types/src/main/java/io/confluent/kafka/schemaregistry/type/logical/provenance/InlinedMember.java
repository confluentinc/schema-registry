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

import java.util.List;

/**
 * One member of one version, as a consumer that has inlined every named type sees it.
 *
 * <p>The path is the coordinate such a consumer walks; the names are the same route in words and
 * are informational only; the location is what identifies this member across versions.
 */
public final class InlinedMember {

  private final List<Integer> path;
  private final List<String> names;
  private final LocatedProvenance location;

  InlinedMember(List<Integer> path, List<String> names, LocatedProvenance location) {
    this.path = path;
    this.names = names;
    this.location = location;
  }

  /**
   * The inlined index path: a struct field or union branch appends its index, an array or multiset
   * element appends {@code 0}, and a map appends {@code 0} for its key and {@code 1} for its value.
   */
  public List<Integer> getPath() {
    return path;
  }

  /**
   * The same route in names, with {@code []}, <code>{key}</code> and <code>{value}</code> for
   * collection steps.
   *
   * <p>Informational. Nothing should parse it: those tokens are unforgeable for Avro and
   * Protobuf, whose name grammars exclude brackets, but not for JSON Schema, where a property
   * may legitimately be called {@code []}.
   */
  public List<String> getNames() {
    return names;
  }

  /**
   * What identifies this member — the chain of provenances locating it, so two uses of one shared
   * named type stay apart.
   */
  public LocatedProvenance getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return path + " " + names + " " + location;
  }
}
