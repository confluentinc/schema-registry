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

import java.util.Objects;

/**
 * A {@link FormatIdentity} for a name resolved through the alias index, tagged with the version
 * that minted it.
 *
 * <p>The minting version is part of the value, not decoration. A name released by a still-present
 * entity is dropped from the resolution index, so a later entity reusing that name mints a fresh
 * identity — and only the minting version keeps the two apart. It is distinct from a
 * {@link Provenance}'s presence start: the minting version distinguishes identities, the presence
 * start distinguishes lifetimes of one identity.
 */
public final class MintedIdentity implements FormatIdentity {

  private final String name;
  private final int originVersion;

  MintedIdentity(String name, int originVersion) {
    this.name = Objects.requireNonNull(name, "name");
    this.originVersion = originVersion;
  }

  /** The name this identity was minted under. Later versions may rename it through an alias. */
  public String getName() {
    return name;
  }

  /** The index, within the supplied sequence, of the version that minted this identity. */
  public int getOriginVersion() {
    return originVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MintedIdentity)) {
      return false;
    }
    MintedIdentity that = (MintedIdentity) o;
    return originVersion == that.originVersion && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, originVersion);
  }

  @Override
  public String toString() {
    return name + "@v" + originVersion;
  }
}
