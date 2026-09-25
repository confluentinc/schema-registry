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
 * What an occurrence is matched by — an occurrence continues the previous version's occurrence of
 * the same identity.
 *
 * <p>An identity is a {@link EntityKind}, a {@link Scope} and a {@link FormatIdentity}. It is never
 * independently allocated: it is derived from the schema's structure and the previous version.
 * All three parts matter. The kind keeps a named type and a member of the same name apart; the
 * scope keeps {@code User.name} and {@code Order.name} apart; the format identity distinguishes an
 * entity from its peers.
 *
 * <p>An identity is itself a {@link Scope}: the members of an entity's type are scoped by that
 * entity's identity.
 *
 * <p>Use {@link Provenance}, not this, as the cross-schema correspondence key. Two occurrences can
 * share an identity and still belong to different chains — a Protobuf field number re-added
 * after a gap — which must not correspond.
 */
public final class Identity implements Scope {

  private final EntityKind kind;
  private final Scope scope;
  private final FormatIdentity formatIdentity;
  private final int hash;

  Identity(EntityKind kind, Scope scope, FormatIdentity formatIdentity) {
    this.kind = Objects.requireNonNull(kind, "kind");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.formatIdentity = Objects.requireNonNull(formatIdentity, "formatIdentity");
    this.hash = Objects.hash(kind, scope, formatIdentity);
  }

  /** What kind of entity this identity belongs to. */
  public EntityKind getKind() {
    return kind;
  }

  /** The hierarchical context this identity was resolved in. */
  public Scope getScope() {
    return scope;
  }

  /** How this entity is distinguished from its peers within the scope. */
  public FormatIdentity getFormatIdentity() {
    return formatIdentity;
  }

  /**
   * The Protobuf field number this identity follows, or {@code null} when it is name-identified.
   */
  public Integer getFieldNumber() {
    return formatIdentity instanceof IntegerIdentity
        ? ((IntegerIdentity) formatIdentity).getValue() : null;
  }

  /**
   * The version that minted this logical identity, or {@code null} when the format identity is
   * stable on its own terms — a field number or an aliasless name has no minting version.
   */
  public Integer getIdentityOriginVersion() {
    return formatIdentity instanceof MintedIdentity
        ? ((MintedIdentity) formatIdentity).getOriginVersion() : null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Identity)) {
      return false;
    }
    Identity that = (Identity) o;
    return hash == that.hash
        && kind == that.kind
        && formatIdentity.equals(that.formatIdentity)
        && scope.equals(that.scope);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    return scope + "." + formatIdentity;
  }
}
