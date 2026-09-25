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


/**
 * Schema field provenance across a sequence of {@link
 * io.confluent.kafka.schemaregistry.type.logical.LogicalType} versions.
 *
 * <p>Schema evolution systems often focus on managing column IDs, but the underlying objective is
 * to determine the correct projection of a record written under one schema onto an evolved one.
 * This package computes the correspondence that projection needs, directly from the schemas: a
 * provenance id for every member location, the same id wherever a location continues another.
 *
 * <h2>Why provenance rather than names</h2>
 *
 * <p>Given a source {@code name[P1], age[P2]} and a target {@code full_name[P1], age[P2],
 * address[P3]}, the members common to both are {@code {P1, P2}}. That {@code name} became
 * {@code full_name} does not enter into it: both occurrences carry {@code P1}. The same holds for a
 * Protobuf field renamed while keeping its number. And when a name is dropped and later reused by
 * an unrelated member, no chain of matches joins the two occurrences, so they never
 * correspond — which is the case naive name matching gets wrong in the data-corrupting direction.
 *
 * <h2>Relation to column IDs</h2>
 *
 * <p>A persistent column ID is a centrally allocated identifier serving the same correspondence
 * role as provenance. Each version is matched against the one before it alone, so the ids over any
 * range of versions pair them exactly as the whole history does: a metastore can derive and
 * persist column IDs from them, while a disconnected compute engine computes the correspondence
 * over just the window it cares about and projects without coordinating with anyone.
 *
 * @see io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceComputer
 */
package io.confluent.kafka.schemaregistry.type.logical.provenance;
