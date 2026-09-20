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
 * Why a member of the target has no counterpart in the source.
 *
 * <p>A missing entry in a correspondence says only that nothing feeds this member. These say which
 * of three different things happened, which is what an operator-facing message needs: "the column
 * was added after the writer's version" and "the column above it changed from an array to a map"
 * call for very different responses.
 */
public enum Absence {

  /** The source simply does not have this member — it was added, or its interval was broken. */
  NOT_IN_SOURCE,

  /** An ancestor of this member is itself absent, so there is no location to descend from. */
  PARENT_ABSENT,

  /**
   * The containing types stopped agreeing — one side is a struct where the other is a map, say — so
   * correspondence below that point is not expressible as a pair of paths.
   */
  TYPE_DIVERGED
}
