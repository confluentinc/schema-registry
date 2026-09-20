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

import java.util.Arrays;

/**
 * One container's correspondence expressed as positions: for each member of the target container,
 * the position of the source member that feeds it.
 *
 * <p>The form a projecting consumer wants. It carries the source container's arity too, because a
 * consumer reading a nested row from a source row has to ask for the source's field count, not the
 * target's.
 */
public final class PositionMapping {

  private final int[] sourcePositions;
  private final int sourceArity;

  PositionMapping(int[] sourcePositions, int sourceArity) {
    this.sourcePositions = sourcePositions;
    this.sourceArity = sourceArity;
  }

  /**
   * Per target member position, the source position feeding it, or {@link ProvenanceResult#ABSENT}.
   * Not a valid index when absent: such a member has no source to read from, only a default or
   * null to supply.
   */
  public int[] getSourcePositions() {
    return sourcePositions.clone();
  }

  /** How many members the source container has. */
  public int getSourceArity() {
    return sourceArity;
  }

  /** True when every target member is fed by the source member at the same position. */
  public boolean isIdentity() {
    if (sourceArity != sourcePositions.length) {
      return false;
    }
    for (int i = 0; i < sourcePositions.length; i++) {
      if (sourcePositions[i] != i) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return Arrays.toString(sourcePositions) + " of " + sourceArity;
  }
}
