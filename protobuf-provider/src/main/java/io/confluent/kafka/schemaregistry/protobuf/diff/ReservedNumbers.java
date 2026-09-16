/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.protobuf.diff;

import com.squareup.wire.schema.internal.parser.ReservedElement;
import java.util.ArrayList;
import java.util.List;
import kotlin.ranges.IntRange;

/**
 * Helpers for checking {@code reserved} field/enum-value numbers, shared between
 * {@link MessageSchemaDiff} and {@link EnumSchemaDiff}. Only numeric entries (tags and tag
 * ranges) are considered; reserved names do not protect a number from reuse.
 */
final class ReservedNumbers {
  private ReservedNumbers() {
  }

  static boolean isReserved(List<ReservedElement> reserveds, int tag) {
    for (ReservedElement reserved : reserveds) {
      for (Object value : reserved.getValues()) {
        if (value instanceof Integer) {
          if ((Integer) value == tag) {
            return true;
          }
        } else if (value instanceof IntRange) {
          IntRange range = (IntRange) value;
          if (tag >= range.getStart() && tag <= range.getEndInclusive()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Returns true if any number covered by {@code original}'s reserved ranges is no longer
   * covered by {@code update}'s reserved ranges, regardless of whether that number is now
   * used by a real field/enum value.
   */
  static boolean anyNumberNoLongerReserved(
      List<ReservedElement> original, List<ReservedElement> update) {
    List<int[]> updateRanges = mergeRanges(numericRanges(update));
    for (int[] range : numericRanges(original)) {
      if (!isFullyCovered(range, updateRanges)) {
        return true;
      }
    }
    return false;
  }

  private static List<int[]> numericRanges(List<ReservedElement> reserveds) {
    List<int[]> ranges = new ArrayList<>();
    for (ReservedElement reserved : reserveds) {
      for (Object value : reserved.getValues()) {
        if (value instanceof Integer) {
          int tag = (Integer) value;
          ranges.add(new int[] {tag, tag});
        } else if (value instanceof IntRange) {
          IntRange range = (IntRange) value;
          ranges.add(new int[] {range.getStart(), range.getEndInclusive()});
        }
      }
    }
    return ranges;
  }

  private static List<int[]> mergeRanges(List<int[]> ranges) {
    List<int[]> sorted = new ArrayList<>(ranges);
    sorted.sort((a, b) -> Integer.compare(a[0], b[0]));
    List<int[]> merged = new ArrayList<>();
    for (int[] range : sorted) {
      if (!merged.isEmpty() && range[0] <= merged.get(merged.size() - 1)[1] + 1) {
        int[] last = merged.get(merged.size() - 1);
        last[1] = Math.max(last[1], range[1]);
      } else {
        merged.add(new int[] {range[0], range[1]});
      }
    }
    return merged;
  }

  private static boolean isFullyCovered(int[] range, List<int[]> mergedRanges) {
    for (int[] covering : mergedRanges) {
      if (covering[0] <= range[0] && covering[1] >= range[1]) {
        return true;
      }
    }
    return false;
  }
}
