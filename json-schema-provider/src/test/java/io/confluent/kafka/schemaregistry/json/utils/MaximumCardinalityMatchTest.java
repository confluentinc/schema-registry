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

package io.confluent.kafka.schemaregistry.json.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MaximumCardinalityMatchTest {

  @Test
  public void testMatching() {
    Set<Edge<Integer, Integer>> edges = new HashSet<>();
    edges.add(new Edge<>(1, 6, 0));
    edges.add(new Edge<>(1, 7, 0));
    edges.add(new Edge<>(1, 8, 0));
    edges.add(new Edge<>(1, 9, 0));
    edges.add(new Edge<>(1, 10, 0));
    edges.add(new Edge<>(2, 6, 0));
    edges.add(new Edge<>(2, 9, 0));
    edges.add(new Edge<>(3, 7, 0));
    edges.add(new Edge<>(3, 9, 0));
    edges.add(new Edge<>(4, 7, 0));
    edges.add(new Edge<>(4, 9, 0));
    edges.add(new Edge<>(4, 10, 0));
    edges.add(new Edge<>(5, 6, 0));
    Set<Integer> partition1 = new HashSet<>();
    partition1.add(1);
    partition1.add(2);
    partition1.add(3);
    partition1.add(4);
    partition1.add(5);
    Set<Integer> partition2 = new HashSet<>();
    partition2.add(6);
    partition2.add(7);
    partition2.add(8);
    partition2.add(9);
    partition2.add(10);
    MaximumCardinalityMatch<Integer, Integer> match =
        new MaximumCardinalityMatch<>(edges, partition1, partition2);
    Set<Edge<Integer, Integer>> matching = match.getMatching();
    assertEquals(5, matching.size());
    assertTrue(matching.contains(new Edge<>(1, 8, 0)));
    assertTrue(matching.contains(new Edge<>(2, 9, 0)));
    assertTrue(matching.contains(new Edge<>(3, 7, 0)));
    assertTrue(matching.contains(new Edge<>(4, 10, 0)));
    assertTrue(matching.contains(new Edge<>(5, 6, 0)));
  }
}