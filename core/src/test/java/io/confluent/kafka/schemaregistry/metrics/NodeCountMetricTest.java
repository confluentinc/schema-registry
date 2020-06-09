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

package io.confluent.kafka.schemaregistry.metrics;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class NodeCountMetricTest extends ClusterTestHarness {

  @Test
  public void testNodeCountMetric() throws Exception {
    int numFollowers = 2;
    int numLeaders = 2;

    Set<RestApp> followerApps = new HashSet<>();
    for (int i = 0; i < numFollowers; i++) {
      RestApp follower = new RestApp(choosePort(), null, bootstrapServers, KAFKASTORE_TOPIC,
              CompatibilityLevel.NONE.name, false, null);
      followerApps.add(follower);
      follower.start();
    }

    // Make a leader-eligible 'cluster'
    Set<RestApp> leaderApps = new HashSet<>();
    for (int i = 0; i < numLeaders; i++) {
      RestApp leader = new RestApp(choosePort(),
              null, bootstrapServers, KAFKASTORE_TOPIC,
              CompatibilityLevel.NONE.name, true, null);
      leaderApps.add(leader);
      leader.start();
      TestUtils.waitUntilLeaderElectionCompletes(leaderApps);
      checkNodeCountMetric(leaderApps, followerApps);
    }

    // Kill the current leader and wait for reelection until no leaders are left
    while (!leaderApps.isEmpty()) {
      RestApp reportedLeader = TestUtils.checkOneLeader(leaderApps);
      leaderApps.remove(reportedLeader);

      reportedLeader.stop();
      TestUtils.waitUntilLeaderElectionCompletes(leaderApps);
      checkNodeCountMetric(leaderApps, followerApps);
    }

    for (RestApp follower : followerApps) {
      follower.stop();
    }
  }

  private void checkNodeCountMetric(final Collection<RestApp>... apps) {
    final long count = Arrays.stream(apps).map(x -> x.size()).reduce(0, Integer::sum);
    TestUtils.waitUntilTrue(() -> {
      for (Collection<RestApp> collection : apps) {
        for (RestApp app : collection) {
          if (app.restApp.schemaRegistry().getMetricsContainer().getNodeCountMetric().get()
                  != count) {
            return false;
          }
        }
      }
      return true;
    }, 3000, "Metrics should have been updated by now");
  }
}
