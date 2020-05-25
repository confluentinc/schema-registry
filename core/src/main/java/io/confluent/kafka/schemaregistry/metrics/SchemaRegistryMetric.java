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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.concurrent.atomic.AtomicLong;

public class SchemaRegistryMetric {
  private final AtomicLong count = new AtomicLong();
  private final Sensor sensor;

  public SchemaRegistryMetric(Metrics metrics, String sensorName, MetricName metricName) {
    sensor = metrics.sensor(sensorName);
    sensor.add(metricName, new Value());
  }

  public void increment() {
    sensor.record(count.addAndGet(1));
  }

  public void set(long value) {
    count.set(value);
    sensor.record(value);
  }

  public long get() {
    return count.get();
  }
}
