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

package io.confluent.kafka.serializers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RuleMetricsTest {

  private Metrics metrics;
  private PluginMetricsImpl pluginMetrics;
  private RuleMetrics ruleMetrics;

  @Before
  public void setUp() {
    metrics = new Metrics();
    LinkedHashMap<String, String> tags = new LinkedHashMap<>();
    tags.put("config", "value.serializer");
    tags.put("class", "KafkaAvroSerializer");
    pluginMetrics = new PluginMetricsImpl(metrics, tags);
    ruleMetrics = new RuleMetrics(pluginMetrics);
  }

  @After
  public void tearDown() throws Exception {
    pluginMetrics.close();
    metrics.close();
  }

  @Test
  public void recordExecutionIncrementsCounter() {
    Rule rule = encryptRule("redact-pii");
    ruleMetrics.recordExecution(rule, RuleMode.WRITE, "topic-value");
    ruleMetrics.recordExecution(rule, RuleMode.WRITE, "topic-value");
    KafkaMetric m = findMetric("rule.executions.total", encryptTags("redact-pii", "WRITE", "topic-value"));
    assertNotNull("expected rule.executions.total metric to exist", m);
    assertEquals(2.0, (Double) m.metricValue(), 0.0);
  }

  @Test
  public void recordSuccessUpdatesGaugeTimestamp() {
    Rule rule = encryptRule("redact-pii");
    long before = System.currentTimeMillis();
    ruleMetrics.recordSuccess(rule, RuleMode.WRITE, "topic-value");
    long after = System.currentTimeMillis();
    KafkaMetric gauge = findMetric("rule.last.success.timestamp.ms",
        encryptTags("redact-pii", "WRITE", "topic-value"));
    assertNotNull(gauge);
    Long ts = (Long) gauge.metricValue();
    assertTrue("gauge " + ts + " not in [" + before + ", " + after + "]",
        ts >= before && ts <= after);
  }

  @Test
  public void recordSuccessAndFailureSplit() {
    Rule rule = encryptRule("redact-pii");
    ruleMetrics.recordSuccess(rule, RuleMode.WRITE, "topic-value");
    ruleMetrics.recordSuccess(rule, RuleMode.WRITE, "topic-value");
    ruleMetrics.recordFailure(rule, RuleMode.WRITE, "topic-value");
    assertEquals(2.0, value(findMetric("rule.success.total",
        encryptTags("redact-pii", "WRITE", "topic-value"))), 0.0);
    assertEquals(1.0, value(findMetric("rule.failure.total",
        encryptTags("redact-pii", "WRITE", "topic-value"))), 0.0);
  }

  @Test
  public void invocationModeDistinguishesWriteFromRead() {
    // A WRITEREAD-declared rule fires in both modes; the metric tags use
    // the invocation mode, so we get two series.
    Rule rule = encryptRule("encrypt-decrypt");
    ruleMetrics.recordExecution(rule, RuleMode.WRITE, "topic-value");
    ruleMetrics.recordExecution(rule, RuleMode.READ, "topic-value");
    ruleMetrics.recordExecution(rule, RuleMode.WRITE, "topic-value");
    assertEquals(2.0, value(findMetric("rule.executions.total",
        encryptTags("encrypt-decrypt", "WRITE", "topic-value"))), 0.0);
    assertEquals(1.0, value(findMetric("rule.executions.total",
        encryptTags("encrypt-decrypt", "WRITE", "topic-value")
            // override mode
            .with("rule_mode", "READ"))), 0.0);
  }

  @Test
  public void subjectDisambiguatesKeyAndValueSerdes() {
    // Under TopicNameStrategy, key and value serializers see subjects
    // "<topic>-key" and "<topic>-value". They share the underlying
    // Metrics registry, so sensor names must differ — verified by the
    // metric counts being independent.
    Rule rule = encryptRule("redact-pii");
    ruleMetrics.recordExecution(rule, RuleMode.WRITE, "topic-key");
    ruleMetrics.recordExecution(rule, RuleMode.WRITE, "topic-value");
    ruleMetrics.recordExecution(rule, RuleMode.WRITE, "topic-value");
    assertEquals(1.0, value(findMetric("rule.executions.total",
        encryptTags("redact-pii", "WRITE", "topic-key"))), 0.0);
    assertEquals(2.0, value(findMetric("rule.executions.total",
        encryptTags("redact-pii", "WRITE", "topic-value"))), 0.0);
  }

  @Test
  public void recordActionTaggedByAction() {
    Rule rule = encryptRule("redact-pii");
    ruleMetrics.recordAction(rule, RuleMode.WRITE, "topic-value", "DLQ");
    ruleMetrics.recordAction(rule, RuleMode.WRITE, "topic-value", "NONE");
    ruleMetrics.recordAction(rule, RuleMode.WRITE, "topic-value", "DLQ");
    TagMap dlqTags = encryptTags("redact-pii", "WRITE", "topic-value")
        .with("action", "DLQ");
    TagMap noneTags = encryptTags("redact-pii", "WRITE", "topic-value")
        .with("action", "NONE");
    assertEquals(2.0, value(findMetric("rule.actions.total", dlqTags)), 0.0);
    assertEquals(1.0, value(findMetric("rule.actions.total", noneTags)), 0.0);
  }

  @Test
  public void skippedHasItsOwnCounter() {
    Rule rule = encryptRule("redact-pii");
    ruleMetrics.recordSkipped(rule, RuleMode.READ, "topic-value");
    ruleMetrics.recordSkipped(rule, RuleMode.READ, "topic-value");
    assertEquals(2.0, value(findMetric("rule.skipped.total",
        encryptTags("redact-pii", "READ", "topic-value"))), 0.0);
  }

  // ---- helpers ----

  private static Rule encryptRule(String name) {
    return new Rule(name, null, RuleKind.TRANSFORM, RuleMode.WRITEREAD,
        "ENCRYPT_PAYLOAD", null, null, null, null, null, false);
  }

  private static TagMap encryptTags(String name, String mode, String subject) {
    return new TagMap()
        .with("rule_type", "ENCRYPT_PAYLOAD")
        .with("rule_mode", mode)
        .with("subject", subject)
        .with("rule_name", name);
  }

  private KafkaMetric findMetric(String name, TagMap requiredTags) {
    for (Map.Entry<MetricName, KafkaMetric> e : metrics.metrics().entrySet()) {
      MetricName mn = e.getKey();
      if (!name.equals(mn.name())) {
        continue;
      }
      Map<String, String> tags = mn.tags();
      boolean allMatch = true;
      for (Map.Entry<String, String> req : requiredTags.entrySet()) {
        if (!req.getValue().equals(tags.get(req.getKey()))) {
          allMatch = false;
          break;
        }
      }
      if (allMatch) {
        return e.getValue();
      }
    }
    return null;
  }

  private static double value(KafkaMetric m) {
    assertNotNull("no matching metric found", m);
    Object v = m.metricValue();
    if (v instanceof Number) {
      return ((Number) v).doubleValue();
    }
    throw new AssertionError("metric value not numeric: " + v);
  }

  private static final class TagMap extends HashMap<String, String> {
    TagMap with(String k, String v) {
      put(k, v);
      return this;
    }
  }
}
