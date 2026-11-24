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

package io.confluent.kafka.serializers.metrics;

import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;

/**
 * Rule-metrics registry for
 * {@link io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe}.
 *
 * <p>Owns the lazy registration of one {@link Sensor} (with a single
 * {@link CumulativeCount} metric) per outcome / per rule tuple, and one
 * gauge per rule tuple for the last-success timestamp. Metrics are
 * registered against a {@link PluginMetrics} obtained via the
 * {@link org.apache.kafka.common.metrics.Monitorable} hook, so they ride
 * the Kafka client's KIP-714 telemetry pipeline and are removed when the
 * host plugin (the serializer/deserializer) closes.
 *
 * <p>Tag set per metric: {@code rule_type}, {@code rule_mode}
 * (invocation mode), {@code subject}, {@code rule_name}, plus
 * {@code action} on action metrics. {@code config} (key/value
 * serializer) and {@code class} (simple class name) are added
 * automatically by {@code PluginMetricsImpl}.
 */
public final class RuleMetrics {

  private static final String EXECUTIONS_NAME = "rule.executions.total";
  private static final String SUCCESS_NAME = "rule.success.total";
  private static final String FAILURE_NAME = "rule.failure.total";
  private static final String SKIPPED_NAME = "rule.skipped.total";
  private static final String ACTIONS_NAME = "rule.actions.total";
  private static final String LAST_SUCCESS_NAME = "rule.last.success.timestamp.ms";

  private final PluginMetrics pluginMetrics;
  private final ConcurrentMap<RuleKey, Sensor> executions = new ConcurrentHashMap<>();
  private final ConcurrentMap<RuleKey, Sensor> success = new ConcurrentHashMap<>();
  private final ConcurrentMap<RuleKey, Sensor> failure = new ConcurrentHashMap<>();
  private final ConcurrentMap<RuleKey, Sensor> skipped = new ConcurrentHashMap<>();
  private final ConcurrentMap<ActionKey, Sensor> actions = new ConcurrentHashMap<>();
  private final ConcurrentMap<RuleKey, AtomicLong> lastSuccess = new ConcurrentHashMap<>();

  public RuleMetrics(PluginMetrics pluginMetrics) {
    this.pluginMetrics = pluginMetrics;
  }

  public void recordExecution(Rule rule, RuleMode mode, String subject) {
    counter(executions, EXECUTIONS_NAME,
        "Total rule dispatches that reached the action stage. "
            + "Equals rule.success.total + rule.failure.total; excludes "
            + "rule.skipped.total.",
        rule, mode, subject).record();
  }

  public void recordSuccess(Rule rule, RuleMode mode, String subject) {
    counter(success, SUCCESS_NAME,
        "Total rule executions where the configured executor's transform "
            + "returned a non-null result (or, for CONDITION rules, returned true).",
        rule, mode, subject).record();
    lastSuccessGauge(rule, mode, subject).set(System.currentTimeMillis());
  }

  public void recordFailure(Rule rule, RuleMode mode, String subject) {
    counter(failure, FAILURE_NAME,
        "Total rule executions that failed. Includes: transform threw a "
            + "RuleException, transform returned null (TRANSFORM kind), "
            + "CONDITION returned false, and no executor registered for "
            + "the rule type.",
        rule, mode, subject).record();
  }

  public void recordSkipped(Rule rule, RuleMode mode, String subject) {
    counter(skipped, SKIPPED_NAME,
        "Total rule invocations skipped by the dispatch loop before "
            + "reaching the action stage (mode mismatch, disabled, or "
            + "filtered). Not counted in rule.executions.total.",
        rule, mode, subject).record();
  }

  public void recordAction(Rule rule, RuleMode mode, String subject, String action) {
    ActionKey key = new ActionKey(ruleKey(rule, mode, subject), action);
    actionSensor(key).record();
  }

  private Sensor counter(ConcurrentMap<RuleKey, Sensor> map, String name,
                         String description,
                         Rule rule, RuleMode mode, String subject) {
    RuleKey key = ruleKey(rule, mode, subject);
    Sensor sensor = map.get(key);
    if (sensor != null) {
      return sensor;
    }
    return map.computeIfAbsent(key, k -> {
      String sn = sensorName(name, k);
      Sensor s = pluginMetrics.addSensor(sn);
      // The addSensor succeeded but s.add can still throw (e.g., on a
      // duplicate MetricName in the underlying Metrics). Roll the sensor
      // back so a retry doesn't hit "Sensor already exists" on a sensor
      // that was never wired up.
      try {
        s.add(pluginMetrics.metricName(name, description, baseTags(k)),
            new CumulativeCount());
      } catch (RuntimeException e) {
        pluginMetrics.removeSensor(sn);
        throw e;
      }
      return s;
    });
  }

  private Sensor actionSensor(ActionKey key) {
    Sensor sensor = actions.get(key);
    if (sensor != null) {
      return sensor;
    }
    return actions.computeIfAbsent(key, k -> {
      String sn = actionSensorName(k);
      Sensor s = pluginMetrics.addSensor(sn);
      try {
        LinkedHashMap<String, String> tags = baseTags(k.rule);
        tags.put("action", k.action);
        s.add(pluginMetrics.metricName(ACTIONS_NAME,
                "Total rule action invocations; only incremented when an action "
                    + "is actually dispatched.", tags),
            new CumulativeCount());
      } catch (RuntimeException e) {
        pluginMetrics.removeSensor(sn);
        throw e;
      }
      return s;
    });
  }

  private AtomicLong lastSuccessGauge(Rule rule, RuleMode mode, String subject) {
    RuleKey key = ruleKey(rule, mode, subject);
    AtomicLong al = lastSuccess.get(key);
    if (al != null) {
      return al;
    }
    return lastSuccess.computeIfAbsent(key, k -> {
      AtomicLong newAl = new AtomicLong();
      pluginMetrics.addMetric(
          pluginMetrics.metricName(LAST_SUCCESS_NAME,
              "Wall-clock time (epoch millis) of the most recent successful "
                  + "rule invocation.",
              baseTags(k)),
          (Gauge<Long>) (cfg, now) -> newAl.get());
      return newAl;
    });
  }

  private static RuleKey ruleKey(Rule rule, RuleMode mode, String subject) {
    return new RuleKey(
        rule.getType() != null ? rule.getType() : "",
        mode != null ? mode.name() : "",
        subject != null ? subject : "",
        rule.getName() != null ? rule.getName() : "");
  }

  private static LinkedHashMap<String, String> baseTags(RuleKey k) {
    LinkedHashMap<String, String> tags = new LinkedHashMap<>();
    tags.put("rule_type", k.type);
    tags.put("rule_mode", k.mode);
    tags.put("subject", k.subject);
    tags.put("rule_name", k.name);
    return tags;
  }

  // Sensor names share a flat namespace in the underlying Metrics
  // instance across all plugins (key.serializer + value.serializer share
  // the producer's Metrics). Subject differs between key and value
  // subjects under TopicNameStrategy, so including it disambiguates.
  private static String sensorName(String metric, RuleKey k) {
    return metric + ":" + k.subject + ":" + k.name + ":" + k.type + ":" + k.mode;
  }

  private static String actionSensorName(ActionKey k) {
    return ACTIONS_NAME + ":" + k.rule.subject + ":" + k.rule.name + ":"
        + k.rule.type + ":" + k.rule.mode + ":" + k.action;
  }

  static final class RuleKey {
    final String type;
    final String mode;
    final String subject;
    final String name;

    RuleKey(String type, String mode, String subject, String name) {
      this.type = type;
      this.mode = mode;
      this.subject = subject;
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RuleKey)) {
        return false;
      }
      RuleKey that = (RuleKey) o;
      return type.equals(that.type)
          && mode.equals(that.mode)
          && subject.equals(that.subject)
          && name.equals(that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, mode, subject, name);
    }
  }

  static final class ActionKey {
    final RuleKey rule;
    final String action;

    ActionKey(RuleKey rule, String action) {
      this.rule = rule;
      this.action = action;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ActionKey)) {
        return false;
      }
      ActionKey that = (ActionKey) o;
      return rule.equals(that.rule) && action.equals(that.action);
    }

    @Override
    public int hashCode() {
      return Objects.hash(rule, action);
    }
  }
}
