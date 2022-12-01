/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.utils.WildcardMatcher;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.header.Headers;

/**
 * A rule context.
 */
public class RuleContext {

  private final ParsedSchema source;
  private final ParsedSchema target;
  private final String subject;
  private final String topic;
  private final Headers headers;
  private final Object originalMessage;
  private final boolean isKey;
  private final RuleMode ruleMode;
  private final Rule rule;
  private final Map<String, Object> customData = new ConcurrentHashMap<>();
  private final Deque<FieldContext> fieldContexts;

  public RuleContext(
      ParsedSchema target,
      String subject,
      String topic,
      Headers headers,
      Object originalMessage,
      boolean isKey,
      RuleMode ruleMode,
      Rule rule) {
    this(null, target, subject, topic, headers, originalMessage, isKey, ruleMode, rule);
  }

  public RuleContext(
      ParsedSchema source,
      ParsedSchema target,
      String subject,
      String topic,
      Headers headers,
      Object originalMessage,
      boolean isKey,
      RuleMode ruleMode,
      Rule rule) {
    this.source = source;
    this.target = target;
    this.subject = subject;
    this.topic = topic;
    this.headers = headers;
    this.originalMessage = originalMessage;
    this.isKey = isKey;
    this.ruleMode = ruleMode;
    this.rule = rule;
    this.fieldContexts = new ArrayDeque<>();
  }

  public ParsedSchema source() {
    return source;
  }

  public ParsedSchema target() {
    return target;
  }

  public String subject() {
    return subject;
  }

  public String topic() {
    return topic;
  }

  public Headers headers() {
    return headers;
  }

  public Object originalMessage() {
    return originalMessage;
  }

  public boolean isKey() {
    return isKey;
  }

  public RuleMode ruleMode() {
    return ruleMode;
  }

  public Rule rule() {
    return rule;
  }

  public Map<String, Object> customData() {
    return customData;
  }

  public Set<String> getAnnotations(String fullName) {
    Set<String> annotations = new HashSet<>();
    Metadata metadata = target.metadata();
    if (metadata != null && metadata.getAnnotations() != null) {
      for (Map.Entry<String, SortedSet<String>> entry : metadata.getAnnotations().entrySet()) {
        if (WildcardMatcher.match(fullName, entry.getKey())) {
          annotations.addAll(entry.getValue());
        }
      }
    }
    return annotations;
  }

  public FieldContext currentField() {
    return fieldContexts.peekLast();
  }

  public FieldContext enterField(RuleContext ctx, Object containingMessage,
      String fullName, String name, RuleContext.Type type, Set<String> annotations) {
    Set<String> allAnnotations = new HashSet<>(annotations);
    allAnnotations.addAll(ctx.getAnnotations(fullName));
    return new FieldContext(containingMessage, fullName, name, type, allAnnotations);
  }

  public class FieldContext implements AutoCloseable {
    private final Object containingMessage;
    private final String fullName;
    private final String name;
    private final Type type;
    private final Set<String> annotations;

    public FieldContext(Object containingMessage, String fullName,
        String name, Type type, Set<String> annotations) {
      this.containingMessage = containingMessage;
      this.fullName = fullName;
      this.name = name;
      this.type = type;
      this.annotations = annotations;
      fieldContexts.addLast(this);
    }

    public Object getContainingMessage() {
      return containingMessage;
    }

    public String getFullName() {
      return fullName;
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }

    public Set<String> getAnnotations() {
      return annotations;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FieldContext that = (FieldContext) o;
      return Objects.equals(containingMessage, that.containingMessage)
          && Objects.equals(fullName, that.fullName)
          && Objects.equals(name, that.name)
          && Objects.equals(type, that.type)
          && Objects.equals(annotations, that.annotations);
    }

    @Override
    public int hashCode() {
      return Objects.hash(containingMessage, fullName, name, type, annotations);
    }

    @Override
    public void close() {
      fieldContexts.removeLast();
    }
  }

  public enum Type {
    RECORD,
    ENUM,
    ARRAY,
    MAP,
    COMBINED,
    FIXED,
    STRING,
    BYTES,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    NULL
  }
}
