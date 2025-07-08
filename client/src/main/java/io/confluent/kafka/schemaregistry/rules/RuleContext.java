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
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
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

  private final Map<String, ?> configs;
  private final ParsedSchema source;
  private final ParsedSchema target;
  private final String subject;
  private final String topic;
  private final Headers headers;
  private final Object originalKey;
  private final Object originalValue;
  private final boolean isKey;
  private final RuleMode ruleMode;
  private final Rule rule;
  private final int index;
  private final List<Rule> rules;
  private final Map<Object, Object> customData = new ConcurrentHashMap<>();
  private final Deque<FieldContext> fieldContexts;

  public RuleContext(
      Map<String, ?> configs,
      ParsedSchema source,
      ParsedSchema target,
      String subject,
      String topic,
      Headers headers,
      Object originalKey,
      Object originalValue,
      boolean isKey,
      RuleMode ruleMode,
      Rule rule,
      int index,
      List<Rule> rules) {
    this.configs = configs;
    this.source = source;
    this.target = target;
    this.subject = subject;
    this.topic = topic;
    this.headers = headers;
    this.originalKey = originalKey;
    this.originalValue = originalValue;
    this.isKey = isKey;
    this.ruleMode = ruleMode;
    this.rule = rule;
    this.index = index;
    this.rules = rules;
    this.fieldContexts = new ArrayDeque<>();
  }

  public Map<String, ?> configs() {
    return configs;
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

  public Object originalKey() {
    return originalKey;
  }

  public Object originalValue() {
    return originalValue;
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

  public int index() {
    return index;
  }

  public List<Rule> rules() {
    return rules;
  }

  public Map<Object, Object> customData() {
    return customData;
  }

  public Set<String> getTags(String fullName) {
    Metadata metadata = target.metadata();
    if (metadata != null && metadata.getTags() != null) {
      Set<String> tags = new HashSet<>();
      for (Map.Entry<String, SortedSet<String>> entry : metadata.getTags().entrySet()) {
        if (WildcardMatcher.match(fullName, entry.getKey())) {
          tags.addAll(entry.getValue());
        }
      }
      return tags;
    }
    return Collections.emptySet();
  }

  public String getParameter(String name) {
    String value = null;
    Map<String, String> params = rule.getParams();
    if (params != null) {
      value = params.get(name);
    }
    if (value == null) {
      Metadata metadata = target.metadata();
      if (metadata != null) {
        // If property not found in rule parameters, look in metadata properties
        Map<String, String> properties = metadata.getProperties();
        if (properties != null) {
          value = properties.get(name);
        }
      }
    }
    return value;
  }

  public FieldContext currentField() {
    return fieldContexts.peekLast();
  }

  public FieldContext enterField(Object containingMessage,
      String fullName, String name, RuleContext.Type type, Set<String> tags) {
    Set<String> metadataTags = getTags(fullName);
    if (!metadataTags.isEmpty()) {
      tags = new HashSet<>(tags);
      tags.addAll(metadataTags);
    }
    Set<String> ruleTags = rule().getTags();
    if (!type.isPrimitive() || ruleTags.isEmpty() || !disjoint(tags, ruleTags)) {
      return new FieldContext(containingMessage, fullName, name, type, tags);
    } else {
      return null;
    }
  }

  // Slightly more efficient than Collections.disjoint for our use case
  public static boolean disjoint(Set<String> set1, Set<String> set2) {
    if (set1.isEmpty() || set2.isEmpty()) {
      return true;
    }
    for (String e : set1) {
      if (set2.contains(e)) {
        return false;
      }
    }
    return true;
  }

  public class FieldContext implements AutoCloseable {
    private final Object containingMessage;
    private final String fullName;
    private final String name;
    private Type type;
    private final Set<String> tags;

    public FieldContext(Object containingMessage, String fullName,
        String name, Type type, Set<String> tags) {
      this.containingMessage = containingMessage;
      this.fullName = fullName;
      this.name = name;
      this.type = type;
      this.tags = tags;
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

    public void setType(Type type) {
      this.type = type;
    }

    public Set<String> getTags() {
      return tags;
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
          && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(containingMessage, fullName, name, type, tags);
    }

    @Override
    public void close() {
      fieldContexts.removeLast();
    }
  }

  public enum Type {
    RECORD(false),
    ENUM(false),
    ARRAY(false),
    MAP(false),
    COMBINED(false),
    FIXED(false),
    STRING(true),
    BYTES(true),
    INT(true),
    LONG(true),
    FLOAT(true),
    DOUBLE(true),
    BOOLEAN(true),
    NULL(true);

    private boolean isPrimitive;

    Type(boolean isPrimitive) {
      this.isPrimitive = isPrimitive;
    }

    public boolean isPrimitive() {
      return isPrimitive;
    }
  }
}
