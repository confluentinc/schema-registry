/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.maven.plugins.annotations.Parameter;

public class Rule {

  // For mojo, cannot have any constructors besides default constructor

  @Parameter(required = true)
  protected String name;

  @Parameter(required = false)
  protected String doc;

  @Parameter(required = false)
  protected RuleKind kind;

  @Parameter(required = false)
  protected RuleMode mode;

  @Parameter(required = true)
  protected String type;

  @Parameter(required = false)
  protected Set<String> tags = new HashSet<>();

  @Parameter(required = false)
  protected Map<String, String> params = new HashMap<>();

  @Parameter(required = false)
  protected String expr;

  @Parameter(required = false)
  protected String onSuccess;

  @Parameter(required = false)
  protected String onFailure;

  @Parameter(required = false)
  protected boolean disabled;

  @Override
  public String toString() {
    return "Rule{"
        + "name='" + name + '\''
        + ", doc=" + doc
        + ", kind=" + kind
        + ", mode=" + mode
        + ", type='" + type + '\''
        + ", tags=" + tags
        + ", params=" + params
        + ", expr='" + expr + '\''
        + ", onSuccess='" + onSuccess + '\''
        + ", onFailure='" + onFailure + '\''
        + ", disabled=" + disabled
        + '}';
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.Rule toRuleEntity() {
    return new io.confluent.kafka.schemaregistry.client.rest.entities.Rule(
        name, doc, kind, mode, type, tags, params, expr, onSuccess, onFailure, disabled
    );
  }
}
