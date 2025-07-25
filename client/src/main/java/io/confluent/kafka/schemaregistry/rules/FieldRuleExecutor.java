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

import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A field-level rule executor.
 */
public abstract class FieldRuleExecutor implements RuleExecutor {

  Logger log = LoggerFactory.getLogger(FieldRuleExecutor.class);

  public static final String PRESERVE_SOURCE_FIELDS = "preserve.source.fields";

  private Boolean preserveSource;

  @Override
  public void configure(Map<String, ?> configs) {
    Object preserveSourceConfig = configs.get(PRESERVE_SOURCE_FIELDS);
    if (preserveSourceConfig != null) {
      this.preserveSource = Boolean.parseBoolean(preserveSourceConfig.toString());
    }
  }

  public boolean isPreserveSource() {
    return Boolean.TRUE.equals(preserveSource);
  }

  public abstract FieldTransform newTransform(RuleContext ctx) throws RuleException;

  @Override
  public Object transform(RuleContext ctx, Object message) throws RuleException {
    if (this.preserveSource == null) {
      String preserveValueConfig = ctx.getParameter(PRESERVE_SOURCE_FIELDS);
      if (preserveValueConfig != null) {
        this.preserveSource = Boolean.parseBoolean(preserveValueConfig);
      }
    }
    switch (ctx.ruleMode()) {
      case WRITE:
      case UPGRADE:
        for (int i = 0; i < ctx.index(); i++) {
          Rule otherRule = ctx.rules().get(i);
          if (areTransformsWithSameTags(ctx.rule(), otherRule)) {
            // ignore this transform if an earlier one has the same tags
            log.debug("Ignoring rule '{}' during {} as rule '{}' has the same tag(s)",
                ctx.rule().getName(), ctx.ruleMode(), otherRule.getName());
            return message;
          }
        }
        break;
      case READ:
      case DOWNGRADE:
        for (int i = ctx.index() + 1; i < ctx.rules().size(); i++) {
          Rule otherRule = ctx.rules().get(i);
          if (areTransformsWithSameTags(ctx.rule(), otherRule)) {
            // ignore this transform if a later one has the same tags
            log.debug("Ignoring rule '{}' during {} as rule '{}' has the same tag(s)",
                ctx.rule().getName(), ctx.ruleMode(), otherRule.getName());
            return message;
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported rule mode " + ctx.ruleMode());
    }

    try (FieldTransform transform = newTransform(ctx)) {
      if (transform != null) {
        if (ctx.ruleMode() == RuleMode.WRITE
            && ctx.rule().getKind() == RuleKind.TRANSFORM
            && isPreserveSource()) {
          try {
            // We use the target schema
            message = ctx.target().copyMessage(message);
          } catch (IOException e) {
            throw new RuleException("Could not copy source message", e);
          }
        }
        return ctx.target().transformMessage(ctx, transform, message);
      } else {
        return message;
      }
    }
  }

  static boolean areTransformsWithSameTags(Rule rule1, Rule rule2) {
    return rule1.getTags().size() > 0
        && rule1.getKind() == RuleKind.TRANSFORM
        && rule1.getKind() == rule2.getKind()
        && rule1.getMode() == rule2.getMode()
        && Objects.equals(rule1.getType(), rule2.getType())
        && Objects.equals(rule1.getTags(), rule2.getTags());
  }
}
