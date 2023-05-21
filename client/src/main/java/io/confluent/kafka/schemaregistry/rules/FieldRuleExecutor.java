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
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A field-level rule executor.
 */
public interface FieldRuleExecutor extends RuleExecutor {

  Logger log = LoggerFactory.getLogger(FieldRuleExecutor.class);

  FieldTransform newTransform(RuleContext ctx) throws RuleException;

  default Object transform(RuleContext ctx, Object message) throws RuleException {
    switch (ctx.ruleMode()) {
      case WRITE:
      case UPGRADE:
        for (int i = ctx.index() + 1; i < ctx.rules().size(); i++) {
          Rule otherRule = ctx.rules().get(i);
          if (areTransformsWithSameTags(ctx.rule(), otherRule)) {
            // ignore this transform if a later one has the same tags
            log.debug("Ignoring rule '" + ctx.rule().getName() + "' during " + ctx.ruleMode()
                + "' as rule '" + otherRule.getName() + "' has the same tag(s) and overrides it");
            return message;
          }
        }
        break;
      case READ:
      case DOWNGRADE:
        for (int i = 0; i < ctx.index(); i++) {
          Rule otherRule = ctx.rules().get(i);
          if (areTransformsWithSameTags(ctx.rule(), otherRule)) {
            // ignore this transform if an earlier one has the same tags
            log.debug("Ignoring rule '" + ctx.rule().getName() + "' during " + ctx.ruleMode()
                + "' as rule '" + otherRule.getName() + "' has the same tag(s) and overrides it");
            return message;
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported rule mode " + ctx.ruleMode());
    }

    try (FieldTransform transform = newTransform(ctx)) {
      if (transform != null) {
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
