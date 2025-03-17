/*
 * Copyright 2025 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldRedactionExecutor extends FieldRuleExecutor {

  private static final Logger log = LoggerFactory.getLogger(FieldRedactionExecutor.class);

  public static final String TYPE = "REDACT";

  private static final String REDACTED_STRING = "<REDACTED>";
  private static final byte[] REDACTED_BYTES = REDACTED_STRING.getBytes(StandardCharsets.UTF_8);

  public FieldRedactionExecutor() {
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public FieldRedactionExecutorTransform newTransform(RuleContext ctx) throws RuleException {
    FieldRedactionExecutorTransform transform = new FieldRedactionExecutorTransform();
    transform.init(ctx);
    return transform;
  }

  public static class FieldRedactionExecutorTransform implements FieldTransform {

    @Override
    public Object transform(RuleContext ctx, FieldContext fieldCtx, Object fieldValue)
        throws RuleException {
      if (fieldValue == null) {
        return null;
      }
      switch (fieldCtx.getType()) {
        case STRING:
          return REDACTED_STRING;
        case BYTES:
          return REDACTED_BYTES;
        default:
          return fieldValue;
      }
    }

    @Override
    public void close() {
    }
  }
}

