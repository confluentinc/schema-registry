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

import org.apache.kafka.common.errors.SerializationException;

public class ErrorAction implements RuleAction {

  public static final String TYPE = "ERROR";

  public String type() {
    return TYPE;
  }

  public void run(RuleContext ctx, Object message, RuleException ex) throws RuleException {
    String msg = "Rule failed: " + ctx.rule().getName();
    // throw a RuntimeException
    throw ex != null ? new SerializationException(msg, ex) : new SerializationException(msg);
  }
}
