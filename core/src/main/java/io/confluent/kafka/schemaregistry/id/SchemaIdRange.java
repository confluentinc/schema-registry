/*
 * Copyright 2014-2019 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.id;

/**
 * A range of schema IDs, from the first available ID to the inclusive upper bound.
 */
public class SchemaIdRange {
  private final int base;
  private final int end;

  public SchemaIdRange(int base, int end) {
    this.base = base;
    this.end = end;
  }

  public int base() {
    return base;
  }

  public int end() {
    return end;
  }
}
