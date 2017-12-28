/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafka.schemaregistry.storage;

import org.codehaus.jackson.annotate.JsonPropertyOrder;

/**
 *
 */
@JsonPropertyOrder(value = {"keytype", "magic"})
public class NoopKey extends SchemaRegistryKey {

  private static final int MAGIC_BYTE = 0;

  public NoopKey() {
    super(SchemaRegistryKeyType.NOOP);
    this.magicByte = MAGIC_BYTE;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{magic=" + this.magicByte + ",");
    sb.append("keytype=" + this.keyType.keyType + "}");
    return sb.toString();
  }
}
