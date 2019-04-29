/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ModeValue implements SchemaRegistryValue {

  private Mode mode;

  public ModeValue(@JsonProperty("mode") Mode mode) {
    this.mode = mode;
  }

  public ModeValue() {
    mode = null;
  }

  @JsonProperty("mode")
  public Mode getMode() {
    return mode;
  }

  @JsonProperty("mode")
  public void setMode(Mode mode) {
    this.mode = mode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ModeValue that = (ModeValue) o;

    if (!this.mode.equals(that.mode)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = 31 * mode.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{mode=" + this.mode + "}");
    return sb.toString();
  }
}
