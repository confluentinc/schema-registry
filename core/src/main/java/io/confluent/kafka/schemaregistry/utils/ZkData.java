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

package io.confluent.kafka.schemaregistry.utils;

import org.apache.zookeeper.data.Stat;

public class ZkData {

  private final String data;
  private final Stat stat;

  public ZkData(String data, Stat stat) {
    this.data = data;
    this.stat = stat;
  }

  public String getData() {
    return this.data;
  }

  public Stat getStat() {
    return this.stat;
  }
}
