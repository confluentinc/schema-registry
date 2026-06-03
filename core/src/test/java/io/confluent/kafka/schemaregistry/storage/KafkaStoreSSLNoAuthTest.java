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

import org.junit.jupiter.api.Test;

public class KafkaStoreSSLNoAuthTest extends KafkaStoreSSLAuthTest {
  protected boolean requireSSLClientAuth() {
    return false;
  }

  // ignore this test because it doesn't apply when SSL client auth is off.
  @Test
  @Override
  public void testInitializationWithoutClientAuth() {}
}
