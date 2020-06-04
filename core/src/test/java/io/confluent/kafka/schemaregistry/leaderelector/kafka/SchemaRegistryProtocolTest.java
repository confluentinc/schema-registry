/**
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.leaderelector.kafka;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertNotNull;

public class SchemaRegistryProtocolTest {

  @Test
  public void testDeserializeWithUnknownProperties() {
    String json = "{\"error\":0," +
            "\"master\":\"master\"," +
            "\"master_identity\":null," +
            "\"unknown_property\":null," +
            "\"version\":1}";
    ByteBuffer byteBuffer = ByteBuffer.wrap(json.getBytes());
    SchemaRegistryProtocol.Assignment assignment =
            SchemaRegistryProtocol.Assignment.fromJson(byteBuffer);
    assertNotNull(assignment);
  }
}
