/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.confluent.kafka.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KafkaJsonSerializerTest {

  private ObjectMapper objectMapper = new ObjectMapper();
  private KafkaJsonSerializer<Object> serializer;

  @Before
  public void setup() {
    serializer = new KafkaJsonSerializer<>();
    serializer.configure(Collections.<String, Object>emptyMap(), false);
  }

  @Test
  public void serializeNull() {
    assertNull(serializer.serialize("foo", null));
  }

  @Test
  public void serialize() throws Exception {
    Map<String, Object> message = new HashMap<>();
    message.put("foo", "bar");
    message.put("baz", 354.99);

    byte[] bytes = serializer.serialize("foo", message);

    Object deserialized = this.objectMapper.readValue(bytes, Object.class);
    assertEquals(message, deserialized);
  }

}
