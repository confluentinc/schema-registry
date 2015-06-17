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
package io.confluent.kafka.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KafkaJsonSerializerTest {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void serialize() throws Exception {
    KafkaJsonSerializer serializer = new KafkaJsonSerializer();
    serializer.configure(Collections.<String, Object>emptyMap(), false);

    Map<String, Object> message = new HashMap<String, Object>();
    message.put("foo", "bar");
    message.put("baz", 354.99);

    byte[] bytes = serializer.serialize("foo", message);

    Object deserialized = this.objectMapper.readValue(bytes, Object.class);
    assertEquals(message, deserialized);
  }

}
