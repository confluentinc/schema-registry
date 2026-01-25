/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.serializers;

import kafka.utils.VerifiableProperties;
import org.junit.Test;

import java.lang.reflect.Constructor;

import static org.junit.Assert.assertEquals;

public class KafkaJsonDecoderTest {

  @Test
  public void constructWithReflection() throws Exception {
    VerifiableProperties props = new VerifiableProperties();
    Constructor<KafkaJsonDecoder> constructor = KafkaJsonDecoder.class.getConstructor(VerifiableProperties.class);
    KafkaJsonDecoder<?> decoder = constructor.newInstance(props);
    assertEquals(Object.class, decoder.getType());
    assertEquals("hello", decoder.fromBytes("\"hello\"".getBytes()));
  }

  @Test
  public void genericConstruct() throws Exception {
    VerifiableProperties props = new VerifiableProperties();
    KafkaJsonDecoder<String> decoder = new KafkaJsonDecoder<String>(props, String.class);
    assertEquals(String.class, decoder.getType());
    assertEquals("hello", decoder.fromBytes("\"hello\"".getBytes()));
  }

}
