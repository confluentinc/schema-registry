/**
 * Copyright 2015 Confluent Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafka.serializers;

import kafka.utils.VerifiableProperties;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.junit.Assert.assertArrayEquals;

public class KafkaJsonEncoderTest {

  @Test
  public void toBytes() {
    KafkaJsonEncoder<Object> encoder = new KafkaJsonEncoder<Object>(new VerifiableProperties());
    assertArrayEquals(asBytes("45.9"), encoder.toBytes(45.9));
    assertArrayEquals(asBytes("\"hello\""), encoder.toBytes("hello"));
  }

  private static byte[] asBytes(String json) {
    return json.getBytes(Charset.forName("UTF-8"));
  }

}
