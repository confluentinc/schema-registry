/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import io.confluent.protobuf.type.Decimal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DecimalUtils#toBigDecimal(Object)} runtime dispatch
 * and the byte-array / scale constructors. The CEL-side bindings reuse this
 * dispatch so anything that works here works inside a rule.
 */
public class DecimalUtilsTest {

  @Test
  void dispatchIdentity_passesBigDecimalThrough() {
    BigDecimal expected = new BigDecimal("123.456");
    assertEquals(expected, DecimalUtils.toBigDecimal(expected));
  }

  @Test
  void dispatchString_parsesLiteral() {
    assertEquals(new BigDecimal("100.50"), DecimalUtils.toBigDecimal("100.50"));
  }

  @Test
  void dispatchLong_widensExactly() {
    assertEquals(new BigDecimal("42"), DecimalUtils.toBigDecimal(42L));
  }

  @Test
  void dispatchDouble_widensExactly() {
    assertEquals(BigDecimal.valueOf(1.5d), DecimalUtils.toBigDecimal(1.5d));
  }

  @Test
  void dispatchProtoDecimal_decodes() {
    Decimal d = Decimal.newBuilder()
        .setValue(ByteString.copyFrom(new BigInteger("12345").toByteArray()))
        .setScale(2)
        .build();
    assertEquals(new BigDecimal("123.45"), DecimalUtils.toBigDecimal(d));
  }

  @Test
  void dispatchRawBytes_throwsWithHint() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> DecimalUtils.toBigDecimal(new byte[] {1, 2, 3}));
    assertEquals(true, e.getMessage().contains("decimal(bytes, scale)"));
  }

  @Test
  void dispatchByteBuffer_throwsWithHint() {
    assertThrows(IllegalArgumentException.class,
        () -> DecimalUtils.toBigDecimal(ByteBuffer.wrap(new byte[] {1, 2, 3})));
  }

  @Test
  void dispatchNull_throws() {
    assertThrows(IllegalArgumentException.class,
        () -> DecimalUtils.toBigDecimal((Object) null));
  }

  @Test
  void dispatchUnsupportedType_throws() {
    assertThrows(IllegalArgumentException.class,
        () -> DecimalUtils.toBigDecimal(new Object()));
  }

  @Test
  void bytesAndScale_reconstructsValue() {
    byte[] unscaled = new BigInteger("100050").toByteArray();
    assertEquals(new BigDecimal("100.050"), DecimalUtils.toBigDecimal(unscaled, 3));
  }

  @Test
  void bytesAndScale_emptyBytesYieldsZero() {
    assertEquals(BigDecimal.ZERO.setScale(2),
        DecimalUtils.toBigDecimal(new byte[0], 2));
  }

  @Test
  void bytesAndScale_negativeUnscaled() {
    byte[] unscaled = new BigInteger("-12345").toByteArray();
    assertEquals(new BigDecimal("-123.45"),
        DecimalUtils.toBigDecimal(unscaled, 2));
  }
}
