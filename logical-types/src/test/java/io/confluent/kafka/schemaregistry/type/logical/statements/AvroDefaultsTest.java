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

package io.confluent.kafka.schemaregistry.type.logical.statements;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AvroDefaultsTest {

  private static Schema type(String expression) {
    return TypeScripts.parse(TypeScripts.typeScript(expression, Map.of())).getRootSchema();
  }

  @Test
  void aDefaultIsReadAsAvroPromotesTheOldTypeToTheNew() {
    assertThat(AvroDefaults.converted("n", type("INT"), type("BIGINT"), 5)).isEqualTo(5L);
    assertThat(AvroDefaults.converted("n", type("INT"), type("DOUBLE"), 5)).isEqualTo(5.0);
    assertThat(AvroDefaults.converted("n", type("INT"), type("INT"), 5)).isEqualTo(5);
    assertThat(AvroDefaults.converted("s", type("STRING"), type("STRING"), "a")).isEqualTo("a");
  }

  @Test
  void aDefaultAvroCouldNotReadUnderTheNewTypeIsRefused() {
    assertThatThrownBy(() -> AvroDefaults.converted("n", type("BIGINT"), type("INT"), 5L))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("The default 5 of attribute n cannot be read as INT, as Avro reads a "
            + "BIGINT written before the change");
    // One that is a string is told as a literal, a quote in it doubled.
    assertThatThrownBy(() -> AvroDefaults.converted("e.n", type("STRING"), type("INT"),
        "it's"))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("The default 'it''s' of attribute e.n cannot be read as INT");
    assertThatThrownBy(() -> AvroDefaults.converted("n", type("INT"), type("BOOLEAN"), 5))
        .isInstanceOf(TypeRefusedException.class);
    // A whole number Avro holds as an int, beyond the new type's range.
    assertThatThrownBy(() -> AvroDefaults.converted("n", type("INT"), type("TINYINT"), 500))
        .isInstanceOf(TypeRefusedException.class);
    // A time of day past midnight; why it was refused is kept.
    assertThatThrownBy(() -> AvroDefaults.converted("t", type("INT"), type("TIME(3)"),
        90_000_000))
        .isInstanceOf(TypeRefusedException.class).hasCauseInstanceOf(RuntimeException.class);
  }

  @Test
  void anEnumsDefaultIsASymbolTheNewEnumHas() {
    Schema colors = type("ENUM('RED', 'GREEN')");
    assertThat(AvroDefaults.converted("c", colors, type("ENUM('GREEN', 'RED', 'BLUE')"),
        "RED")).isEqualTo("RED");
    assertThatThrownBy(() -> AvroDefaults.converted("c", colors, type("ENUM('GREEN')"), "RED"))
        .isInstanceOf(TypeRefusedException.class);
    // A default that is no symbol, as a hand-written schema's may be.
    assertThatThrownBy(() -> AvroDefaults.converted("c", colors, colors, 5))
        .isInstanceOf(TypeRefusedException.class);
  }
}
