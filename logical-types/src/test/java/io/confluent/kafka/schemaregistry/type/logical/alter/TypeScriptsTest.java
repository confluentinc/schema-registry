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

package io.confluent.kafka.schemaregistry.type.logical.alter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.alter.TypeScripts.BodyKind;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TypeScriptsTest {

  @Test
  void aStructOrEnumIsDeclaredUnderItsNameWithItsClausesAndProperties() {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("avro.name", "it's");
    assertThat(TypeScripts.createScript("order", BodyKind.STRUCT, "(id BIGINT)",
        "COMMENT 'an order'", params, Map.of()))
        .isEqualTo("STRUCT `order` (id BIGINT) COMMENT 'an order' "
            + "WITH ('avro.name' = 'it''s')");
    assertThat(TypeScripts.createScript("co`lor", BodyKind.ENUM, "('RED', 'GREEN')", Map.of(),
        Map.of())).isEqualTo("ENUM `co``lor` ('RED', 'GREEN')");
  }

  @Test
  void anyOtherTypeIsTheScriptsRootAndTakesNoProperties() {
    assertThat(TypeScripts.createScript("id", BodyKind.OTHER, "BIGINT NOT NULL", Map.of(),
        Map.of())).isEqualTo("TYPE BIGINT NOT NULL");
    assertThatThrownBy(() -> TypeScripts.createScript("id", BodyKind.OTHER, "BIGINT",
        Map.of("k", "v"), Map.of()))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("WITH properties [k] are only allowed on a struct or enum type");
  }

  @Test
  void aliasesNameTheTypesAScriptRefersToByTheirFullNames() {
    assertThat(TypeScripts.typeScript("ARRAY<address>", Map.of("address", "com.acme.Address")))
        .isEqualTo("USING TYPE `address` FOR `com`.`acme`.`Address`;\nTYPE ARRAY<address>");
  }

  @Test
  void aScriptParsesToItsLogicalType() {
    // One struct is the script's root.
    LogicalType struct = TypeScripts.parse(TypeScripts.createScript("order", BodyKind.STRUCT,
        "(id BIGINT NOT NULL, note STRING)", Map.of(), Map.of()));
    assertThat(struct.getRootSchema().getFields()).extracting(Schema.Field::getName)
        .containsExactly("id", "note");

    Schema.Field added = TypeScripts.parse(TypeScripts.fieldScript("when", "TIMESTAMP(3)",
        Map.of())).getRootSchema().getFields().get(0);
    assertThat(added.getName()).isEqualTo("when");
    assertThat(added.getSchema().getType()).isEqualTo(Schema.Type.TIMESTAMP);

    assertThat(TypeScripts.parse(TypeScripts.typeScript("MAP<STRING, INT>", Map.of()))
        .getRootSchema().getType()).isEqualTo(Schema.Type.MAP);
    // A name the script doesn't declare is another type's, left for its references.
    assertThat(TypeScripts.parse(TypeScripts.typeScript("geo.point", Map.of()))
        .getExternalTypes()).containsExactly("geo.point");
  }

  @Test
  void textThatIsNoTypeIsRefused() {
    assertThatThrownBy(() -> TypeScripts.parse(TypeScripts.createScript("order",
        BodyKind.STRUCT, "(id NOSUCHTYPE(", Map.of(), Map.of())))
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith("Invalid type: ");
  }

  @Test
  void aCheckClauseHasItsNameAndMessageIfGiven() {
    assertThat(TypeScripts.checkClause(null, "amount > 0", null))
        .isEqualTo("CHECK (amount > 0)");
    assertThat(TypeScripts.checkClause("positive", "amount > 0", "it's not"))
        .isEqualTo("CONSTRAINT `positive` CHECK (amount > 0) MESSAGE 'it''s not'");
  }
}
