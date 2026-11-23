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

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ConstraintsTest {

  private static LogicalType struct(String body) {
    return TypeScripts.parse(TypeScripts.createScript("t", BodyKind.STRUCT, body, Map.of(),
        Map.of()));
  }

  @Test
  void aColumnIsTheFirstNameOfAReference() {
    assertThat(Constraints.columns("amount > 0 AND note IS NOT NULL"))
        .containsExactly("amount", "note");
    assertThat(Constraints.columns("addr.city = 'x' OR `my col` > 1"))
        .containsExactly("addr", "my col");
  }

  @Test
  void anIterationVariableIsNotAColumn() {
    String sql = "EVERY(tags, t, LENGTH(t) > 0) AND ANY(tags, x, x = t)";
    // The second t is outside the macro that binds it, so it is the column.
    assertThat(Constraints.columns(sql)).containsExactly("tags", "t");
    assertThat(Constraints.variables(sql)).containsExactly("t", "x");
  }

  @Test
  void aRenameFollowsTheColumnAndLeavesStringsAndVariablesAlone() {
    assertThat(Constraints.rename("b > 1 AND `b` < 9 AND s <> 'b'", "b", "c"))
        .isEqualTo("c > 1 AND c < 9 AND s <> 'b'");
    assertThat(Constraints.rename("EVERY(tags, e, e <> x)", "x", "y"))
        .isEqualTo("EVERY(tags, e, e <> y)");
    assertThat(Constraints.rename("EVERY(tags, x, x <> '')", "x", "y"))
        .isEqualTo("EVERY(tags, x, x <> '')");
    // Token positions count code points, and a string's chars two for an emoji.
    assertThat(Constraints.rename("s <> '😀' AND b > 1", "b", "c"))
        .isEqualTo("s <> '😀' AND c > 1");
    // A name that doesn't read back as itself is quoted.
    assertThat(Constraints.rename("b > 1", "b", "my col")).isEqualTo("`my col` > 1");
  }

  @Test
  void textThatIsNoCheckIsRefused() {
    assertThatThrownBy(() -> Constraints.columns("a >"))
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith("Invalid CHECK (a >): ");
    assertThatThrownBy(() -> Constraints.columns("a > 0) OR (b"))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Invalid CHECK (a > 0) OR (b): unexpected ')'");
  }

  @Test
  void constraintsWrittenWithoutANameAreNamedAsPostgresNamesThem() {
    LogicalType named = Constraints.named(struct("(a INT CHECK (a > 0) CHECK (a <> 5), b INT "
        + "CONSTRAINT b_small CHECK (b < 9), CHECK (a < b))"), ":.prod:com.acme.t");

    Schema t = named.getRootSchema();
    assertThat(t.getRules()).extracting(Rule::getName).containsExactly("com_acme_t_check");
    assertThat(t.getFields().get(0).getRules()).extracting(Rule::getName)
        .containsExactly("com_acme_t_a_check", "com_acme_t_a_check1");
    assertThat(t.getFields().get(1).getRules()).extracting(Rule::getName)
        .containsExactly("b_small");
    assertThat(Constraints.names(t)).containsExactly("com_acme_t_check", "com_acme_t_a_check",
        "com_acme_t_a_check1", "b_small");
  }

  @Test
  void aNameMadeIsNoneGivenOrMadeBefore() {
    Set<String> used = new HashSet<>(Set.of("t_check", "t_check1"));
    assertThat(Constraints.newName("t", used)).isEqualTo("t_check2");
    assertThat(used).contains("t_check2");

    LogicalType named = Constraints.named(struct("(a INT, CHECK (a > 0))"), "t", used);
    assertThat(named.getRootSchema().getRules()).extracting(Rule::getName)
        .containsExactly("t_check3");
  }

  @Test
  void aNameGivenTwiceInAnyCaseIsRefused() {
    assertThatThrownBy(() -> Constraints.named(struct("(a INT CONSTRAINT c CHECK (a > 0), "
        + "b INT, CONSTRAINT C CHECK (b > 0))"), "t"))
        // The type's own constraints are read before its attributes'.
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Constraint c is named more than once");
  }
}
