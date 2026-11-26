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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class FormatsTest {

  private static LogicalType struct(String body) {
    return TypeScripts.parse(TypeScripts.createScript("t", BodyKind.STRUCT, body, Map.of(),
        Map.of()));
  }

  @Test
  void aFormatIsNamedInAnyCaseAndARegisteredSchemaWithoutOneIsAvro() {
    assertThat(Formats.of(" protobuf ")).isEqualTo(SchemaType.PROTOBUF);
    assertThat(Formats.ofSchemaType("JSON")).isEqualTo(SchemaType.JSON);
    assertThat(Formats.ofSchemaType(null)).isEqualTo(SchemaType.AVRO);
    assertThatThrownBy(() -> Formats.of("xml")).isInstanceOf(TypeRefusedException.class)
        .hasMessage("Unknown format 'xml'; must be one of AVRO, PROTOBUF, JSON");
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeIsRegisteredAsDdlAndConvertsToItsFormat(SchemaType format) {
    LogicalType lt = struct("(id BIGINT NOT NULL, note STRING)");

    RegisterSchemaRequest request = Formats.request(format, lt);
    assertThat(request.getSchemaType()).isEqualTo(format.name());
    assertThat(request.getSchema()).contains("STRUCT t (").contains("id BIGINT NOT NULL");
    assertThat(Formats.toNative(format, lt, "t").schemaType()).isEqualTo(format.name());
  }

  @Test
  void aDefaultIsRefusedOutsideAvro() {
    LogicalType lt = struct("(n INT DEFAULT 5)");
    Formats.refuseUnwritable(SchemaType.AVRO, lt);
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF, lt))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Attribute n has a DEFAULT, which a PROTOBUF type cannot apply: a record "
            + "without it reads the zero value");
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON, lt))
        .isInstanceOf(TypeRefusedException.class).hasMessageEndingWith("reads NULL");
  }

  @Test
  void aNotNullUnionIsRefusedInProtobufAtAnyDepth() {
    LogicalType top = struct("(u UNION(a INT, b STRING) NOT NULL)");
    LogicalType nested = struct("(v ARRAY<UNION(a INT, b STRING) NOT NULL>)");
    Formats.refuseUnwritable(SchemaType.AVRO, top);
    Formats.refuseUnwritable(SchemaType.PROTOBUF, struct("(u UNION(a INT, b STRING))"));
    for (LogicalType lt : new LogicalType[] {top, nested}) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF, lt))
          .isInstanceOf(TypeRefusedException.class).hasMessageContaining("always nullable");
    }
  }

  // A VARIANT is written as any JSON value, so a value of any branch beside it matches both.
  @Test
  void aJsonUnionHasNoBranchBesideAVariant() {
    for (String union : new String[] {"UNION(v VARIANT, s STRUCT<x INT>)",
        "UNION(a ARRAY<INT>, v VARIANT)", "UNION(v VARIANT, n INT)"}) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
          struct("(u " + union + ")")))
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("which are written as one JSON type");
      Formats.refuseUnwritable(SchemaType.AVRO, struct("(u " + union + ")"));
    }
    Formats.refuseUnwritable(SchemaType.JSON, struct("(u UNION(v VARIANT))"));
    Formats.refuseUnwritable(SchemaType.JSON, struct("(u UNION(s STRUCT<x INT>, n INT))"));
  }

  @Test
  void aJsonUnionsBranchesAreWrittenAsDifferentJsonTypes() {
    Formats.refuseUnwritable(SchemaType.JSON, struct("(u UNION(a INT, b STRING))"));
    Formats.refuseUnwritable(SchemaType.AVRO, struct("(u UNION(a BIGINT, b DOUBLE))"));
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
        struct("(u UNION(a BIGINT, b DOUBLE))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("A UNION of attribute u in a JSON type cannot have branches a and b, "
            + "which are written as one JSON type: a value of either would match both");

    // A branch that is a union is written as any of its members is: a value of one of them
    // that another branch is written as would match both.
    for (String union : new String[] {"UNION(a INT, b UNION(c INT, d STRING))",
        "UNION(b UNION(c BOOLEAN, d STRING), a VARCHAR(3))",
        "UNION(a INT, b UNION(c BOOLEAN, d UNION(e STRING, f DOUBLE)))",
        "UNION(a UNION(p INT, q STRING), b UNION(c BOOLEAN, d STRING))",
        "UNION(a INT, b UNION(c BOOLEAN, v VARIANT))"}) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
          struct("(u " + union + ")"))).as(union)
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("which are written as one JSON type");
      Formats.refuseUnwritable(SchemaType.PROTOBUF, struct("(u " + union + ")"));
    }
    Formats.refuseUnwritable(SchemaType.JSON,
        struct("(u UNION(a INT, b UNION(c BOOLEAN, d STRING)))"));
  }

  // A row that has an attribute of its own type is come to again while two rows are
  // compared: the attributes after it still tell them apart, or do not.
  @Test
  void aJsonUnionsRowsOfTheirOwnTypeAreCompared() {
    String rows = "STRUCT A (child UNION(link A, leaf INT) NOT NULL, kind %s NOT NULL); "
        + "STRUCT B (child UNION(link B, leaf INT) NOT NULL, kind %s NOT NULL); "
        + "STRUCT t (u UNION(x A, y B)); TYPE t;";
    Formats.refuseUnwritable(SchemaType.JSON,
        TypeScripts.parse(String.format(rows, "ENUM('a')", "ENUM('b')")));
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
        TypeScripts.parse(String.format(rows, "ENUM('a')", "ENUM('a')"))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches x and y");
    // So of rows of the type they are in, and of a row beside a map of it.
    Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse("STRUCT node (u UNION("
        + "add STRUCT<l node NOT NULL, r node NOT NULL, kind ENUM('add') NOT NULL>, "
        + "mul STRUCT<l node NOT NULL, r node NOT NULL, kind ENUM('mul') NOT NULL>, "
        + "lit DOUBLE) NOT NULL);"));
    Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse("STRUCT tree ("
        + "kids MAP<STRING, tree> NOT NULL, name STRING NOT NULL, "
        + "u UNION(one tree, many MAP<STRING, tree>));"));
  }

  // A union two of whose branches a value was of both before is left; another two that a
  // value would be of both with the types its names are then of are told.
  @Test
  void aJsonUnionsNewPairIsToldBesideAnOldOne() {
    LogicalType type = TypeScripts.parse("STRUCT R (a INT); STRUCT t (u UNION(p STRUCT<k INT>, "
        + "q STRUCT<k INT>, x R, y STRUCT<b STRING>)); TYPE t;");
    Schema union = type.getRootSchema().getType() != Schema.Type.NAMED_TYPE_REF
        ? type.getRootSchema()
        : type.getNamedTypes().get(type.getRootSchema().getQualifiedName());
    KnownTypes before = KnownTypes.of(Map.of("R", struct("(a INT)").getRootSchema()));
    KnownTypes after = KnownTypes.of(Map.of("R", struct("(a INT, b STRING)").getRootSchema()));
    assertThat(Formats.newlyOverlapping(union, before, before, null)).isNull();
    assertThat(Formats.newlyOverlapping(union, before, after, null))
        .containsExactly("u", "x", "y");
  }

  // Two families of rows, each of which names every row of its family: by a union with
  // it, or as itself; the second family's in another order of them, or not; with a tag on
  // every row, on the first, on the last, or on none.
  private static LogicalType families(int n, String link, String tagged, boolean shifted) {
    StringBuilder script = new StringBuilder();
    for (String family : new String[] {"A", "B"}) {
      for (int i = 0; i < n; i++) {
        script.append("STRUCT ").append(family).append(i).append(" (");
        for (int j = 0; j < n; j++) {
          int to = shifted && family.equals("B") ? (i + j) % n : j;
          script.append("c").append(j).append(' ').append(link.replace("$", family + to))
              .append(", ");
        }
        boolean tag = tagged.equals("all") || tagged.equals("first") && i == 0
            || tagged.equals("last") && i == n - 1;
        script.append("tag ENUM('").append(tag ? family : "same").append("') NOT NULL); ");
      }
    }
    return TypeScripts.parse(script + "STRUCT t (u UNION(x A0, y B0)); TYPE t;");
  }

  // Rows that name each other are come to by as many ways as there are orders of them:
  // each two are judged by what the others are taken to be, and not by every such way.
  @Test
  void aJsonUnionsRowsThatNameEachOtherAreJudgedTwoByTwo() {
    String union = "UNION(link $, leaf INT) NOT NULL";
    String direct = "$ NOT NULL";
    assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
      for (LogicalType told : List.of(families(16, union, "all", false),
          families(16, union, "first", false), families(16, direct, "first", false),
          families(16, direct, "last", true))) {
        Formats.refuseUnwritable(SchemaType.JSON, told);
      }
      // Rows of one type under two attributes, forty deep: each two are judged once.
      StringBuilder deep = new StringBuilder();
      for (String family : new String[] {"A", "B"}) {
        for (int i = 0; i < 40; i++) {
          deep.append("STRUCT ").append(family).append(i).append(" (c0 ").append(family)
              .append(i + 1).append(" NOT NULL, c1 ").append(family).append(i + 1)
              .append(" NOT NULL); ");
        }
        deep.append("STRUCT ").append(family).append("40 (k INT NOT NULL); ");
      }
      for (LogicalType alike : List.of(families(16, union, "none", false),
          families(16, direct, "none", true), families(16, union, "last", true),
          TypeScripts.parse(deep + "STRUCT t (u UNION(x A0, y B0)); TYPE t;"))) {
        assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON, alike))
            .isInstanceOf(TypeRefusedException.class)
            .hasMessageContaining("cannot have branches x and y");
      }
    });
  }

  // Two rows are told apart by what tells apart the rows they name, however many steps
  // away that is and in whatever order they are met; and are told by both of them.
  @Test
  void aJsonUnionsRowsAreToldApartByTheRowsTheyName() {
    Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse(
        "STRUCT X (q Q1 NOT NULL, k INT NOT NULL); STRUCT Y (q Q2 NOT NULL, k STRING NOT NULL);"
            + " STRUCT Q1 (p X NOT NULL); STRUCT Q2 (p Y NOT NULL); "
            + "STRUCT t (u UNION(q1 Q1, q2 Q2)); TYPE t;"));
    Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse(
        "STRUCT A0 (n A1 NOT NULL); STRUCT A1 (n A2 NOT NULL); STRUCT A2 (n A3 NOT NULL); "
            + "STRUCT A3 (k INT NOT NULL); STRUCT B0 (n B1 NOT NULL); STRUCT B1 (n B2 NOT NULL); "
            + "STRUCT B2 (n B3 NOT NULL); STRUCT B3 (k STRING NOT NULL); "
            + "STRUCT t (u UNION(x A0, y B0)); TYPE t;"));
    for (String[] three : new String[][] {
        {"STRUCT A (k INT NOT NULL); STRUCT B1 (k STRING NOT NULL); STRUCT B2 (k INT NOT NULL); "
            + "STRUCT t (u UNION(x A, y B1, z B2)); TYPE t;", "x and z"},
        {"STRUCT A1 (k INT NOT NULL); STRUCT B (k STRING NOT NULL); "
            + "STRUCT A2 (k STRING NOT NULL); STRUCT t (u UNION(x A1, y B, z A2)); TYPE t;",
            "y and z"}}) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
          TypeScripts.parse(three[0]))).isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("cannot have branches " + three[1]);
    }
    // A type of the document's own is written with no null, whatever it says.
    Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse("STRUCT N (p INT); STRUCT t ("
        + "u UNION(r1 STRUCT<a N NOT NULL>, r2 STRUCT<a BOOLEAN>) NOT NULL); TYPE t;"));
  }

  // Every two that were judged by two others are judged again when those are told apart,
  // and not the first of them alone, in whichever order the branches are.
  @Test
  void aJsonUnionsRowsJudgedByOthersAreAllJudgedAgain() {
    String rows = "STRUCT QA (k INT NOT NULL); STRUCT QB (k STRING NOT NULL); "
        + "STRUCT A1 (q QA NOT NULL, t ENUM('one') NOT NULL); "
        + "STRUCT B1 (q QB NOT NULL, t ENUM('one') NOT NULL); "
        + "STRUCT A2 (q QA NOT NULL, t ENUM('two') NOT NULL); "
        + "STRUCT B2 (q QB NOT NULL, t ENUM('two') NOT NULL); ";
    for (String union : List.of(
        "STRUCT t (u UNION(a A1, b B1, c A2, d B2)); TYPE t;",
        "STRUCT t (u UNION(d B2, c A2, b B1, a A1)); TYPE t;",
        "STRUCT A3 (q QA NOT NULL, t ENUM('three') NOT NULL, x A1 NOT NULL, y A2 NOT NULL); "
            + "STRUCT B3 (q QB NOT NULL, t ENUM('three') NOT NULL, x B1 NOT NULL, "
            + "y B2 NOT NULL); STRUCT t (u UNION(a A1, b B1, c A2, d B2, e A3, f B3)); "
            + "TYPE t;")) {
      Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse(rows + union));
    }
    // A number said for a union is no number of a field only in a Protobuf type.
    String numbered = "STRUCT t (a INT, r STRUCT<m INT, v UNION(x BOOLEAN, y STRING) WITH "
        + "('protobuf.field.number' = '7')>); TYPE t;";
    Formats.refuseUnwritable(SchemaType.AVRO, TypeScripts.parse(numbered));
    Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse(numbered));
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF,
        TypeScripts.parse(numbered))).isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Attribute r.v cannot have a Protobuf field number");
  }

  // Two rings of rows of lengths with no common measure, told apart in one place, and two
  // chains told apart at their ends: each two are judged again only when one they were
  // judged by is told apart.
  @Test
  void aJsonUnionsLongChainsOfRowsAreJudgedInTheirOrder() {
    StringBuilder rings = new StringBuilder();
    for (int i = 0; i < 140; i++) {
      rings.append("STRUCT A").append(i).append(" (nxt A").append((i + 1) % 140)
          .append(" NOT NULL, y ").append(i == 139 ? "INT" : "VARIANT").append(" NOT NULL); ");
    }
    for (int j = 0; j < 141; j++) {
      rings.append("STRUCT B").append(j).append(" (nxt B").append((j + 1) % 141)
          .append(" NOT NULL, y ").append(j == 140 ? "STRING" : "VARIANT")
          .append(" NOT NULL); ");
    }
    StringBuilder chains = new StringBuilder();
    for (String family : new String[] {"A", "B"}) {
      for (int i = 0; i < 1500; i++) {
        chains.append("STRUCT ").append(family).append(i).append(" (c0 ").append(family)
            .append(i + 1).append(" NOT NULL, c1 ").append(family).append(i + 1)
            .append(" NOT NULL); ");
      }
      chains.append("STRUCT ").append(family).append("1500 (k ")
          .append(family.equals("B") ? "STRING" : "INT").append(" NOT NULL); ");
    }
    List<LogicalType> told = List.of(
        TypeScripts.parse(rings + "STRUCT t (u UNION(x A0, y B0)); TYPE t;"),
        TypeScripts.parse(chains + "STRUCT t (u UNION(x A0, y B0)); TYPE t;"));
    assertTimeoutPreemptively(Duration.ofSeconds(20),
        () -> told.forEach(lt -> Formats.refuseUnwritable(SchemaType.JSON, lt)));
  }

  // Two rows come to again while they are compared are taken to match; two compared once
  // and told apart are so wherever they are come to again.
  @Test
  void aJsonUnionsRowsComeToAgainAreJudgedAsTheyWere() {
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse(
        "STRUCT A (inner B NOT NULL); STRUCT B (inner A, k INT); "
            + "STRUCT t (u UNION(x A, y B)); TYPE t;")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches x and y");
    Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse(
        "STRUCT R1 (k INT NOT NULL); STRUCT R2 (k STRING NOT NULL); "
            + "STRUCT S1 (a R1 NOT NULL, t1 INT NOT NULL); "
            + "STRUCT S2 (a R1 NOT NULL, t2 INT NOT NULL); "
            + "STRUCT T (a R2 NOT NULL, t2 INT NOT NULL); "
            + "STRUCT t (u UNION(x UNION(s1 S1, s2 S2), y T)); TYPE t;"));
    // Two that match only while two others are taken to are told apart once those are.
    Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse(
        "STRUCT X (q Q1 NOT NULL, k INT NOT NULL); STRUCT Y (q Q2 NOT NULL, k STRING NOT NULL);"
            + " STRUCT Q1 (p X NOT NULL); STRUCT Q2 (p Y NOT NULL); "
            + "STRUCT t (u UNION(x X, y Y, q1 Q1, q2 Q2)); TYPE t;"));
  }

  // An enum value's number may be 0 or less, as a field's may not.
  @Test
  void aProtobufEnumValuesNumberMayBeZeroOrLess() {
    Formats.refuseUnwritable(SchemaType.PROTOBUF, struct("(e ENUM("
        + "'A' WITH ('protobuf.enum.number' = '0'), "
        + "'B' WITH ('protobuf.enum.number' = '-1')))"));
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF,
        struct("(a INT WITH ('protobuf.field.number' = '0'))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("is not a field's number, which is 1 or more: '0'");
  }

  // A union in an attribute of a row is told by that attribute's path; and one of thousands
  // of branches of one kind is told by its first two, with no word of the rest.
  @Test
  void aJsonUnionIsToldByItsPathAndByItsFirstTwoBranches() {
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
        struct("(e STRUCT<a UNION(x INT, y BIGINT), b ARRAY<STRUCT<c UNION(p INT, q INT)>>>)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("A UNION of attribute e.a in a JSON type cannot have branches "
            + "x and y");
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
        struct("(e STRUCT<b ARRAY<STRUCT<c UNION(p INT, q INT)>>>)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("A UNION of attribute e.b.c in a JSON type cannot have");
    StringBuilder many = new StringBuilder("(u UNION(");
    for (int i = 0; i < 4000; i++) {
      many.append(i > 0 ? ", " : "").append("b").append(i).append(" INT");
    }
    LogicalType lt = struct(many + "))");
    assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
        assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON, lt))
            .isInstanceOf(TypeRefusedException.class)
            .hasMessageContaining("cannot have branches b0 and b1"));
  }

  // A union of hundreds of rows is gone through once the rows are judged, and not again
  // for each two of them: whether none of them is another, or the last two are.
  @Test
  void aJsonUnionOfManyRowsIsJudgedOnce() {
    StringBuilder apart = new StringBuilder("(u UNION(");
    for (int i = 0; i < 400; i++) {
      apart.append(i > 0 ? ", " : "").append("b").append(i).append(" STRUCT<k").append(i)
          .append(" INT NOT NULL>");
    }
    LogicalType told = struct(apart + "))");
    LogicalType alike = struct(apart + ", b400 STRUCT<k399 INT NOT NULL>))");
    assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
      Formats.refuseUnwritable(SchemaType.JSON, told);
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON, alike))
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("cannot have branches b399 and b400");
    });
  }

  // What is in a member's type is told with the member before it; an enum value's number is
  // no field's, and may be one of those kept for fields or above their last.
  @Test
  void aMembersAttributeIsToldWithTheMemberAndAnEnumsNumberIsNoFields() {
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF,
        struct("(id INT, u UNION(p INT, q STRUCT<k INT DEFAULT 1>))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Attribute u.q.k has a DEFAULT");
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
        struct("(id INT, l ARRAY<UNION(p INT, q STRUCT<w UNION(a INT, b BIGINT)>)>)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("A UNION of attribute l.q.w in a JSON type");
    Formats.refuseUnwritable(SchemaType.PROTOBUF, struct("(id INT, e ENUM('A', "
        + "'B' WITH ('protobuf.enum.number' = '19000'), "
        + "'C' WITH ('protobuf.enum.number' = '19999'), "
        + "'D' WITH ('protobuf.enum.number' = '536870912')))"));
  }

  // A reference that moves is judged by every two branches before and after, and not by
  // the first two; the union is told by its path.
  @Test
  void aMovedReferenceIsJudgedByEveryTwoBranchesAndToldByItsPath() {
    String union = "UNION(a INT, b BIGINT, c DOUBLE, r1 N, r2 STRUCT<k INT NOT NULL>)";
    String[][] rows = {
        {"STRUCT t (id INT, u " + union + "); TYPE t;", "u"},
        {"STRUCT t (id INT, u INT, r STRUCT<u " + union + ">); TYPE t;", "r.u"},
        {"STRUCT t (id INT, o UNION(p INT, q STRUCT<w " + union + ">)); TYPE t;", "o.q.w"}};
    for (String[] row : rows) {
      LogicalType before = TypeScripts.parse("STRUCT N (k STRING NOT NULL); " + row[0]);
      LogicalType after = TypeScripts.parse("STRUCT N (k INT NOT NULL); " + row[0]);
      Schema root = before.getRootSchema().getType() != Schema.Type.NAMED_TYPE_REF
          ? before.getRootSchema()
          : before.getNamedTypes().get(before.getRootSchema().getQualifiedName());
      assertThat(Formats.newlyOverlapping(root, KnownTypes.of(before.getNamedTypes()),
          KnownTypes.of(after.getNamedTypes()), null)).as(row[1])
          .containsExactly(row[1], "r1", "r2");
      assertThat(Formats.newlyOverlapping(root, KnownTypes.of(before.getNamedTypes()),
          KnownTypes.of(before.getNamedTypes()), null)).as(row[1]).isNull();
    }
  }

  // A name with a dot in it is quoted in a path, which is then no other attribute's; and a
  // value a statement gave is told as the literal it is, a quote in it doubled.
  @Test
  void aPathQuotesANameWithADotAndAValueIsToldAsALiteral() {
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF,
        struct("(`e.x` STRUCT<`a.b` INT DEFAULT 1, c INT>)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Attribute `e.x`.`a.b` has a DEFAULT");
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF,
        struct("(e STRUCT<x STRUCT<v INT DEFAULT 1>>)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Attribute e.x.v has a DEFAULT");
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF,
        struct("(e STRUCT<x INT WITH ('protobuf.field.number' = 'it''s')>)")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Property protobuf.field.number "
            + "of attribute e.x is not a valid number: 'it''s'");
    assertThat(Formats.segment("plain")).isEqualTo("plain");
    assertThat(Formats.segment("a.`b")).isEqualTo("`a.``b`");
  }

  // Rows, arrays and maps are written as objects and arrays: two branches are refused where
  // a value of one would be a value of the other.
  @Test
  void aJsonUnionsRowsArraysAndMapsAreToldApart() {
    for (String union : new String[] {
        // A row's value, written with each of its attributes, is another row's: it has no
        // attribute the other lacks, and each the other requires.
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<a INT, b STRING>)",
        "UNION(r1 STRUCT<a INT, b STRING>, r2 STRUCT<b STRING>)",
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<a INT>)",
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<a STRING>)",
        "UNION(r1 STRUCT<a INT NOT NULL>, r2 STRUCT<a BIGINT NOT NULL>)",
        "UNION(r1 STRUCT<kind ENUM('circle', 'both') NOT NULL>, "
            + "r2 STRUCT<kind ENUM('square', 'both') NOT NULL>)",
        "UNION(r1 STRUCT<kind ENUM('circle')>, r2 STRUCT<kind ENUM('square')>)",
        "UNION(r1 ENUM('A', 'B'), r2 ENUM('B', 'C'))",
        "UNION(r1 ENUM('A', 'B'), r2 VARCHAR(3))",
        "UNION(r1 STRUCT<x INT>, r2 UNION(c STRING, d STRUCT<x INT>))",
        // An empty array is a value of any type written as one.
        "UNION(r1 ARRAY<INT>, r2 ARRAY<STRING>)",
        "UNION(r1 ARRAY<INT>, r2 MAP<INT, STRING>)",
        // A map written as an object takes any property.
        "UNION(r1 MAP<STRING, INT>, r2 STRUCT<a INT>)",
        "UNION(r1 MAP<STRING, INT>, r2 STRUCT<a INT NOT NULL>)",
        "UNION(r1 MAP<STRING, INT>, r2 MAP<STRING, STRING>)",
        "UNION(r1 MULTISET<STRING>, r2 STRUCT<a INT>)",
        // A row's attribute that may be null does not tell it from a map, nor one of the
        // type of a multiset's counts, nor one of a name no type is known by.
        "UNION(r1 STRUCT<k STRING>, r2 MAP<STRING, INT>)",
        "UNION(r1 STRUCT<k INT NOT NULL>, r2 MULTISET<STRING>)",
        "UNION(r1 STRUCT<k Missing NOT NULL>, r2 STRUCT<k INT NOT NULL>)",
        "UNION(r1 STRUCT<k Missing NOT NULL>, r2 MAP<STRING, INT>)"}) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
          struct("(u " + union + ")"))).as(union)
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("cannot have branches r1 and r2");
      Formats.refuseUnwritable(SchemaType.AVRO, struct("(u " + union + ")"));
    }
    for (String union : new String[] {
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<b STRING>)",
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<a INT, b STRING NOT NULL>)",
        // An attribute both require, whose value in one is never one in the other, tells
        // two rows apart, and a row from a map of other values.
        "UNION(r1 STRUCT<a INT NOT NULL>, r2 STRUCT<a STRING NOT NULL>)",
        // One of them requiring it is enough: the other's null is not a value of it.
        "UNION(r1 STRUCT<a INT NOT NULL>, r2 STRUCT<a STRING>)",
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<a STRING NOT NULL>)",
        // As rows tagged by an attribute that is one symbol in each.
        "UNION(r1 STRUCT<kind ENUM('circle') NOT NULL, r DOUBLE>, "
            + "r2 STRUCT<kind ENUM('square') NOT NULL, r DOUBLE>)",
        "UNION(r1 ENUM('A', 'B'), r2 ENUM('C'))",
        "UNION(r1 STRUCT<a STRUCT<x INT NOT NULL> NOT NULL>, "
            + "r2 STRUCT<a STRUCT<y STRING NOT NULL> NOT NULL>)",
        "UNION(r1 MAP<STRING, STRING>, r2 STRUCT<a INT NOT NULL>)",
        "UNION(r1 STRUCT<k BOOLEAN NOT NULL>, r2 MULTISET<STRING>)",
        "UNION(r1 STRUCT<a INT>, r2 ARRAY<INT>, r3 INT)",
        "UNION(r1 MAP<STRING, INT>, r2 ARRAY<INT>)",
        "UNION(r1 MAP<STRING, INT>, r2 MAP<INT, STRING>)"}) {
      Formats.refuseUnwritable(SchemaType.JSON, struct("(u " + union + ")"));
    }
  }

  private static final String FIVE = " WITH ('protobuf.field.number' = '5')";

  // A number two attributes share is a Protobuf type's matter alone; there it is one by
  // its value, however it is spelt, and what is no number a field can have is told as that.
  @Test
  void aSharedNumberIsToldByItsValueAndWhatIsNoNumberAsThat() {
    for (SchemaType format : List.of(SchemaType.AVRO, SchemaType.JSON)) {
      Formats.refuseUnwritable(format, struct("(a INT" + FIVE + ", b INT" + FIVE + ", u "
          + "UNION(p INT" + FIVE + ", q STRING))"));
    }
    for (String spelt : List.of("05", "+5")) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF, struct("(r "
          + "STRUCT<x INT" + FIVE + ", y INT WITH ('protobuf.field.number' = '" + spelt
          + "')>)"))).as(spelt).isInstanceOf(TypeRefusedException.class)
          .hasMessage("Attribute r.y cannot have the Protobuf field number 5: attribute r.x "
              + "has it; give it another, or none");
    }
    String[][] none = {{"x", "is not a valid number: 'x'"}, {"", "is not a valid number: ''"},
        {" 5", "is not a valid number: ' 5'"}, {"0", "is not a field's number"},
        {"19000", "Protobuf keeps it for itself"},
        {"536870912", "a field's number is 536870911 at most"}};
    for (String[] number : none) {
      String with = " WITH ('protobuf.field.number' = '" + number[0] + "')";
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF,
          struct("(a INT" + with + ", b INT" + with + ")"))).as(number[0])
          .isInstanceOf(TypeRefusedException.class).hasMessageContaining(number[1])
          .hasMessageNotContaining("has it");
    }
  }

  // A number a union's member has is no other member's, of that union or of another of
  // the row, and no field's after it; a union's members in a row of their own are another
  // message's.
  @Test
  void aNumberOfAUnionsMemberIsNoLaterHoldersOfTheRow() {
    String[][] shared = {
        {"(u UNION(p INT" + FIVE + ", q STRING), x INT" + FIVE + ")", "Attribute x cannot "
            + "have the Protobuf field number 5: branch p of attribute u has it; give it "
            + "another, or none"},
        {"(u UNION(p INT" + FIVE + ", q STRING" + FIVE + "))", "Branch q of attribute u "
            + "cannot have the Protobuf field number 5: branch p of attribute u has it; give "
            + "it another, or none"},
        {"(u UNION(p INT" + FIVE + ", q STRING), v UNION(c INT" + FIVE + ", d STRING))",
            "Branch c of attribute v cannot have the Protobuf field number 5: branch p of "
            + "attribute u has it; give it another, or none"}};
    for (String[] row : shared) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF, struct(row[0])))
          .as(row[0]).isInstanceOf(TypeRefusedException.class).hasMessage(row[1]);
    }
    Formats.refuseUnwritable(SchemaType.PROTOBUF, struct("(x INT" + FIVE + ", r STRUCT<u "
        + "UNION(p INT" + FIVE + ", q STRING)>)"));
  }

  // A union of thousands of branches whose first two a value would be of both is told by
  // those two, and is not gone through for every two of them: scalars and rows alike.
  @Test
  void aJsonUnionOfThousandsIsToldByItsFirstTwo() {
    List<UnionBranch> scalars = new ArrayList<>();
    List<UnionBranch> rows = new ArrayList<>();
    for (int i = 0; i < 30000; i++) {
      scalars.add(new UnionBranch("b" + i, Schema.create(Schema.Type.INT)));
      rows.add(new UnionBranch("b" + i, Schema.createStruct(List.of(
          new Field("t", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 0),
          new Field("v", Schema.create(Schema.Type.INT), 1)))));
    }
    for (List<UnionBranch> branches : List.of(scalars, rows)) {
      Schema union = Schema.createUnion(branches);
      assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
          assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON, union))
              .isInstanceOf(TypeRefusedException.class)
              .hasMessageContaining("cannot have branches b0 and b1"));
    }
  }

  // Two rows that each hold a union of many rows: the rows of the two unions are met as
  // many at a turn as were told apart, and not one at a turn with all before it asked again.
  @Test
  void rowsThatHoldUnionsOfManyRowsAreJudgedOnce() {
    StringBuilder body = new StringBuilder("(id INT, u UNION(");
    for (String[] holder : new String[][] {{"x", "m", "a"}, {"y", "n", "b"}}) {
      body.append(holder[0]).append(" STRUCT<a UNION(");
      for (int i = 0; i < 250; i++) {
        body.append(i > 0 ? ", " : "").append(holder[1]).append(i).append(" STRUCT<")
            .append(holder[2]).append(i).append(" INT NOT NULL>");
      }
      body.append(") NOT NULL>").append(holder[0].equals("x") ? ", " : "");
    }
    LogicalType lt = struct(body + "))");
    assertTimeoutPreemptively(Duration.ofSeconds(20),
        () -> Formats.refuseUnwritable(SchemaType.JSON, lt));
  }

  // A name with a dot or a backtick in it is quoted in a path, a backtick in it doubled.
  @Test
  void aNameWithADotOrABacktickIsQuotedInAPath() {
    assertThat(Formats.segment("a")).isEqualTo("a");
    assertThat(Formats.segment("a.b")).isEqualTo("`a.b`");
    assertThat(Formats.segment("`w")).isEqualTo("```w`");
    assertThat(Formats.segment("'q")).isEqualTo("`'q`");
    assertThat(Formats.joined("e", "`w")).isEqualTo("e.```w`");
    assertThat(Formats.joined("`e.x`", "y")).isEqualTo("`e.x`.y");
  }

  // Rows of a union that each require one union of rows: the rows held are met a few at a
  // turn, and not all for every two holders, which keeps every two held for each.
  @Test
  void rowsThatRequireASharedUnionOfRowsAreNotAllMetForEveryTwo() {
    StringBuilder script = new StringBuilder();
    StringBuilder held = new StringBuilder("UNION(");
    StringBuilder holders = new StringBuilder("UNION(");
    for (int i = 0; i < 120; i++) {
      script.append("STRUCT R").append(i).append(" (v").append(i).append(" INT NOT NULL); ");
      held.append(i > 0 ? ", " : "").append("r").append(i).append(" R").append(i);
      holders.append(i > 0 ? ", " : "").append("s").append(i).append(" S").append(i);
    }
    for (int i = 0; i < 120; i++) {
      script.append("STRUCT S").append(i).append(" (u ").append(held).append(") NOT NULL, k")
          .append(i).append(" INT NOT NULL); ");
    }
    LogicalType lt = TypeScripts.parse(script + "STRUCT t (id INT, x " + holders
        + ")); TYPE t;");
    assertTimeoutPreemptively(Duration.ofSeconds(20),
        () -> Formats.refuseUnwritable(SchemaType.JSON, lt));
  }

  // Rows of a union that each require a union of rows of their own, the first two of which
  // match: every two holders meet one pair of those held, and not every two.
  @Test
  void rowsThatRequireUnionsOfTheirOwnRowsMeetOnePairWhereTheFirstMatch() {
    StringBuilder script = new StringBuilder();
    StringBuilder holders = new StringBuilder("UNION(");
    for (int i = 0; i < 20; i++) {
      StringBuilder held = new StringBuilder("UNION(");
      for (int j = 0; j < 30; j++) {
        held.append(j > 0 ? ", " : "").append("r").append(j).append(" STRUCT<v").append(j)
            .append(" INT NOT NULL>");
      }
      script.append("STRUCT S").append(i).append(" (u ").append(held).append(") NOT NULL, k")
          .append(i).append(" INT NOT NULL); ");
      holders.append(i > 0 ? ", " : "").append("s").append(i).append(" S").append(i);
    }
    LogicalType lt = TypeScripts.parse(script + "STRUCT t (id INT, x " + holders
        + ")); TYPE t;");
    Schema union = lt.getRootSchema().getField("x").getSchema();
    // 190 pairs of holders, and one pair held for each: all held would be 171,000 more.
    assertThat(Formats.rowsMet(union, lt.getNamedTypes())).isEqualTo(380);
  }
}
