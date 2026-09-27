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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Every history either has provenance or is rejected by name — no logical form, recursion, or
 * names and aliases that determine no single identity. Anything else the computation throws would
 * reach the registry as a server error, retried on every record, so it is a condition to name.
 */
class ProvenanceHistorySweepTest {

  private static final String I = "\"int\"";
  private static final String OBJ = "{\"type\":\"object\",\"properties\":";
  private static final String A = rec("A", f("x", I));
  private static final String B = rec("B", f("y", I));

  static Stream<Arguments> histories() {
    List<Arguments> h = new ArrayList<>();
    // Avro field aliases.
    h.add(of("two fields alias one old name", avro(f("a", I)),
        avro(fa("b", I, "a"), fa("c", I, "a"))));
    h.add(of("one field aliases two old names", avro(f("a", I), f("b", I)),
        avro(fa("c", I, "a", "b"))));
    h.add(of("an alias that is its own name", avro(f("a", I)), avro(fa("a", I, "a"))));
    h.add(of("a duplicate alias", avro(f("a", I)), avro(fa("b", I, "a", "a"))));
    h.add(of("two fields swapped by aliases", avro(f("a", I), f("b", I)),
        avro(fa("b", I, "a"), fa("a", I, "b"))));
    h.add(of("a renamed field and a new one taking its name", avro(f("a", I)),
        avro(fa("b", I, "a"), f("a", I))));
    h.add(of("an alias naming a new sibling", avro(f("x", I)), avro(f("a", I), fa("b", I, "a"))));
    h.add(of("renamed, then renamed back", avro(f("a", I)), avro(fa("b", I, "a")),
        avro(fa("a", I, "b"))));
    h.add(of("an alias to a dormant name", avro(f("a", I)), avro(f("z", I)),
        avro(fa("b", I, "a"), f("z", I))));
    h.add(of("an alias naming a sibling added later", avro(f("a", I)), avro(f("a", I), f("b", I)),
        avro(fa("c", I, "a", "b"))));
    // Avro named types.
    h.add(of("two types merged by alias", avro(f("u", A), f("v", B)),
        avro(f("u", rec("A", fa("x", I, "y"), "B")), f("v", "\"A\""))));
    h.add(of("a type aliasing two old types", avro(f("u", A), f("v", B)),
        avro(f("u", rec("C", f("x", I), "A", "B")), f("v", "\"C\""))));
    h.add(of("one type split in two", avro(f("u", A), f("v", "\"A\"")),
        avro(f("u", A), f("v", rec("C", f("x", I), "A")))));
    h.add(of("two types merged inside unions", avro(f("u", "[\"null\"," + A + "]"),
        f("v", "[\"null\"," + B + "]")), avro(f("u", "[\"null\"," + rec("A", fa("x", I, "y"), "B")
        + "]"), f("v", "[\"null\",\"A\"]"))));
    h.add(of("an enum renamed by alias",
        avro(f("e", "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"X\"]}")),
        avro(f("e", "{\"type\":\"enum\",\"name\":\"F\",\"aliases\":[\"E\"],"
            + "\"symbols\":[\"X\"]}"))));
    h.add(of("a field of a shared type renamed", avro(f("u", A), f("v", "\"A\"")),
        avro(f("u", rec("A", fa("z", I, "x"))), f("v", "\"A\""))));
    // Protobuf, identified by number.
    h.add(of("numbers swapped", proto("int32 a = 1;\n int32 b = 2;"),
        proto("int32 a = 2;\n int32 b = 1;")));
    h.add(of("fields moved into a oneof", proto("int32 a = 1;\n int32 b = 2;"),
        proto("oneof o {\n  int32 a = 1;\n  int32 b = 2;\n }")));
    h.add(of("a oneof split", proto("oneof o {\n  int32 a = 1;\n  int32 b = 2;\n }"),
        proto("oneof o {\n  int32 a = 1;\n }\n oneof q {\n  int32 b = 2;\n }")));
    h.add(of("oneofs renamed across",
        proto("oneof o {\n  int32 a = 1;\n }\n oneof q {\n  int32 b = 2;\n }"),
        proto("oneof q {\n  int32 a = 1;\n }\n oneof o {\n  int32 b = 2;\n }")));
    h.add(of("two message types merged",
        proto("N n = 1;\n K o = 2;\n}\nmessage N {\n int32 x = 1;\n}\nmessage K {\n int32 x = 1;"),
        proto("N n = 1;\n N o = 2;\n}\nmessage N {\n int32 x = 1;")));
    // JSON, identified by name.
    h.add(of("properties swapped",
        json("{\"a\":{\"type\":\"integer\"},\"b\":{\"type\":\"string\"}}"),
        json("{\"b\":{\"type\":\"integer\"},\"a\":{\"type\":\"string\"}}")));
    h.add(of("a property moved into a union branch", json("{\"a\":{\"type\":\"integer\"}}"),
        json("{\"u\":{\"oneOf\":[{\"type\":\"string\"}," + OBJ
            + "{\"a\":{\"type\":\"integer\"}}}]}}")));
    h.add(of("one definition split in two",
        json("{\"a\":{\"$ref\":\"#/definitions/D\"},\"b\":{\"$ref\":\"#/definitions/D\"}},"
            + "\"definitions\":{\"D\":" + OBJ + "{\"x\":{\"type\":\"integer\"}}}}"),
        json("{\"a\":{\"$ref\":\"#/definitions/D\"},\"b\":{\"$ref\":\"#/definitions/E\"}},"
            + "\"definitions\":{\"D\":" + OBJ + "{\"x\":{\"type\":\"integer\"}}},\"E\":" + OBJ
            + "{\"x\":{\"type\":\"integer\"}}}}")));
    // A subject whose format changed.
    h.add(of("Avro, then JSON", avro(f("a", I)), json("{\"a\":{\"type\":\"integer\"}}")));
    return h.stream();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("histories")
  void provenanceIsComputedOrTheHistoryIsRejectedByName(String label, List<ParsedSchema> versions) {
    List<ProvenanceHistory.Entry> entries = new ArrayList<>();
    for (int i = 0; i < versions.size(); i++) {
      entries.add(new ProvenanceHistory.Entry(i + 1, i + 1, false));
    }
    try {
      ProvenanceHistory.compute("s", entries, versions, false);
    } catch (ValidationException | RecursiveTypeException | AmbiguousProvenanceException e) {
      // Rejected by name: the registry answers 422, and the reader falls back.
    }
  }

  // -------------------------------------------------------------------------------------------

  private static Arguments of(String label, ParsedSchema... versions) {
    return Arguments.of(label, Arrays.asList(versions));
  }

  private static AvroSchema avro(String... fields) {
    return new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + String.join(",", fields) + "]}");
  }

  private static ProtobufSchema proto(String body) {
    return new ProtobufSchema("syntax = \"proto3\";\npackage p;\nmessage M {\n " + body + "\n}\n");
  }

  private static JsonSchema json(String properties) {
    return new JsonSchema(OBJ + properties + "}");
  }

  private static String f(String name, String type) {
    return "{\"name\":\"" + name + "\",\"type\":" + type + "}";
  }

  private static String fa(String name, String type, String... aliases) {
    return "{\"name\":\"" + name + "\",\"type\":" + type + ",\"aliases\":[\""
        + String.join("\",\"", aliases) + "\"]}";
  }

  private static String rec(String name, String fields, String... aliases) {
    return "{\"type\":\"record\",\"name\":\"" + name + "\"" + (aliases.length > 0
        ? ",\"aliases\":[\"" + String.join("\",\"", aliases) + "\"]" : "")
        + ",\"fields\":[" + fields + "]}";
  }
}
