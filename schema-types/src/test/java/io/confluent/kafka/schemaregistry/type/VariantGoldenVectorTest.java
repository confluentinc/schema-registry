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

package io.confluent.kafka.schemaregistry.type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Golden-vector tests over the binary variant corpus vendored from apache/parquet-testing.
 *
 * <p>Each case is a metadata/value byte pair on disk plus an entry in {@code manifest.json}
 * giving its type and the exact canonical JSON text it must produce. The same corpus and
 * manifest are vendored into the other clients, which run the equivalent assertions, so the
 * seven implementations are pinned to one another and not merely to their own expectations.
 */
public class VariantGoldenVectorTest {

  /**
   * Guards against a drifted copy of the manifest: every client asserts this same digest, so
   * a hand-edited manifest fails loudly instead of quietly grading itself.
   */
  private static final String MANIFEST_SHA256 =
      "9bec19cf0086f1cfb16d6832788fe1e4d9791851c2c42163bf941c5a6658e3de";

  private static final int EXPECTED_CASE_COUNT = 29;

  private static Path dir;
  private static byte[] manifestBytes;
  private static Map<String, JsonNode> cases;

  @BeforeClass
  public static void loadManifest() throws Exception {
    dir = Paths.get(
        VariantGoldenVectorTest.class.getResource("/variant-golden/manifest.json").toURI())
        .getParent();
    manifestBytes = Files.readAllBytes(dir.resolve("manifest.json"));
    cases = new LinkedHashMap<>();
    for (JsonNode c : new ObjectMapper().readTree(manifestBytes).get("cases")) {
      cases.put(c.get("name").asText(), c);
    }
  }

  /** The canonical cross-language name for a variant type. */
  private static String label(Variant.Type type) {
    switch (type) {
      case BYTE: return "int8";
      case SHORT: return "int16";
      case INT: return "int32";
      case LONG: return "int64";
      default: return type.name().toLowerCase(Locale.ROOT);
    }
  }

  private static Variant read(String name) throws Exception {
    return new Variant(Files.readAllBytes(dir.resolve(name + ".value")),
        Files.readAllBytes(dir.resolve(name + ".metadata")));
  }

  private static String hex(ByteBuffer bb) {
    StringBuilder sb = new StringBuilder();
    for (int i = bb.position(); i < bb.limit(); i++) {
      sb.append(String.format("%02x", bb.get(i)));
    }
    return sb.toString();
  }

  @Test
  public void testManifestIsTheOneWeExpect() throws Exception {
    StringBuilder actual = new StringBuilder();
    for (byte b : MessageDigest.getInstance("SHA-256").digest(manifestBytes)) {
      actual.append(String.format("%02x", b));
    }
    Assert.assertEquals("manifest.json has changed; update MANIFEST_SHA256 in all seven clients",
        MANIFEST_SHA256, actual.toString());
    Assert.assertEquals(EXPECTED_CASE_COUNT, cases.size());
  }

  /**
   * Without this, a mistyped or dropped manifest entry would simply never be exercised and
   * every other test here would still pass.
   */
  @Test
  public void testCorpusAndManifestCoverExactlyTheSameCases() throws Exception {
    TreeSet<String> onDisk = new TreeSet<>();
    try (java.util.stream.Stream<Path> files = Files.list(dir)) {
      files.forEach(p -> {
        String f = p.getFileName().toString();
        if (f.endsWith(".metadata")) {
          onDisk.add(f.substring(0, f.length() - ".metadata".length()));
        }
      });
    }
    Assert.assertEquals("corpus files and manifest entries disagree",
        new TreeSet<>(cases.keySet()), onDisk);
    Assert.assertEquals(EXPECTED_CASE_COUNT, onDisk.size());
    for (String name : onDisk) {
      Assert.assertTrue(name + " has no .value file", Files.exists(dir.resolve(name + ".value")));
    }
  }

  @Test
  public void testDecodesToExpectedTypeAndJson() throws Exception {
    List<String> failures = new ArrayList<>();
    for (Map.Entry<String, JsonNode> e : cases.entrySet()) {
      String name = e.getKey();
      try {
        Variant v = read(name);
        String type = label(v.getType());
        if (!e.getValue().get("type").asText().equals(type)) {
          failures.add(name + ": type expected " + e.getValue().get("type").asText()
              + " but was " + type);
        }
        String json = VariantUtils.toJsonString(v);
        if (!e.getValue().get("json").asText().equals(json)) {
          failures.add(name + ": json expected " + e.getValue().get("json").asText()
              + " but was " + json);
        }
      } catch (Exception ex) {
        failures.add(name + ": threw " + ex);
      }
    }
    Assert.assertEquals("", String.join("\n", failures));
  }

  /**
   * Re-encodes each decoded value. Cases flagged byte-exact must reproduce both buffers
   * verbatim; the rest are objects whose field and dictionary ordering is an encoder choice,
   * so they are held to value equivalence instead.
   */
  @Test
  public void testReEncodes() throws Exception {
    List<String> failures = new ArrayList<>();
    for (Map.Entry<String, JsonNode> e : cases.entrySet()) {
      String name = e.getKey();
      Variant golden = read(name);
      VariantBuilder b = new VariantBuilder();
      rebuild(golden, b);
      Variant again = b.build();
      if (e.getValue().get("writeByteExact").asBoolean()) {
        if (!hex(golden.getValueBuffer()).equals(hex(again.getValueBuffer()))) {
          failures.add(name + ": value bytes differ");
        }
        if (!hex(golden.getMetadataBuffer()).equals(hex(again.getMetadataBuffer()))) {
          failures.add(name + ": metadata bytes differ");
        }
      } else {
        String expected = e.getValue().get("json").asText();
        String actual = VariantUtils.toJsonString(again);
        if (!expected.equals(actual)) {
          failures.add(name + ": re-encoded to " + actual);
        }
      }
    }
    Assert.assertEquals("", String.join("\n", failures));
  }

  @Test
  public void testTruncatedValueIsRejected() throws Exception {
    byte[] metadata = Files.readAllBytes(dir.resolve("primitive_int32.metadata"));
    byte[] value = Files.readAllBytes(dir.resolve("primitive_int32.value"));
    byte[] truncated = new byte[1];
    truncated[0] = value[0];
    try {
      new Variant(truncated, metadata).getInt();
      Assert.fail("a one-byte int32 payload was accepted");
    } catch (RuntimeException expected) {
      // a truncated payload must not read past the buffer
    }
  }

  private static void rebuild(Variant v, VariantBuilder b) {
    switch (v.getType()) {
      case NULL: b.appendNull(); break;
      case BOOLEAN: b.appendBoolean(v.getBoolean()); break;
      case BYTE: b.appendByte(v.getByte()); break;
      case SHORT: b.appendShort(v.getShort()); break;
      case INT: b.appendInt(v.getInt()); break;
      case LONG: b.appendLong(v.getLong()); break;
      case STRING: b.appendString(v.getString()); break;
      case DOUBLE: b.appendDouble(v.getDouble()); break;
      case FLOAT: b.appendFloat(v.getFloat()); break;
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16: b.appendDecimal(v.getDecimal()); break;
      case DATE: b.appendDate(v.getInt()); break;
      case TIME: b.appendTime(v.getLong()); break;
      case TIMESTAMP_TZ: b.appendTimestampTz(v.getLong()); break;
      case TIMESTAMP_NTZ: b.appendTimestampNtz(v.getLong()); break;
      case TIMESTAMP_NANOS_TZ: b.appendTimestampNanosTz(v.getLong()); break;
      case TIMESTAMP_NANOS_NTZ: b.appendTimestampNanosNtz(v.getLong()); break;
      case BINARY: b.appendBinary(v.getBinary()); break;
      case UUID: b.appendUUID(v.getUUID()); break;
      case OBJECT: {
        VariantObjectBuilder ob = b.startObject();
        for (int i = 0; i < v.numObjectFields(); i++) {
          Variant.ObjectField f = v.getFieldAtIndex(i);
          ob.appendKey(f.key);
          rebuild(f.value, ob);
        }
        b.endObject();
        break;
      }
      case ARRAY: {
        VariantArrayBuilder ab = b.startArray();
        for (int i = 0; i < v.numArrayElements(); i++) {
          rebuild(v.getElementAtIndex(i), ab);
        }
        b.endArray();
        break;
      }
      default:
        throw new IllegalStateException("unhandled variant type " + v.getType());
    }
  }
}
