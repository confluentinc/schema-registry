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
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The canonical float rendering suite: shortest, then nearest, then ties-to-even, laid out as
 * the JDK lays it out.
 *
 * <p>Two kinds of assertion. The vendored artifact pins the exact expected text, and is copied
 * byte-identically into the other six clients so all seven are held to one authority. The
 * properties restate the contract directly and need no reference implementation, so they catch
 * an implementation that is self-consistently wrong.
 *
 * <p>The artifact is read, never written. Regenerating it is a deliberate manual step, specified
 * in the design document; a test that regenerated its own expectations would pass regardless of
 * what the implementation did.
 */
public class CanonicalNumberTest {

  /**
   * Guards against a drifted copy: every client asserts this same digest, so a hand-edited
   * artifact fails loudly instead of quietly grading itself.
   */
  private static final String ARTIFACT_SHA256 =
      "6aa0004dc3ed1f8b93645fd8bddd919596d4d770f4ae6509f85cd375a61b1046";

  private static final int EXPECTED_LAYOUT_CASES = 168;
  private static final int EXPECTED_VALUE_CASES = 630;

  private static byte[] artifactBytes;
  private static JsonNode artifact;

  @BeforeClass
  public static void loadArtifact() throws Exception {
    Path path = Paths.get(CanonicalNumberTest.class
        .getResource("/float-vectors/float-vectors.json").toURI());
    artifactBytes = Files.readAllBytes(path);
    artifact = new ObjectMapper().readTree(artifactBytes);
  }

  private static double asDouble(String hex) {
    return Double.longBitsToDouble(Long.parseUnsignedLong(hex, 16));
  }

  private static float asFloat(String hex) {
    return Float.intBitsToFloat(Integer.parseUnsignedInt(hex, 16));
  }

  // ---- artifact integrity -------------------------------------------------

  /**
   * A digest alone would not catch a well-formed artifact that had quietly lost its interesting
   * cases: every other test here would still pass. Coverage is therefore asserted directly.
   */
  @Test
  public void artifactIsTheOneWeExpect() throws Exception {
    StringBuilder digest = new StringBuilder();
    for (byte b : MessageDigest.getInstance("SHA-256").digest(artifactBytes)) {
      digest.append(String.format("%02x", b));
    }
    Assert.assertEquals(
        "float-vectors.json has changed; update ARTIFACT_SHA256 in all seven clients",
        ARTIFACT_SHA256, digest.toString());

    JsonNode layout = artifact.get("layout");
    JsonNode values = artifact.get("values");
    Assert.assertEquals(EXPECTED_LAYOUT_CASES, layout.size());
    Assert.assertEquals(EXPECTED_VALUE_CASES, values.size());

    int plain = 0;
    int scientific = 0;
    TreeSet<Integer> exponents = new TreeSet<>();
    for (JsonNode c : layout) {
      exponents.add(c.get("exponent").asInt());
      if (c.get("text").asText().indexOf('E') < 0) {
        plain++;
      } else {
        scientific++;
      }
    }
    Assert.assertTrue("layout must cover the plain branch", plain > 0);
    Assert.assertTrue("layout must cover the scientific branch", scientific > 0);
    for (int threshold : new int[] {-4, -3, 6, 7}) {
      Assert.assertTrue("layout must pin the threshold at exponent " + threshold,
          exponents.contains(threshold));
    }

    TreeSet<String> texts = new TreeSet<>();
    int doubles = 0;
    int floats = 0;
    int ties = 0;
    for (JsonNode c : values) {
      texts.add(c.get("text").asText());
      boolean isDouble = "f64".equals(c.get("type").asText());
      if (isDouble) {
        doubles++;
        double v = asDouble(c.get("bits").asText());
        if (Double.isFinite(v) && v != 0.0 && isTie(v)) {
          ties++;
        }
      } else {
        floats++;
      }
    }
    Assert.assertTrue("must cover both types", doubles > 0 && floats > 0);
    for (String special : new String[] {"NaN", "Infinity", "-Infinity", "0.0", "-0.0"}) {
      Assert.assertTrue("must cover the special value " + special, texts.contains(special));
    }
    Assert.assertTrue("must cover at least 20 exact ties, found " + ties, ties >= 20);
    // The 34 patterns where this contract deliberately differs from the JDK.
    for (String magnitude : new String[] {"5.0E-324", "1.0E-323", "5.0E-323", "6.0E-323",
        "7.0E-323", "8.0E-323", "9.0E-323", "1.0E-322",
        "1.0E-45", "3.0E-45", "4.0E-45", "6.0E-45", "8.0E-45", "1.0E-44", "3.0E-44",
        "4.0E-44", "1.0E-43"}) {
      Assert.assertTrue("must cover the divergence pattern " + magnitude,
          texts.contains(magnitude) && texts.contains("-" + magnitude));
    }
  }

  // ---- the artifact's expected text --------------------------------------

  @Test
  public void layoutMatchesTable() {
    List<String> failures = new ArrayList<>();
    for (JsonNode c : artifact.get("layout")) {
      CanonicalNumber.Decimal triple = new CanonicalNumber.Decimal(
          c.get("negative").asBoolean(), c.get("digits").asText(), c.get("exponent").asInt());
      String actual = CanonicalNumber.layout(triple);
      if (!c.get("text").asText().equals(actual)) {
        failures.add(String.format("(%s, %s, %d) -> %s, expected %s",
            c.get("negative").asText(), c.get("digits").asText(), c.get("exponent").asInt(),
            actual, c.get("text").asText()));
      }
    }
    Assert.assertEquals("", String.join("\n", failures));
  }

  @Test
  public void valuesMatchVectors() {
    List<String> failures = new ArrayList<>();
    for (JsonNode c : artifact.get("values")) {
      String hex = c.get("bits").asText();
      String expected = c.get("text").asText();
      String actual = "f64".equals(c.get("type").asText())
          ? CanonicalNumber.render(asDouble(hex))
          : CanonicalNumber.render(asFloat(hex));
      if (!expected.equals(actual)) {
        failures.add(c.get("type").asText() + " " + hex + " -> " + actual
            + ", expected " + expected);
      }
    }
    Assert.assertEquals("", String.join("\n", failures));
  }

  // ---- the contract, restated as properties -------------------------------

  @Test
  public void propertiesHoldOverArtifactAndGenerators() {
    List<String> failures = new ArrayList<>();
    for (JsonNode c : artifact.get("values")) {
      String hex = c.get("bits").asText();
      if ("f64".equals(c.get("type").asText())) {
        checkDouble(asDouble(hex), failures);
      } else {
        checkFloat(asFloat(hex), failures);
      }
    }
    Random r = new Random(4242);
    for (int i = 0; i < 20000; i++) {
      checkDouble(Double.longBitsToDouble(r.nextLong()), failures);
      checkFloat(Float.intBitsToFloat(r.nextInt()), failures);
    }
    // Dyadic rationals, where exact ties are dense; uniform sampling produces almost none.
    for (int j = 1; j <= 40; j++) {
      for (int i = 0; i < 200; i++) {
        checkDouble(Math.scalb((double) ((r.nextLong() >>> 11) | (1L << 52)), -j), failures);
        checkFloat(Math.scalb((float) (r.nextInt(1 << 20) + 1), -j), failures);
      }
    }
    for (int bits = 1; bits <= 4000; bits++) {
      checkFloat(Float.intBitsToFloat(bits), failures);
    }
    Assert.assertEquals("", String.join("\n", failures.subList(0, Math.min(20, failures.size()))));
  }

  private static void checkDouble(double v, List<String> failures) {
    if (!Double.isFinite(v) || v == 0.0) {
      return;
    }
    CanonicalNumber.Decimal chosen = CanonicalNumber.digitsOf(v);
    String text = CanonicalNumber.render(v);
    long wanted = Double.doubleToLongBits(v);

    if (Double.doubleToLongBits(Double.parseDouble(text)) != wanted) {
      failures.add("round-trip: " + v + " -> " + text);
    }
    if (chosen.digits.endsWith("0")) {
      failures.add("canonicality: " + v + " -> digits " + chosen.digits);
    }
    if (!CanonicalNumber.layout(chosen).equals(text)) {
      failures.add("layout purity: " + v);
    }
    BigDecimal exact = new BigDecimal(Math.abs(v));
    int length = chosen.digits.length();
    for (int shorter = 1; shorter < length; shorter++) {
      BigDecimal nearest = exact.round(new MathContext(shorter, RoundingMode.HALF_EVEN));
      if (Double.doubleToLongBits(Math.copySign(nearest.doubleValue(), v)) == wanted) {
        failures.add("minimality: " + v + " used " + length + " but " + shorter + " suffices");
        break;
      }
    }
    // Nearest, and ties to even, checked against the two neighbours rather than by re-deriving
    // with the same rounding mode.
    BigDecimal lo = exact.round(new MathContext(length, RoundingMode.FLOOR));
    BigDecimal hi = exact.round(new MathContext(length, RoundingMode.CEILING));
    BigDecimal picked = new BigDecimal(chosen.digits)
        .scaleByPowerOfTen(chosen.exponent - length + 1);
    int cmp = exact.subtract(lo).compareTo(hi.subtract(exact));
    BigDecimal expected;
    if (cmp < 0) {
      expected = lo;
    } else if (cmp > 0) {
      expected = hi;
    } else {
      expected = lo.unscaledValue().testBit(0) ? hi : lo;
    }
    if (picked.compareTo(expected) != 0) {
      failures.add((cmp == 0 ? "tie parity: " : "nearest: ") + v
          + " picked " + picked.toPlainString() + " expected " + expected.toPlainString());
    }
  }

  private static void checkFloat(float v, List<String> failures) {
    if (!Float.isFinite(v) || v == 0.0f) {
      return;
    }
    CanonicalNumber.Decimal chosen = CanonicalNumber.digitsOf(v);
    String text = CanonicalNumber.render(v);
    int wanted = Float.floatToIntBits(v);

    if (Float.floatToIntBits(Float.parseFloat(text)) != wanted) {
      failures.add("round-trip: " + v + " -> " + text);
    }
    if (chosen.digits.endsWith("0")) {
      failures.add("canonicality: " + v + " -> digits " + chosen.digits);
    }
    if (!CanonicalNumber.layout(chosen).equals(text)) {
      failures.add("layout purity: " + v);
    }
    BigDecimal exact = new BigDecimal((double) Math.abs(v));
    int length = chosen.digits.length();
    for (int shorter = 1; shorter < length; shorter++) {
      BigDecimal nearest = exact.round(new MathContext(shorter, RoundingMode.HALF_EVEN));
      if (Float.floatToIntBits(Math.copySign(nearest.floatValue(), v)) == wanted) {
        failures.add("minimality: " + v + " used " + length + " but " + shorter + " suffices");
        break;
      }
    }
    // Nearest, and ties to even, against the two neighbours rather than by re-deriving with
    // the same rounding mode. Rust and JavaScript both settle float ties the other way, so this
    // is the property most worth holding on the float side.
    BigDecimal lo = exact.round(new MathContext(length, RoundingMode.FLOOR));
    BigDecimal hi = exact.round(new MathContext(length, RoundingMode.CEILING));
    BigDecimal picked = new BigDecimal(chosen.digits)
        .scaleByPowerOfTen(chosen.exponent - length + 1);
    int cmp = exact.subtract(lo).compareTo(hi.subtract(exact));
    BigDecimal expected;
    if (cmp < 0) {
      expected = lo;
    } else if (cmp > 0) {
      expected = hi;
    } else {
      expected = lo.unscaledValue().testBit(0) ? hi : lo;
    }
    if (picked.compareTo(expected) != 0) {
      failures.add((cmp == 0 ? "tie parity (float): " : "nearest (float): ") + v
          + " picked " + picked.toPlainString() + " expected " + expected.toPlainString());
    }
  }

  private static boolean isTie(double v) {
    BigDecimal exact = new BigDecimal(Math.abs(v));
    String digits = exact.stripTrailingZeros().unscaledValue().abs().toString();
    int length = CanonicalNumber.digitsOf(v).digits.length();
    return digits.length() == length + 1 && digits.charAt(digits.length() - 1) == '5';
  }
}
