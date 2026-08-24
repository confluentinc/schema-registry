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

package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import dev.cel.common.CelVarDecl;
import dev.cel.common.types.SimpleType;
import dev.cel.runtime.CelEvaluationException;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils.RegexEngine;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils.ScriptType;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the {@code timestamp} constructor: the extension overloads
 * {@code timestamp(timestamp)} and {@code timestamp(int, int)} alongside the standard
 * {@code timestamp(string)} and {@code timestamp(int)}, over Proto and Avro schemas.
 */
public class CelValidatorTimestampTest {

  private static final String SCHEMA = "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"google/protobuf/timestamp.proto\";\n"
      + "message Event {\n"
      + "  google.protobuf.Timestamp created_at = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"notFuture\","
      + "             expr: \"timestamp(this) < now\"}]\n"
      + "  }];\n"
      + "}\n";

  private static DynamicMessage event(Instant when) {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    Descriptor desc = schema.toDescriptor("test.Event");
    Timestamp ts = Timestamp.newBuilder()
        .setSeconds(when.getEpochSecond())
        .setNanos(when.getNano())
        .build();
    return DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("created_at"), ts)
        .build();
  }

  @Test
  void pastTimestamp_passesNow() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), event(Instant.now().minusSeconds(60)));
    assertTrue(errors.isEmpty(), "Past timestamp should pass, got: " + errors);
  }

  @Test
  void futureTimestamp_failsNow() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), event(Instant.now().plusSeconds(3600)));
    assertEquals(1, errors.size());
    assertEquals("notFuture", errors.get(0).getRule().getName());
  }

  @Test
  void fromEpochMillis_unitArg() {
    // long field interpreted as epoch millis via timestamp(long, 3).
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int64 ts_ms = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"timestamp(this, 3) < now\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");

    // Past time → passes
    DynamicMessage past = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("ts_ms"),
            Instant.now().minusSeconds(60).toEpochMilli())
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), past).isEmpty());

    // Future time → fails
    DynamicMessage future = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("ts_ms"),
            Instant.now().plusSeconds(3600).toEpochMilli())
        .build();
    assertEquals(1, schema.validateMessage(new CelValidator(), future).size());
  }

  @Test
  void unknownPrecision_failsAtRuntime() {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int64 ts_ms = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"timestamp(this, 7) < now\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("ts_ms"), 1000L)
        .build();
    List<ValidationRuleError> errors = schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errors.size());
  }

  // ---- Avro timestamp logical type ----

  private static final String AVRO_TIMESTAMP_MILLIS_SCHEMA = ""
      + "{"
      + "  \"type\":\"record\","
      + "  \"name\":\"Event\","
      + "  \"namespace\":\"test\","
      + "  \"fields\":["
      + "    {"
      + "      \"name\":\"created_at\","
      + "      \"type\":{"
      + "        \"type\":\"long\","
      + "        \"logicalType\":\"timestamp-millis\""
      + "      },"
      + "      \"confluent:rules\":["
      + "        {\"name\":\"notFuture\","
      + "         \"expr\":\"timestamp(this) < now\"}]"
      + "    }"
      + "  ]"
      + "}";

  @Test
  void avroTimestampMillis_convertersOn_instantArrives_pastPasses() {
    // With useLogicalTypeConverters=true, the timestamp-millis field arrives
    // as an Instant, which TimestampUtils.toInstant passes straight through.
    AvroSchema schema = new AvroSchema(AVRO_TIMESTAMP_MILLIS_SCHEMA);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("created_at", Instant.now().minusSeconds(60));
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }

  @Test
  void avroTimestampMillis_convertersOn_futureFails() {
    AvroSchema schema = new AvroSchema(AVRO_TIMESTAMP_MILLIS_SCHEMA);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("created_at", Instant.now().plusSeconds(3600));
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), r);
    assertEquals(1, errs.size());
    assertEquals("notFuture", errs.get(0).getRule().getName());
  }

  @Test
  void avroPlainLong_needsThePrecisionArg() {
    // A plain long field carries no logical type, so nothing at the boundary can supply a
    // unit and the one-arg form would read it as epoch seconds. The two-arg overload is how
    // a rule says what the number means.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Event\","
        + "  \"namespace\":\"test\","
        + "  \"fields\":["
        + "    {"
        + "      \"name\":\"ts_ms\","
        + "      \"type\":\"long\","
        + "      \"confluent:rules\":["
        + "        {\"name\":\"r\","
        + "         \"expr\":\"timestamp(this, 3) < now\"}]"
        + "    }"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("ts_ms", Instant.now().minusSeconds(60).toEpochMilli());
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }

  /** Whether {@code needle} appears anywhere in the cause chain's messages. */
  private static boolean causeChainContains(Throwable t, String needle) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (cause.getMessage() != null && cause.getMessage().contains(needle)) {
        return true;
      }
    }
    return false;
  }

  @Test
  void avroTimestampMillis_convertersOff_schemaUnitIsApplied() {
    // avro.use.logical.type.converters defaults to false, so this value is a bare long whose
    // unit lives only in the schema. Applying it at the boundary makes the rule read the same
    // either way; without that, CEL would take the long for epoch seconds.
    AvroSchema schema = new AvroSchema(AVRO_TIMESTAMP_MILLIS_SCHEMA);
    GenericRecord past = new GenericData.Record(schema.rawSchema());
    past.put("created_at", Instant.now().minusSeconds(60).toEpochMilli());
    assertTrue(schema.validateMessage(new CelValidator(), past).isEmpty());

    GenericRecord future = new GenericData.Record(schema.rawSchema());
    future.put("created_at", Instant.now().plusSeconds(3600).toEpochMilli());
    assertEquals(1, schema.validateMessage(new CelValidator(), future).size());
  }

  @Test
  void avroTimestampMillis_convertersOff_isNotReadAsSeconds() {
    // 1700000000123 millis is 2023-11-14T22:13:20.123Z. Read as seconds it would be some
    // 55000 years on, so this equality is what distinguishes the two readings.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Event\","
        + "  \"namespace\":\"test\","
        + "  \"fields\":["
        + "    {"
        + "      \"name\":\"created_at\","
        + "      \"type\":{"
        + "        \"type\":\"long\","
        + "        \"logicalType\":\"timestamp-millis\""
        + "      },"
        + "      \"confluent:rules\":["
        + "        {\"name\":\"exact\","
        + "         \"expr\":\"timestamp(this) == timestamp(\\\""
        + "2023-11-14T22:13:20.123Z\\\")\"}]"
        + "    }"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("created_at", 1700000000123L);
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }

  @Test
  void avroTimestampMicros_convertersOff_schemaUnitIsApplied() {
    // Sub-millisecond precision survives: 1700000000123456 micros keeps the .123456.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Event\","
        + "  \"namespace\":\"test\","
        + "  \"fields\":["
        + "    {"
        + "      \"name\":\"created_at\","
        + "      \"type\":{"
        + "        \"type\":\"long\","
        + "        \"logicalType\":\"timestamp-micros\""
        + "      },"
        + "      \"confluent:rules\":["
        + "        {\"name\":\"exact\","
        + "         \"expr\":\"timestamp(this) == timestamp(\\\""
        + "2023-11-14T22:13:20.123456Z\\\")\"}]"
        + "    }"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("created_at", 1700000000123456L);
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }

  @Test
  void avroLocalTimestampMillis_convertersOff_isStillRefused() {
    // A local-timestamp value carries no zone, so it is refused rather than silently read at
    // UTC. Presenting it as a LocalDateTime is what routes it to that refusal instead of
    // letting a bare long through as an epoch value.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Event\","
        + "  \"namespace\":\"test\","
        + "  \"fields\":["
        + "    {"
        + "      \"name\":\"local_at\","
        + "      \"type\":{"
        + "        \"type\":\"long\","
        + "        \"logicalType\":\"local-timestamp-millis\""
        + "      },"
        + "      \"confluent:rules\":["
        + "        {\"name\":\"r\","
        + "         \"expr\":\"timestamp(this) < now\"}]"
        + "    }"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("local_at", 1700000000123L);
    List<ValidationRuleError> errors = schema.validateMessage(new CelValidator(), r);
    assertEquals(1, errors.size());
    assertTrue(causeChainContains(errors.get(0).getCause(), "local-timestamp"),
        "Expected the local-timestamp refusal, got: " + errors.get(0));
  }

  @Test
  void avroTimestampMillis_inNestedRecordAndArray_schemaUnitIsApplied() {
    // The boundary walk has to reach a timestamp nested in a record and inside an array, not
    // only a top-level field.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Outer\","
        + "  \"namespace\":\"test\","
        + "  \"confluent:rules\":["
        + "    {\"name\":\"r\","
        + "     \"expr\":\"timestamp(this.inner.at) == timestamp(\\\""
        + "2023-11-14T22:13:20.123Z\\\") && "
        + "timestamp(this.stamps[0]) == timestamp(\\\"2023-11-14T22:13:20.123Z\\\")\"}],"
        + "  \"fields\":["
        + "    {\"name\":\"inner\",\"type\":{"
        + "       \"type\":\"record\",\"name\":\"Inner\",\"fields\":["
        + "         {\"name\":\"at\",\"type\":{\"type\":\"long\","
        + "          \"logicalType\":\"timestamp-millis\"}}]}},"
        + "    {\"name\":\"stamps\",\"type\":{\"type\":\"array\",\"items\":{"
        + "       \"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}}"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    Schema raw = schema.rawSchema();
    GenericRecord inner = new GenericData.Record(raw.getField("inner").schema());
    inner.put("at", 1700000000123L);
    GenericRecord outer = new GenericData.Record(raw);
    outer.put("inner", inner);
    outer.put("stamps", Collections.singletonList(1700000000123L));
    assertTrue(schema.validateMessage(new CelValidator(), outer).isEmpty());
  }

  /**
   * Cross-client parity: an Avro timestamp logical type is usable as a timestamp with **no
   * constructor call at all**. The boundary applies the schema's unit, so the value is already a
   * CEL timestamp — comparable against {@code now}, and carrying the timestamp accessors. Every
   * one of the seven clients has this test; the wrapper is only needed for a plain numeric field
   * whose unit the schema cannot supply.
   */
  @Test
  void avroTimestampMillis_usableWithoutTheConstructor() {
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Event\","
        + "  \"namespace\":\"test\","
        + "  \"fields\":["
        + "    {"
        + "      \"name\":\"ts\","
        + "      \"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},"
        + "      \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"%s\"}]"
        + "    }"
        + "  ]"
        + "}";
    long past = Instant.now().minusSeconds(60).toEpochMilli();

    // Bare comparison against `now`, with the converters off (a raw long) and on (an Instant).
    for (Object value : new Object[] {past, Instant.ofEpochMilli(past)}) {
      AvroSchema schema = new AvroSchema(String.format(s, "this < now"));
      GenericRecord r = new GenericData.Record(schema.rawSchema());
      r.put("ts", value);
      assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty(),
          "bare `this < now` should hold for a past " + value.getClass().getSimpleName());
    }

    // Negative control: a future value must fail, so the comparison is really happening.
    AvroSchema future = new AvroSchema(String.format(s, "this < now"));
    GenericRecord fr = new GenericData.Record(future.rawSchema());
    fr.put("ts", Instant.now().plusSeconds(3600).toEpochMilli());
    assertEquals(1, future.validateMessage(new CelValidator(), fr).size());

    // The schema's millis unit is applied, not guessed: bare equality against a known instant.
    AvroSchema exact = new AvroSchema(
        String.format(s, "this == timestamp(\\\"2023-11-14T22:13:20.123Z\\\")"));
    GenericRecord er = new GenericData.Record(exact.rawSchema());
    er.put("ts", 1700000000123L);
    assertTrue(exact.validateMessage(new CelValidator(), er).isEmpty());

    // And the timestamp accessors work on it directly.
    AvroSchema accessor = new AvroSchema(String.format(s, "this.getFullYear() == 2023"));
    GenericRecord ar = new GenericData.Record(accessor.rawSchema());
    ar.put("ts", 1700000000123L);
    assertTrue(accessor.validateMessage(new CelValidator(), ar).isEmpty());
  }

  @Test
  void avroMultiBranchUnion_resolvesTheBranchTheValueTook() {
    // A legal multi-branch union whose arms are a timestamp-millis long AND a plain int. An
    // Integer belongs to the int arm, so it must NOT be read as a timestamp; picking the first
    // non-null branch instead would convert 5 to 1970-01-01T00:00:00.005Z.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Event\","
        + "  \"namespace\":\"test\","
        + "  \"confluent:rules\":["
        + "    {\"name\":\"r\",\"expr\":\"this.either == 5\"}],"
        + "  \"fields\":["
        + "    {\"name\":\"either\",\"type\":["
        + "       {\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},\"int\"]}"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    GenericRecord asInt = new GenericData.Record(schema.rawSchema());
    asInt.put("either", 5);
    assertTrue(schema.validateMessage(new CelValidator(), asInt).isEmpty(),
        "an int-arm value must stay an int");

    // The long arm of the same union is still normalized to a timestamp.
    String tsRule = s.replace("this.either == 5",
        "timestamp(this.either) == timestamp(\\\"2023-11-14T22:13:20.123Z\\\")");
    AvroSchema tsSchema = new AvroSchema(tsRule);
    GenericRecord asLong = new GenericData.Record(tsSchema.rawSchema());
    asLong.put("either", 1700000000123L);
    assertTrue(tsSchema.validateMessage(new CelValidator(), asLong).isEmpty(),
        "a long-arm value must be read at the schema's millis unit");
  }

  @Test
  void avroTimestampMicros_convertersOn_instantArrives() {
    // Same as millis but with the micros logical type. With converters on,
    // Instant arrives regardless of underlying precision.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Event\","
        + "  \"namespace\":\"test\","
        + "  \"fields\":["
        + "    {"
        + "      \"name\":\"created_at\","
        + "      \"type\":{"
        + "        \"type\":\"long\","
        + "        \"logicalType\":\"timestamp-micros\""
        + "      },"
        + "      \"confluent:rules\":["
        + "        {\"name\":\"r\","
        + "         \"expr\":\"timestamp(this) < now\"}]"
        + "    }"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("created_at", Instant.now().minusSeconds(60));
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }

  // ---- stdlib timestamp(int): bare ints are epoch SECONDS ----

  /** Evaluate a variable-free expression under both regex engines, asserting they agree. */
  private static Object eval(String expr) throws Exception {
    Object result = null;
    for (RegexEngine engine : RegexEngine.values()) {
      Object current = CelUtils
          .buildProgram(ScriptType.JSON, expr, null, Collections.emptyList(), engine)
          .eval();
      if (result == null) {
        result = current;
      } else {
        assertEquals(result, current, expr + " differed across regex engines");
      }
    }
    return result;
  }

  /**
   * Pins the cross-client contract: the stdlib one-arg {@code timestamp(int)} conversion treats a
   * bare integer as Unix epoch <em>seconds</em>, never millis. cel-java declares this as
   * {@code int64_to_timestamp} ("Type conversion of integers as Unix epoch seconds to
   * timestamps") and gates it on {@code CelOptions.enableTimestampEpoch}, which
   * {@code CelOptions.current()} — and therefore {@code CelOptions.DEFAULT}, which
   * {@link CelUtils} derives its options from — sets to true. All seven Schema Registry clients
   * agree on seconds here; this test is the Java anchor for that.
   */
  @Test
  void bareIntToTimestamp_isEpochSeconds() throws Exception {
    assertEquals("2023-11-14T22:13:20Z", eval("string(timestamp(1700000000))"));
    assertEquals(Boolean.TRUE,
        eval("timestamp(1700000000) == timestamp(\"2023-11-14T22:13:20Z\")"));
    // Not millis: if the int were read as millis this would be 1970-01-20T16:13:20Z.
    assertEquals(Boolean.FALSE,
        eval("timestamp(1700000000) == timestamp(\"1970-01-20T16:13:20Z\")"));
    // Epoch itself, and one second past it.
    assertEquals("1970-01-01T00:00:00Z", eval("string(timestamp(0))"));
    assertEquals("1970-01-01T00:00:01Z", eval("string(timestamp(1))"));
  }

  /** Evaluate {@code expr} with {@code x} declared dyn and bound to {@code value}. */
  private static Object evalWithDyn(String expr, Object value) throws Exception {
    return CelUtils.buildProgram(ScriptType.JSON, expr, null,
            Collections.singletonList(CelVarDecl.newVarDeclaration("x", SimpleType.DYN)))
        .eval(Collections.singletonMap("x", value));
  }

  /**
   * The dispatch that the extension overload has to keep unambiguous. A dyn argument is
   * assignable to string, int and timestamp alike, so the checker lists all three of
   * {@code timestamp}'s overload ids and cel-java picks between them by the runtime value's
   * Java class. That only resolves because the three bindings take disjoint classes — String,
   * Long and Temporal. Anything wider on our side (Object) would match a String or a Long too
   * and every one of these calls would fail with AMBIGUOUS_OVERLOAD.
   */
  @Test
  void timestampOfDyn_dispatchesByRuntimeType() throws Exception {
    // String → stdlib's RFC 3339 parse.
    assertEquals(Boolean.TRUE,
        evalWithDyn("timestamp(x) == timestamp(\"2023-11-14T22:13:20Z\")",
            "2023-11-14T22:13:20Z"));
    // Long → stdlib's epoch seconds.
    assertEquals(Boolean.TRUE,
        evalWithDyn("timestamp(x) == timestamp(\"2023-11-14T22:13:20Z\")", 1700000000L));
    // Instant → our Temporal overload, passing it through.
    assertEquals(Boolean.TRUE,
        evalWithDyn("timestamp(x) == timestamp(\"2023-11-14T22:13:20Z\")",
            Instant.ofEpochSecond(1700000000L)));
  }

  /**
   * The temporal shapes beyond Instant. The standard identity overload binds Instant alone, so
   * without the extension overload shadowing it these would have no matching overload.
   */
  @Test
  void timestampOfDyn_acceptsOffsetAndZonedDateTime() throws Exception {
    assertEquals(Boolean.TRUE,
        evalWithDyn("timestamp(x) == timestamp(\"2023-11-14T22:13:20Z\")",
            OffsetDateTime.ofInstant(Instant.ofEpochSecond(1700000000L), ZoneOffset.ofHours(5))));
    assertEquals(Boolean.TRUE,
        evalWithDyn("timestamp(x) == timestamp(\"2023-11-14T22:13:20Z\")",
            ZonedDateTime.ofInstant(Instant.ofEpochSecond(1700000000L), ZoneOffset.UTC)));
  }

  /** A LocalDateTime carries no zone, so it is refused rather than read at some guessed one. */
  @Test
  void timestampOfDyn_refusesLocalDateTime() {
    CelEvaluationException e = assertThrows(CelEvaluationException.class,
        () -> evalWithDyn("timestamp(x) == timestamp(0)", LocalDateTime.of(2023, 11, 14, 22, 13, 20)));
    assertTrue(causeChainContains(e, "local-timestamp"),
        "Expected the local-timestamp refusal, got: " + e);
  }

  /** All four precisions of the two-arg constructor, and the rejection outside {0, 3, 6, 9}. */
  @Test
  void twoArgPrecisions() throws Exception {
    assertEquals(Boolean.TRUE,
        eval("timestamp(1700000000, 0) == timestamp(\"2023-11-14T22:13:20Z\")"));
    assertEquals(Boolean.TRUE,
        eval("timestamp(1700000000123, 3) == timestamp(\"2023-11-14T22:13:20.123Z\")"));
    assertEquals(Boolean.TRUE,
        eval("timestamp(1700000000123456, 6) == timestamp(\"2023-11-14T22:13:20.123456Z\")"));
    assertEquals(Boolean.TRUE,
        eval("timestamp(1700000000123456789, 9) "
            + "== timestamp(\"2023-11-14T22:13:20.123456789Z\")"));

    CelEvaluationException e = assertThrows(CelEvaluationException.class,
        () -> eval("timestamp(1700000000, 4) == timestamp(0)"));
    assertTrue(causeChainContains(e, "Unknown timestamp precision"),
        "Expected the precision rejection, got: " + e);
  }

  /** Negative bare ints are pre-epoch seconds, not an error and not millis. */
  @Test
  void negativeBareIntToTimestamp_isPreEpochSeconds() throws Exception {
    assertEquals("1969-12-31T23:59:59Z", eval("string(timestamp(-1))"));
    assertEquals("1969-12-31T00:00:00Z", eval("string(timestamp(-86400))"));
    assertEquals(Boolean.TRUE,
        eval("timestamp(-86400) == timestamp(\"1969-12-31T00:00:00Z\")"));
  }

  /**
   * The two-arg form is unaffected by the epoch-seconds contract above: it honors the explicit
   * precision, so the same integer means different instants through the two arities. Keeps the
   * two entry points distinguishable now that they share a name.
   */
  @Test
  void twoArgPrecision_isUnaffectedByEpochSecondsContract() throws Exception {
    // Same instant as timestamp(1700000000), reached with an explicit millis value.
    assertEquals("2023-11-14T22:13:20Z", eval("string(timestamp(1700000000000, 3))"));
    assertEquals(Boolean.TRUE,
        eval("timestamp(1700000000000, 3) == timestamp(1700000000)"));
    // The same integer differs between the two surfaces: seconds vs. millis.
    assertEquals("1970-01-20T16:13:20Z", eval("string(timestamp(1700000000, 3))"));
    assertEquals(Boolean.FALSE,
        eval("timestamp(1700000000, 3) == timestamp(1700000000)"));
    // "seconds" explicitly agrees with the bare-int conversion.
    assertEquals(Boolean.TRUE,
        eval("timestamp(1700000000, 0) == timestamp(1700000000)"));
  }
}
