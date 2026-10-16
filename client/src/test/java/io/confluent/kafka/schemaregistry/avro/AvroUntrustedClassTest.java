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

package io.confluent.kafka.schemaregistry.avro;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.BeforeClass;
import org.junit.Test;

// Runs in its own surefire execution, before any other test trusts or loads its class, so the
// class is untrusted (AVRO-4189); skipped in other executions.
public class AvroUntrustedClassTest {

  // A record named after a class on the classpath that Avro does not trust
  private static final Schema SCHEMA = SchemaBuilder.record(AvroUntrustedClassTest.class.getName())
      .fields().requiredString("f").endRecord();

  @BeforeClass
  public static void requireUntrustedFork() {
    assumeTrue(Boolean.getBoolean("avro.untrusted.classes"));
  }

  @Test
  public void untrustedClassIsRejectedByAvro() {
    // Guards against this test running in a JVM that trusts all packages
    assertThrows(SecurityException.class, () -> new SpecificData().getClass(SCHEMA));
  }

  @Test
  public void untrustedClassFallsBackToDefaultModel() {
    assertSame(AvroSchemaUtils.getSpecificData(),
        AvroSchemaUtils.getSpecificDataForSchema(SCHEMA, true));
    assertSame(SpecificData.get(), AvroSchemaUtils.getSpecificDataForSchema(SCHEMA, false));
  }

  @Test
  public void untrustedClassDoesNotBreakThreadLocalData() {
    try {
      AvroSchemaUtils.setThreadLocalData(SCHEMA, true, false);
      assertSame(AvroSchemaUtils.getSpecificData(), AvroSchemaUtils.getThreadLocalSpecificData());
    } finally {
      AvroSchemaUtils.clearThreadLocalData();
    }
  }

  // A hand-written specific record with its own model, not generated, so not trusted by default
  public static class Modeled extends SpecificRecordBase {
    public static final SpecificData MODEL$ = new SpecificData();

    @Override
    public Schema getSchema() {
      return null;
    }

    @Override
    public Object get(int field) {
      return null;
    }

    @Override
    public void put(int field, Object value) {
    }
  }

  @Test
  public void trustingAClassTakesEffectDespiteTheUntrustedCache() {
    Schema schema = SchemaBuilder.record("Modeled")
        .namespace(AvroUntrustedClassTest.class.getName()).fields().endRecord();
    // Remembered as untrusted
    assertSame(AvroSchemaUtils.getSpecificData(),
        AvroSchemaUtils.getSpecificDataForSchema(schema, true));
    assertSame(SpecificData.get(), AvroSchemaUtils.getSpecificDataForSchema(schema, false));
    try {
      AvroTrustedClasses.trustExactly(Modeled.class);
      assertSame(Modeled.MODEL$, AvroSchemaUtils.getSpecificDataForSchema(schema, true));
      assertSame(Modeled.MODEL$, AvroSchemaUtils.getSpecificDataForSchema(schema, false));
    } finally {
      AvroTrustedClasses.resetToDefaults();
    }
  }
}
