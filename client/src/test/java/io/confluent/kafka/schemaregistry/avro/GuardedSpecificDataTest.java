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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.FastReaderBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;

public class GuardedSpecificDataTest {

  @Test
  public void testModelStateStaysLive() {
    SpecificData model = new SpecificData();
    SpecificData guarded = new AvroSchemaUtils.GuardedSpecificData(model);

    Conversion<?> conversion = new Conversions.UUIDConversion();
    model.addLogicalTypeConversion(conversion);
    model.setFastReaderEnabled(!model.isFastReaderEnabled());
    model.setCustomCoders(!model.useCustomCoders());

    assertSame(conversion, guarded.getConversionFor(LogicalTypes.uuid()));
    assertEquals(model.isFastReaderEnabled(), guarded.isFastReaderEnabled());
    assertEquals(model.useCustomCoders(), guarded.useCustomCoders());
  }

  @Test
  public void testFastReaderSettingsStayLive() {
    SpecificData model = new SpecificData();
    FastReaderBuilder guarded = new AvroSchemaUtils.GuardedSpecificData(model)
        .getFastReaderBuilder();

    model.getFastReaderBuilder().withKeyClassEnabled(false).withClassPropEnabled(false);
    assertFalse(guarded.isKeyClassEnabled());
    assertFalse(guarded.isClassPropEnabled());

    guarded.withKeyClassEnabled(true).withClassPropEnabled(true);
    assertTrue(model.getFastReaderBuilder().isKeyClassEnabled());
    assertTrue(model.getFastReaderBuilder().isClassPropEnabled());
  }

  // Fails from Avro 1.12.2, whose getClass no longer initializes and only loads trusted classes
  // (org.apache.avro.SERIALIZABLE_*): GuardedSpecificData should defer to Avro there.
  @Test
  public void testResolvesTheSameClassesAsAvro() {
    Schema[] schemas = {
        record("io.confluent.kafka.schemaregistry.avro", "GuardedTopLevelRecord"),
        // Avro retries pkg.Outer.Inner as pkg.Outer$Inner.
        record(GuardedSpecificDataTest.class.getName(), "NestedRecord"),
        record("io.confluent.no.such", "Record"),
        // No name, so no class.
        Schema.createRecord(Collections.emptyList())
    };
    for (Schema schema : schemas) {
      assertSame(schema.getFullName(), new SpecificData().getClass(schema),
          new AvroSchemaUtils.GuardedSpecificData(new SpecificData()).getClass(schema));
    }
  }

  @Test
  public void testEveryInheritedMethodIsDelegatedOrVetted() {
    // None of these reads model state that can change (checked in the bytecode of Avro 1.11.5,
    // 1.12.1 and 1.12.2). A method a newer Avro adds fails here until it is listed or delegated.
    Set<String> vetted = new HashSet<>(Arrays.asList("compare", "compareMaps",
        "createDatumReader", "createDatumWriter", "createEnum", "createFixed", "createSchema",
        "createString", "deepCopy", "getArrayAsCollection", "getClassLoader", "getDefaultValue",
        "getEnumSchema",
        "getField", "getFixedSchema", "getNewRecordSupplier", "getPrimitiveTypeCache",
        "getProtocol", "getRecordSchema", "getRecordState", "getSchema", "getSchemaName",
        "hashCode", "hashCodeAdd", "induce", "instanceOf", "isArray", "isBoolean", "isBytes",
        "isDouble", "isEnum", "isFixed", "isFloat", "isInteger", "isLong", "isMap", "isRecord",
        "isString", "isStringable", "isStringType", "newArray", "newMap", "newRecord", "setField",
        "toString", "validate"));
    Set<String> unchecked = new TreeSet<>();
    for (Class<?> c : new Class<?>[] {GenericData.class, SpecificData.class}) {
      for (Method m : c.getDeclaredMethods()) {
        int mod = m.getModifiers();
        if ((Modifier.isPublic(mod) || Modifier.isProtected(mod)) && !Modifier.isStatic(mod)
            && !Modifier.isFinal(mod) && !m.isSynthetic() && !vetted.contains(m.getName())) {
          try {
            AvroSchemaUtils.GuardedSpecificData.class
                .getDeclaredMethod(m.getName(), m.getParameterTypes());
          } catch (NoSuchMethodException e) {
            unchecked.add(m.toString());
          }
        }
      }
    }
    assertEquals(Collections.emptySet(), unchecked);
  }

  @Test
  public void testCustomModelOfGeneratedClassIsKept() {
    assertSame(CustomModelRecord.MODEL$,
        AvroSchemaUtils.getSpecificDataForSchema(CustomModelRecord.SCHEMA$, false));
  }

  private static Schema record(String namespace, String name) {
    return SchemaBuilder.record(name).namespace(namespace).fields().endRecord();
  }

  public static class NestedRecord extends GuardedTopLevelRecord {
  }

  // As generated from a custom template that uses its own SpecificData subclass.
  public static class CustomModelRecord extends SpecificRecordBase {
    public static final Schema SCHEMA$ =
        record(GuardedSpecificDataTest.class.getName(), "CustomModelRecord");
    public static final SpecificData MODEL$ = new SpecificData() {
    };

    @Override
    public Schema getSchema() {
      return SCHEMA$;
    }

    @Override
    public Object get(int field) {
      return null;
    }

    @Override
    public void put(int field, Object value) {
    }
  }
}

class GuardedTopLevelRecord implements IndexedRecord {
  @Override
  public void put(int i, Object v) {
  }

  @Override
  public Object get(int i) {
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}
