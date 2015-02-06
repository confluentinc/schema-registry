/**
 * Copyright 2014 Confluent Inc.
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

import org.apache.avro.Schema;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AvroCompatibilityTest {

  @Test
  public void testBasicCompatibility() {
    String schemaString1 = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":\"f1\"}]}";
    Schema schema1 = AvroUtils.parseSchema(schemaString1).schemaObj;

    String schemaString2 = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":\"f1\"},"
                           + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}";
    Schema schema2 = AvroUtils.parseSchema(schemaString2).schemaObj;

    String schemaString3 = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":\"f1\"},"
                           + " {\"type\":\"string\",\"name\":\"f2\"}]}";
    Schema schema3 = AvroUtils.parseSchema(schemaString3).schemaObj;

    String schemaString4 = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":\"string\",\"name\":\"f1_new\", \"aliases\": [\"f1\"]}]}";
    Schema schema4 = AvroUtils.parseSchema(schemaString4).schemaObj;

    String schemaString6 = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":[\"null\", \"string\"],\"name\":\"f1\","
                           + " \"doc\":\"doc of f1\"}]}";
    Schema schema6 = AvroUtils.parseSchema(schemaString6).schemaObj;

    String schemaString7 = "{\"type\":\"record\","
                           + "\"name\":\"myrecord\","
                           + "\"fields\":"
                           + "[{\"type\":[\"null\", \"string\", \"int\"],\"name\":\"f1\","
                           + " \"doc\":\"doc of f1\"}]}";
    Schema schema7 = AvroUtils.parseSchema(schemaString7).schemaObj;

    AvroCompatibilityChecker backwardChecker = AvroCompatibilityChecker.BACKWARD_CHECKER;
    assertTrue("adding a field with default is a backward compatible change",
               backwardChecker.isCompatible(schema2, schema1));
    assertFalse("adding a field w/o default is not a backward compatible change",
                backwardChecker.isCompatible(schema3, schema1));
    assertFalse("changing field name is not a backward compatible change",
                backwardChecker.isCompatible(schema4, schema1));
    assertTrue("evolving a field type to a union is a backward compatible change",
               backwardChecker.isCompatible(schema6, schema1));
    assertFalse("removing a type from a union is not a backward compatible change",
                backwardChecker.isCompatible(schema1, schema6));
    assertTrue("adding a new type in union is a backward compatible change",
               backwardChecker.isCompatible(schema7, schema6));
    assertFalse("removing a type from a union is not a backward compatible change",
                backwardChecker.isCompatible(schema6, schema7));

    AvroCompatibilityChecker forwardChecker = AvroCompatibilityChecker.FORWARD_CHECKER;
    assertTrue("adding a field is a forward compatible change",
               forwardChecker.isCompatible(schema2, schema1));

    AvroCompatibilityChecker fullChecker = AvroCompatibilityChecker.FULL_CHECKER;
    assertTrue("adding a field with default is a backward and a forward compatible change",
               fullChecker.isCompatible(schema2, schema1));
  }
}
