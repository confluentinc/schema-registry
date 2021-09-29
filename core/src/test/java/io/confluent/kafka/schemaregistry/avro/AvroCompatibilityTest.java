/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.confluent.kafka.schemaregistry.avro;

import org.apache.avro.Schema;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import io.confluent.kafka.schemaregistry.CompatibilityChecker;

public class AvroCompatibilityTest {

  private final String schemaString1 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"}]}";
  private final AvroSchema schema1 = AvroUtils.parseSchema(schemaString1);
  
  private final String schemaString2 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"},"
      + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}";
  private final AvroSchema schema2 = AvroUtils.parseSchema(schemaString2);
  
  private final String schemaString3 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"},"
      + " {\"type\":\"string\",\"name\":\"f2\"}]}";
  private final AvroSchema schema3 = AvroUtils.parseSchema(schemaString3);

  private final String schemaString4 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1_new\", \"aliases\": [\"f1\"]}]}";
  private final AvroSchema schema4 = AvroUtils.parseSchema(schemaString4);
  
  private final String schemaString6 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":[\"null\", \"string\"],\"name\":\"f1\","
      + " \"doc\":\"doc of f1\"}]}";
  private final AvroSchema schema6 = AvroUtils.parseSchema(schemaString6);
  
  private final String schemaString7 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":[\"null\", \"string\", \"int\"],\"name\":\"f1\","
      + " \"doc\":\"doc of f1\"}]}";
  private final AvroSchema schema7 = AvroUtils.parseSchema(schemaString7);

  private final String schemaString8 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"},"
      + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]},"
      + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"bar\"}]}";
  private final AvroSchema schema8 = AvroUtils.parseSchema(schemaString8);

  private final String badDefaultNullString = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":[\"null\", \"string\"],\"name\":\"f1\", \"default\": \"null\"},"
      + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"},"
      + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"bar\"}]}";


  @Test
  public void testBadDefaultNull() {
    assertNotNull(AvroUtils.parseSchema(badDefaultNullString));
  }

  /*
   * Backward compatibility: A new schema is backward compatible if it can be used to read the data
   * written in the previous schema.
   */
  @Test
  public void testBasicBackwardsCompatibility() {
    CompatibilityChecker checker = CompatibilityChecker.BACKWARD_CHECKER;
    assertTrue("adding a field with default is a backward compatible change",
               checker.isCompatible(schema2, Collections.singletonList(schema1)).isEmpty());
    assertFalse("adding a field w/o default is not a backward compatible change",
                checker.isCompatible(schema3, Collections.singletonList(schema1)).isEmpty());
    assertTrue("changing field name with alias is a backward compatible change",
                checker.isCompatible(schema4, Collections.singletonList(schema1)).isEmpty());
    assertTrue("evolving a field type to a union is a backward compatible change",
               checker.isCompatible(schema6, Collections.singletonList(schema1)).isEmpty());
    assertFalse("removing a type from a union is not a backward compatible change",
                checker.isCompatible(schema1, Collections.singletonList(schema6)).isEmpty());
    assertTrue("adding a new type in union is a backward compatible change",
               checker.isCompatible(schema7, Collections.singletonList(schema6)).isEmpty());
    assertFalse("removing a type from a union is not a backward compatible change",
                checker.isCompatible(schema6, Collections.singletonList(schema7)).isEmpty());
    
    // Only schema 2 is checked
    assertTrue("removing a default is not a transitively compatible change",
        checker.isCompatible(schema3, Arrays.asList(schema1, schema2)).isEmpty());
  }
  
  /*
   * Backward transitive compatibility: A new schema is backward compatible if it can be used to read the data
   * written in all previous schemas.
   */
  @Test
  public void testBasicBackwardsTransitiveCompatibility() {
    CompatibilityChecker checker = CompatibilityChecker.BACKWARD_TRANSITIVE_CHECKER;
    // All compatible
    assertTrue("iteratively adding fields with defaults is a compatible change",
        checker.isCompatible(schema8, Arrays.asList(schema1, schema2)).isEmpty());
    
    // 1 == 2, 2 == 3, 3 != 1
    assertTrue("adding a field with default is a backward compatible change",
        checker.isCompatible(schema2, Collections.singletonList(schema1)).isEmpty());
    assertTrue("removing a default is a compatible change, but not transitively",
        checker.isCompatible(schema3, Arrays.asList(schema2)).isEmpty());
    assertFalse("removing a default is not a transitively compatible change",
        checker.isCompatible(schema3, Arrays.asList(schema2, schema1)).isEmpty());
  }
  
  /*
   * Forward compatibility: A new schema is forward compatible if the previous schema can read data written in this
   * schema.
   */
  @Test
  public void testBasicForwardsCompatibility() {
    CompatibilityChecker checker = CompatibilityChecker.FORWARD_CHECKER;
    assertTrue("adding a field is a forward compatible change",
        checker.isCompatible(schema2, Collections.singletonList(schema1)).isEmpty());
    assertTrue("adding a field is a forward compatible change",
        checker.isCompatible(schema3, Collections.singletonList(schema1)).isEmpty());
    assertTrue("adding a field is a forward compatible change",
        checker.isCompatible(schema3, Collections.singletonList(schema2)).isEmpty());
    assertTrue("adding a field is a forward compatible change",
        checker.isCompatible(schema2, Collections.singletonList(schema3)).isEmpty());
    
    // Only schema 2 is checked
    assertTrue("removing a default is not a transitively compatible change",
        checker.isCompatible(schema1, Arrays.asList(schema3, schema2)).isEmpty());
  }
  
  /*
   * Forward transitive compatibility: A new schema is forward compatible if all previous schemas can read data written
   * in this schema.
   */
  @Test
  public void testBasicForwardsTransitiveCompatibility() {
    CompatibilityChecker checker = CompatibilityChecker.FORWARD_TRANSITIVE_CHECKER;
    // All compatible
    assertTrue("iteratively removing fields with defaults is a compatible change",
        checker.isCompatible(schema1, Arrays.asList(schema8, schema2)).isEmpty());
    
    // 1 == 2, 2 == 3, 3 != 1
    assertTrue("adding default to a field is a compatible change",
        checker.isCompatible(schema2, Collections.singletonList(schema3)).isEmpty());
    assertTrue("removing a field with a default is a compatible change",
        checker.isCompatible(schema1, Arrays.asList(schema2)).isEmpty());
    assertFalse("removing a default is not a transitively compatible change",
        checker.isCompatible(schema1, Arrays.asList(schema2, schema3)).isEmpty());
  }
  
  /*
   * Full compatibility: A new schema is fully compatible if it’s both backward and forward compatible.
   */
  @Test
  public void testBasicFullCompatibility() {
    CompatibilityChecker checker = CompatibilityChecker.FULL_CHECKER;
    assertTrue("adding a field with default is a backward and a forward compatible change",
        checker.isCompatible(schema2, Collections.singletonList(schema1)).isEmpty());
    
    // Only schema 2 is checked!
    assertTrue("transitively adding a field without a default is not a compatible change",
        checker.isCompatible(schema3, Arrays.asList(schema1, schema2)).isEmpty());
    // Only schema 2 is checked!
    assertTrue("transitively removing a field without a default is not a compatible change",
        checker.isCompatible(schema1, Arrays.asList(schema3, schema2)).isEmpty());
  }
  
  /*
   * Full transitive compatibility: A new schema is fully compatible if it’s both transitively backward
   * and transitively forward compatible with the entire schema history.
   */
  @Test
  public void testBasicFullTransitiveCompatibility() {
    CompatibilityChecker checker = CompatibilityChecker.FULL_TRANSITIVE_CHECKER;
    
    // Simple check
    assertTrue("iteratively adding fields with defaults is a compatible change",
        checker.isCompatible(schema8, Arrays.asList(schema1, schema2)).isEmpty());
    assertTrue("iteratively removing fields with defaults is a compatible change",
        checker.isCompatible(schema1, Arrays.asList(schema8, schema2)).isEmpty());
    
    assertTrue("adding default to a field is a compatible change",
        checker.isCompatible(schema2, Collections.singletonList(schema3)).isEmpty());
    assertTrue("removing a field with a default is a compatible change",
        checker.isCompatible(schema1, Arrays.asList(schema2)).isEmpty());
    
    assertTrue("adding a field with default is a compatible change",
        checker.isCompatible(schema2, Collections.singletonList(schema1)).isEmpty());
    assertTrue("removing a default from a field compatible change",
        checker.isCompatible(schema3, Arrays.asList(schema2)).isEmpty());

    assertFalse("transitively adding a field without a default is not a compatible change",
        checker.isCompatible(schema3, Arrays.asList(schema2, schema1)).isEmpty());
    assertFalse("transitively removing a field without a default is not a compatible change",
        checker.isCompatible(schema1, Arrays.asList(schema2, schema3)).isEmpty());
  }
  
}
