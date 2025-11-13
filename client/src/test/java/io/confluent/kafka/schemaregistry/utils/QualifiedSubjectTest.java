/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.utils;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_TENANT;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.GLOBAL_CONTEXT_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class QualifiedSubjectTest {

  @Test
  public void testSimpleContextSubject() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_:.ctx1:subject1");
    assertEquals("tenant1", qs.getTenant());
    assertEquals(".ctx1", qs.getContext());
    assertEquals("subject1", qs.getSubject());
  }

  @Test
  public void testSubjectWithColon() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_:.ctx1:sub:ject1");
    assertEquals("tenant1", qs.getTenant());
    assertEquals(".ctx1", qs.getContext());
    assertEquals("sub:ject1", qs.getSubject());
  }

  @Test
  public void testSubjectWithQuote() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_:.ctx1:'sub''ject1'");
    assertEquals("tenant1", qs.getTenant());
    assertEquals(".ctx1", qs.getContext());
    assertEquals("'sub''ject1'", qs.getSubject());
  }

  @Test
  public void testContextWildcard() {
    // Treated as a subject since a leading dot is required for the context
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_:*:");
    assertEquals("tenant1", qs.getTenant());
    assertEquals("*", qs.getContext());
    assertEquals("", qs.getSubject());
  }

  @Test
  public void testInvalidContextSubject() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_:ctx1:subject1");
    assertEquals("tenant1", qs.getTenant());
    assertEquals(DEFAULT_CONTEXT, qs.getContext());
    assertEquals(":ctx1:subject1", qs.getSubject());
  }

  @Test
  public void testMissingAll() {
    QualifiedSubject qs = new QualifiedSubject("default", "");
    assertEquals(DEFAULT_TENANT, qs.getTenant());
    assertEquals(DEFAULT_CONTEXT, qs.getContext());
    assertEquals("", qs.getSubject());
  }

  @Test
  public void testSubjectOnly() {
    QualifiedSubject qs = new QualifiedSubject("default", "subject1");
    assertEquals(DEFAULT_TENANT, qs.getTenant());
    assertEquals(DEFAULT_CONTEXT, qs.getContext());
    assertEquals("subject1", qs.getSubject());
  }

  @Test
  public void testContextOnly() {
    QualifiedSubject qs = new QualifiedSubject("default", ":.ctx1:");
    assertEquals(DEFAULT_TENANT, qs.getTenant());
    assertEquals(".ctx1", qs.getContext());
    assertEquals("", qs.getSubject());
  }

  @Test
  public void testMissingTenant() {
    QualifiedSubject qs = new QualifiedSubject("default", ":.ctx1:subject1");
    assertEquals(DEFAULT_TENANT, qs.getTenant());
    assertEquals(".ctx1", qs.getContext());
    assertEquals("subject1", qs.getSubject());
  }

  @Test
  public void testDefaultNull() {
    QualifiedSubject qs = new QualifiedSubject("default", null);
    assertEquals(DEFAULT_TENANT, qs.getTenant());
    assertEquals(DEFAULT_CONTEXT, qs.getContext());
    assertEquals("", qs.getSubject());
  }

  @Test
  public void testAllNull() {
    QualifiedSubject qs = new QualifiedSubject(null, null);
    assertEquals(DEFAULT_TENANT, qs.getTenant());
    assertEquals(DEFAULT_CONTEXT, qs.getContext());
    assertEquals("", qs.getSubject());
  }

  @Test
  public void testTenantOnly() {
    // Treated as a subject since a leading dot is required for the context
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_::");
    assertEquals("tenant1", qs.getTenant());
    assertEquals(DEFAULT_CONTEXT, qs.getContext());
    assertEquals("::", qs.getSubject());
  }

  @Test
  public void testMissingContext() {
    // Treated as a subject since a leading dot is required for the context
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_::subject1");
    assertEquals("tenant1", qs.getTenant());
    assertEquals(DEFAULT_CONTEXT, qs.getContext());
    assertEquals("::subject1", qs.getSubject());
  }

  @Test
  public void testQualifiedContext() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_:.ctx1:");
    assertEquals("tenant1", qs.getTenant());
    assertEquals(".ctx1", qs.getContext());
    assertEquals("", qs.getSubject());
  }

  @Test
  public void testQualifiedContextMissingLastColon() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_:.ctx1");
    assertEquals("tenant1", qs.getTenant());
    assertEquals(".ctx1", qs.getContext());
    assertEquals("", qs.getSubject());
  }

  @Test
  public void testTenantWildcard() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", "tenant1_*");
    assertEquals("tenant1", qs.getTenant());
    assertEquals(DEFAULT_CONTEXT, qs.getContext());
    assertEquals("*", qs.getSubject());
  }

  @Test
  public void testToString() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", ".ctx1", "subject1");
    assertEquals("tenant1_:.ctx1:subject1", qs.toString());
  }

  @Test
  public void testToStringMissingAll() {
    QualifiedSubject qs = new QualifiedSubject(null, null, null);
    assertEquals("", qs.toString());
  }

  @Test
  public void testToStringSubjectOnly() {
    QualifiedSubject qs = new QualifiedSubject(null, null, "subject1");
    assertEquals("subject1", qs.toString());
  }

  @Test
  public void testToStringContextOnly() {
    QualifiedSubject qs = new QualifiedSubject(null, ".ctx1", null);
    assertEquals(":.ctx1:", qs.toString());
  }

  @Test
  public void testToStringMissingTenant() {
    QualifiedSubject qs = new QualifiedSubject(null, ".ctx1", "subject1");
    assertEquals(":.ctx1:subject1", qs.toString());
  }

  @Test
  public void testToStringTenantOnly() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", null, null);
    assertEquals("tenant1_", qs.toString());
  }

  @Test
  public void testToStringMissingContext() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", null, "subject1");
    assertEquals("tenant1_subject1", qs.toString());
  }

  @Test
  public void testToStringQualifiedContext() {
    QualifiedSubject qs = new QualifiedSubject("tenant1", ".ctx1", null);
    assertEquals("tenant1_:.ctx1:", qs.toString());
  }

  @Test
  public void testSubjectValidation() {
    assertTrue(QualifiedSubject.isValidSubject("default", "foo"));
    assertFalse(QualifiedSubject.isValidSubject("default", null));
    assertTrue(QualifiedSubject.isValidSubject("default", ""));
    assertFalse(QualifiedSubject.isValidSubject("default", String.valueOf((char) 31)));
    assertTrue(QualifiedSubject.isValidSubject("default", "  "));
    assertFalse(QualifiedSubject.isValidSubject("default", "__GLOBAL"));
    assertFalse(QualifiedSubject.isValidSubject("default", "__EMPTY"));
  }

  @Test
  public void testIsGlobalContext() {
    assertFalse(QualifiedSubject.isGlobalContext("default", ":" + DEFAULT_CONTEXT + ":foo"));
    assertFalse(QualifiedSubject.isGlobalContext("default", ":" + DEFAULT_CONTEXT + ":"));
    assertFalse(QualifiedSubject.isGlobalContext("default", "foo"));
    assertFalse(QualifiedSubject.isGlobalContext("default", ":" + GLOBAL_CONTEXT_NAME + ":foo"));
    assertTrue(QualifiedSubject.isGlobalContext("default", ":" + GLOBAL_CONTEXT_NAME + ":"));
    assertTrue(QualifiedSubject.isGlobalContext("default", ":" + GLOBAL_CONTEXT_NAME));
  }

  @Test
  public void testIsContext() {
    // Test null qualified subject - should return true (default context)
    assertTrue(QualifiedSubject.isContext("default", null));
    assertTrue(QualifiedSubject.isContext("tenant1", null));

    // Test empty string qualified subject - should return true (default context)
    assertTrue(QualifiedSubject.isContext("default", ""));
    assertTrue(QualifiedSubject.isContext("tenant1", "tenant1_"));

    // Test context with empty subject - should return true
    assertTrue(QualifiedSubject.isContext("default", ":.ctx1:"));
    assertTrue(QualifiedSubject.isContext("tenant1", "tenant1_:.ctx1:"));
    assertTrue(QualifiedSubject.isContext("default", ":.prod:"));
    assertTrue(QualifiedSubject.isContext("tenant1", "tenant1_:.prod:"));

    // Test context with missing last colon but empty subject - should return true
    assertTrue(QualifiedSubject.isContext("default", ":.ctx1"));
    assertTrue(QualifiedSubject.isContext("tenant1", "tenant1_:.ctx1"));

    // Test wildcard subject in non-default tenant - should return true
    assertTrue(QualifiedSubject.isContext("tenant1", "tenant1_*"));
    assertTrue(QualifiedSubject.isContext("tenant2", "tenant2_*"));

    // Test wildcard subject in default tenant - should return false
    assertFalse(QualifiedSubject.isContext("default", "*"));
    assertFalse(QualifiedSubject.isContext(DEFAULT_TENANT, "*"));

    // Test regular subjects - should return false
    assertFalse(QualifiedSubject.isContext("default", "subject1"));
    assertFalse(QualifiedSubject.isContext("tenant1", "tenant1_subject1"));
    assertFalse(QualifiedSubject.isContext("default", ":.ctx1:subject1"));
    assertFalse(QualifiedSubject.isContext("tenant1", "tenant1_:.ctx1:subject1"));

    // Test context wildcard (special context) - should return true (empty subject)
    assertTrue(QualifiedSubject.isContext("tenant1", "tenant1_:*:"));

    // Test invalid context formats with subjects - should return false
    assertFalse(QualifiedSubject.isContext("default", ":ctx1:subject1"));
    assertFalse(QualifiedSubject.isContext("tenant1", "tenant1_:ctx1:subject1"));
  }

  @Test
  public void testIsContextEdgeCases() {
    // Test with null tenant
    assertTrue(QualifiedSubject.isContext(null, null));
    assertTrue(QualifiedSubject.isContext(null, ""));
    assertTrue(QualifiedSubject.isContext(null, ":.ctx:"));

    // Wildcard with null tenant (treated as default tenant)
    assertFalse(QualifiedSubject.isContext(null, "*"));

    // Test with empty context (default context)
    assertTrue(QualifiedSubject.isContext("default", ":.:"));
    assertTrue(QualifiedSubject.isContext("tenant1", "tenant1_:.:"));

    // Test subjects that look like contexts but have content after
    assertFalse(QualifiedSubject.isContext("default", ":.ctx:foo"));
    assertFalse(QualifiedSubject.isContext("tenant1", "tenant1_:.ctx:foo"));

    // Test with special subjects that are not wildcards
    assertFalse(QualifiedSubject.isContext("tenant1", "tenant1_**"));
    assertFalse(QualifiedSubject.isContext("tenant1", "tenant1_*subject"));
    assertFalse(QualifiedSubject.isContext("default", "**"));
  }
}
