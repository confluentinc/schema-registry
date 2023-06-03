package io.confluent.kafka.schemaregistry.utils;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_TENANT;
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
  }
}
