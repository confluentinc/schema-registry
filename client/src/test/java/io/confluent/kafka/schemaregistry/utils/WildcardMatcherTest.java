package io.confluent.kafka.schemaregistry.utils;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_TENANT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class WildcardMatcherTest {

  @Test
  public void testMatch() {
    assertFalse(WildcardMatcher.match(null, "Foo"));
    assertFalse(WildcardMatcher.match("Foo", null));
    assertTrue(WildcardMatcher.match(null, null));
    assertTrue(WildcardMatcher.match("Foo", "Foo"));
    assertTrue(WildcardMatcher.match("", ""));
    assertTrue(WildcardMatcher.match("", "*"));
    assertFalse(WildcardMatcher.match("", "?"));
    assertTrue(WildcardMatcher.match("Foo", "Fo*"));
    assertTrue(WildcardMatcher.match("Foo", "Fo?"));
    assertTrue(WildcardMatcher.match("Foo Bar and Catflap", "Fo*"));
    assertTrue(WildcardMatcher.match("New Bookmarks", "N?w ?o?k??r?s"));
    assertFalse(WildcardMatcher.match("Foo", "Bar"));
    assertTrue(WildcardMatcher.match("Foo Bar Foo", "F*o Bar*"));
    assertTrue(WildcardMatcher.match("Adobe Acrobat Installer", "Ad*er"));
    assertTrue(WildcardMatcher.match("Foo", "*Foo"));
    assertTrue(WildcardMatcher.match("BarFoo", "*Foo"));
    assertTrue(WildcardMatcher.match("Foo", "Foo*"));
    assertTrue(WildcardMatcher.match("FooBar", "Foo*"));
    assertFalse(WildcardMatcher.match("FOO", "*Foo"));
    assertFalse(WildcardMatcher.match("BARFOO", "*Foo"));
    assertFalse(WildcardMatcher.match("FOO", "Foo*"));
    assertFalse(WildcardMatcher.match("FOOBAR", "Foo*"));
  }
}
