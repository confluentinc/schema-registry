package io.confluent.kafka.schemaregistry.client.rest.entities;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SchemaTest {

  @Test
  public void testCopyWithNullFields() {
    // Create schema with null fields
    Schema original = new Schema("subject1", 1, 100);
    original.setSchemaType(null);
    original.setReferences(null);
    original.setMetadata(null);
    original.setRuleSet(null);
    original.setSchema(null);
    original.setSchemaTags(null);

    // Copy the schema
    Schema copy = original.copy();

    // Verify null fields are handled correctly
    assertEquals(original.getSubject(), copy.getSubject());
    assertEquals(original.getVersion(), copy.getVersion());
    assertEquals(original.getId(), copy.getId());
    assertEquals(AvroSchema.TYPE, copy.getSchemaType()); // Default is AvroSchema.TYPE
    assertNotNull(copy.getReferences()); // Default is empty list
    assertTrue(copy.getReferences().isEmpty());
    assertNull(copy.getMetadata());
    assertNull(copy.getRuleSet());
    assertNull(copy.getSchema());
    assertNull(copy.getSchemaTags());

    // Verify copy is a different instance
    assertNotSame(original, copy);
  }

  @Test
  public void testCopyBasicFields() {
    // Create schema with basic fields
    Schema original = new Schema("subject1", 1, 100);
    original.setSchemaType("JSON");
    original.setSchema("{\"type\":\"string\"}");

    // Copy the schema
    Schema copy = original.copy();

    // Verify basic fields are copied correctly
    assertEquals(original.getSubject(), copy.getSubject());
    assertEquals(original.getVersion(), copy.getVersion());
    assertEquals(original.getId(), copy.getId());
    assertEquals(original.getSchemaType(), copy.getSchemaType());
    assertEquals(original.getSchema(), copy.getSchema());

    // Verify the strings are equal but separate objects
    original.setSchema("{\"type\":\"boolean\"}");
    assertNotSame(original.getSchema(), copy.getSchema());
    assertEquals("{\"type\":\"string\"}", copy.getSchema());
  }

  @Test
  public void testCopyReferences() {
    // Create schema with references
    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("ref1", "subject1", 1),
        new SchemaReference("ref2", "subject2", 2)
    );
    Schema original = new Schema("subject1", 1, 100);
    original.setReferences(references);

    // Copy the schema
    Schema copy = original.copy();

    // Verify references are copied correctly
    assertEquals(original.getReferences().size(), copy.getReferences().size());
    assertEquals(original.getReferences().get(0).getName(), copy.getReferences().get(0).getName());
    assertEquals(original.getReferences().get(1).getSubject(), copy.getReferences().get(1).getSubject());

    // Verify references are new instances (deep copy)
    assertNotSame(original.getReferences(), copy.getReferences());
    assertNotSame(original.getReferences().get(0), copy.getReferences().get(0));
    assertNotSame(original.getReferences().get(1), copy.getReferences().get(1));

    // Modify original reference to confirm deep copy
    original.getReferences().get(0).setVersion(100);
    assertEquals(1, copy.getReferences().get(0).getVersion().intValue());
  }

  @Test
  public void testCopyWithVersionAndId() {
    // Create original schema
    Schema original = new Schema("subject1", 1, 100);
    original.setSchemaType("AVRO");
    original.setSchema("{\"type\":\"string\"}");

    // Copy with new version and id
    Schema copy = original.copy(2, 200);

    // Verify new version and id are set
    assertEquals("subject1", copy.getSubject());
    assertEquals(Integer.valueOf(2), copy.getVersion());
    assertEquals(Integer.valueOf(200), copy.getId());
    assertEquals("AVRO", copy.getSchemaType());
    assertEquals("{\"type\":\"string\"}", copy.getSchema());

    // Original remains unchanged
    assertEquals(Integer.valueOf(1), original.getVersion());
    assertEquals(Integer.valueOf(100), original.getId());
  }

  @Test
  public void testMetadataImmutability() {
    // Create metadata
    Map<String, Set<String>> tags = new TreeMap<>();
    tags.put("tag1", new TreeSet<>(Arrays.asList("value1", "value2")));
    Map<String, String> properties = new TreeMap<>();
    properties.put("prop1", "value1");
    Set<String> sensitive = Collections.singleton("sensitive1");
    Metadata metadata = new Metadata(tags, properties, sensitive);

    // Verify metadata is immutable
    assertThrows(UnsupportedOperationException.class, () -> metadata.getTags().put("tag2", new TreeSet<>(Arrays.asList("tag2", "value1"))));
    assertThrows(UnsupportedOperationException.class, () -> metadata.getTags().get("tag1").add("value3"));
    assertThrows(UnsupportedOperationException.class, () -> metadata.getProperties().put("prop2", "value2"));
    assertThrows(UnsupportedOperationException.class, () -> metadata.getSensitive().add("sensitive2"));
  }
}