package io.confluent.kafka.schemaregistry.client.rest.entities;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import java.util.Set;
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
  public void testCopyMetadata() {
    // Create schema with metadata
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");

    // Create proper maps for metadata constructor
    Map<String, Set<String>> tags = new HashMap<>();
    tags.put("tag1", Collections.singleton("value1"));

    Metadata metadata = new Metadata(tags, properties, Collections.singleton("sensitive"));
    Schema original = new Schema("subject1", 1, 100);
    original.setMetadata(metadata);

    // Copy the schema
    Schema copy = original.copy();

    // Verify metadata is copied correctly
    assertEquals(original.getMetadata().getTags().size(), copy.getMetadata().getTags().size());
    assertEquals(original.getMetadata().getTags().get("tag1"), copy.getMetadata().getTags().get("tag1"));
    assertEquals(original.getMetadata().getSensitive(), copy.getMetadata().getSensitive());
    assertEquals(original.getMetadata().getProperties().size(), copy.getMetadata().getProperties().size());
    assertEquals(original.getMetadata().getProperties().get("key1"),
        copy.getMetadata().getProperties().get("key1"));

    // Verify metadata is a new instance (deep copy)
    assertNotSame(original.getMetadata(), copy.getMetadata());
    assertNotSame(original.getMetadata().getProperties(), copy.getMetadata().getProperties());
    assertNotSame(original.getMetadata().getTags(), copy.getMetadata().getTags());
    assertNotSame(original.getMetadata().getSensitive(), copy.getMetadata().getSensitive());
  }

  @Test
  public void testCopySchemaTags() {
    // Create schema with schema tags
    SchemaEntity entity = new SchemaEntity("path1", SchemaEntity.EntityType.SR_FIELD);
    List<String> tags = Arrays.asList("tag1", "tag2");
    SchemaTags schemaTags = new SchemaTags(entity, tags);
    Schema original = new Schema("subject1", 1, 100);
    original.setSchemaTags(Collections.singletonList(schemaTags));

    // Copy the schema
    Schema copy = original.copy();

    // Verify schema tags are copied correctly
    assertEquals(1, copy.getSchemaTags().size());
    assertEquals(original.getSchemaTags().get(0).getSchemaEntity().getEntityPath(),
        copy.getSchemaTags().get(0).getSchemaEntity().getEntityPath());
    assertEquals(original.getSchemaTags().get(0).getTags().size(),
        copy.getSchemaTags().get(0).getTags().size());
    assertEquals(original.getSchemaTags().get(0).getTags().get(0),
        copy.getSchemaTags().get(0).getTags().get(0));

    // Verify schema tags are new instances (deep copy)
    assertNotSame(original.getSchemaTags(), copy.getSchemaTags());
    assertNotSame(original.getSchemaTags().get(0), copy.getSchemaTags().get(0));
    assertNotSame(original.getSchemaTags().get(0).getTags(),
        copy.getSchemaTags().get(0).getTags());

    // Modify original schema tags to confirm deep copy
    original.getSchemaTags().get(0).getTags().set(0, "modified");
    assertEquals("tag1", copy.getSchemaTags().get(0).getTags().get(0));
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
}