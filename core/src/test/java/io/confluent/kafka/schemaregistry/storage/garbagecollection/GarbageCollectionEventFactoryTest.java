/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage.garbagecollection;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.GarbageCollectionEvent;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.entities.WireEvent;
import io.confluent.kafka.schemaregistry.storage.garbagecollection.serde.MetadataChangeDeserializer;
import io.confluent.protobuf.events.catalog.v1.MetadataChange;
import io.confluent.protobuf.events.catalog.v1.OpType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class GarbageCollectionEventFactoryTest {

  // TestingCloudEvent allows setting null values for testing validation
  private static class TestingCloudEvent implements CloudEvent {
    Map<String, Object> properties = new HashMap<>();
    Map<String, Object> extensions = new HashMap<>();

    public void setProperty(String key, Object value) {
      properties.put(key, value);
    }

    @Override
    public CloudEventData getData() {
      Object data = properties.get("data");
      if (data == null) {
        return null;
      }
      if (data instanceof CloudEventData) {
        return (CloudEventData) data;
      }
      // Wrap byte[] as CloudEventData
      final byte[] bytes = (byte[]) data;
      return new CloudEventData() {
        @Override
        public byte[] toBytes() {
          return bytes;
        }
      };
    }

    @Override
    public SpecVersion getSpecVersion() {
      return SpecVersion.V1;
    }

    @Override
    public String getId() {
      return (String) properties.get("id");
    }

    @Override
    public String getType() {
      return (String) properties.get("type");
    }

    @Override
    public URI getSource() {
      return (URI) properties.get("source");
    }

    @Override
    public String getDataContentType() {
      return (String) properties.get("datacontenttype");
    }

    @Override
    public URI getDataSchema() {
      return null;
    }

    @Override
    public String getSubject() {
      return (String) properties.get("subject");
    }

    @Override
    public OffsetDateTime getTime() {
      return (OffsetDateTime) properties.get("time");
    }

    @Override
    public Object getAttribute(String attributeName) {
      return null;
    }

    @Override
    public Object getExtension(String extensionName) {
      return this.extensions.get(extensionName);
    }

    @Override
    public Set<String> getExtensionNames() {
      return extensions.keySet();
    }

    public void setExtensions(Map<String, Object> extensions) {
      this.extensions = (extensions != null) ? new HashMap<>(extensions) : new HashMap<>();
    }
  }

  @Mock
  private MetadataChangeDeserializer mockDeserializer;

  private GarbageCollectionEventFactory factory;

  private static final String TEST_ID = "test-id-123";
  private static final String TEST_TYPE = "io.confluent.catalog.v1.metadata.change";
  private static final String TEST_SUBJECT = "topic";
  private static final String TEST_TENANT = "lsrc-test";
  private static final String TEST_SOURCE = "lkc-123";
  private static final String PROTOBUF_CONTENT_TYPE = "application/protobuf";
  private static final String JSON_CONTENT_TYPE = "application/json";

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    factory = new GarbageCollectionEventFactory(mockDeserializer);
  }

  @Test
  public void testCreateFromValid_DeltaEvent() {
    // Arrange
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.DELETE)
        .setSource(TEST_SOURCE)
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
        .thenReturn(metadataChange);

    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act
    GarbageCollectionEvent result = factory.createFrom(wireEvent);

    // Assert
    assertNotNull(result);
    assertEquals(TEST_ID, result.getId());
    assertEquals(TEST_TYPE, result.getType());
    assertEquals(TEST_SUBJECT, result.getSubject());
    assertEquals(TEST_TENANT, result.getTenant());
    assertEquals(metadataChange, result.getMetadataChange());
    assertNull("Delta events should not have snapshotId", result.getSnapshotId());
    assertNull("Delta events should not have page", result.getPage());
    assertNull("Delta events should not have totalPages", result.getTotalPages());
    assertNull("Delta events should not have isLastPage", result.isLastPage());
  }

  @Test
  public void testCreateFromValid_SnapshotEvent() {
    // Arrange
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.SNAPSHOT)
        .setSource(TEST_SOURCE)
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
        .thenReturn(metadataChange);

    Map<String, Object> extensions = new HashMap<>();
    extensions.put("lsrc", TEST_TENANT);
    extensions.put("snapshotid", "snapshot-123");
    extensions.put("page", "0");
    extensions.put("total", "5");
    extensions.put("lastpage", "false");

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        extensions
    );

    WireEvent wireEvent = new WireEvent(new HashMap<>(), cloudEvent);

    // Act
    GarbageCollectionEvent result = factory.createFrom(wireEvent);

    // Assert
    assertNotNull(result);
    assertEquals(TEST_ID, result.getId());
    assertEquals(TEST_TYPE, result.getType());
    assertEquals(TEST_SUBJECT, result.getSubject());
    assertEquals(TEST_TENANT, result.getTenant());
    assertEquals("snapshot-123", result.getSnapshotId());
    assertEquals(Integer.valueOf(0), result.getPage());
    assertEquals(Integer.valueOf(5), result.getTotalPages());
    assertEquals(Boolean.FALSE, result.isLastPage());
    assertEquals(metadataChange, result.getMetadataChange());
  }

  @Test
  public void testCreateFrom_NullWireEvent() {
    // Act & Assert
    try {
      factory.createFrom(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_MissingSubject() {
    // Arrange
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        null,  // missing subject
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_MissingType() {
    // Arrange
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        null,  // missing type
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException or NullPointerException");
    } catch (Exception e) {
      // Either IllegalArgumentException or NPE is acceptable depending on CloudEvent behavior
    }
  }

  @Test
  public void testCreateFrom_MissingId() {
    // Arrange
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        null,  // missing id
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException or NullPointerException");
    } catch (Exception e) {
      // Either IllegalArgumentException or NPE is acceptable
    }
  }

  @Test
  public void testCreateFrom_MissingTenantHeader_BothFormats() {
    // Arrange
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.DELETE)
        .setSource(TEST_SOURCE)
        .build();

    Map<String, String> headers = new HashMap<>();
    // Missing both _lsrc and ce_lsrc

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_TenantFromLsrcHeader() {
    // Arrange
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.DELETE)
        .setSource(TEST_SOURCE)
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
        .thenReturn(metadataChange);

    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", "tenant-from-ce-header");

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act
    GarbageCollectionEvent result = factory.createFrom(wireEvent);

    // Assert
    assertEquals("tenant-from-ce-header", result.getTenant());
  }

  @Test
  public void testCreateFrom_TenantFromExtension() {
    // Arrange
    MetadataChange metadataChange = MetadataChange.newBuilder()
            .setOp(OpType.DELETE)
            .setSource(TEST_SOURCE)
            .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
            .thenReturn(metadataChange);
    Map<String, Object> extensions = new HashMap<>();
    extensions.put("lsrc", "tenant-from-extension");

    CloudEvent cloudEvent = buildCloudEvent(
            TEST_ID,
            TEST_TYPE,
            TEST_SUBJECT,
            PROTOBUF_CONTENT_TYPE,
            new byte[]{1, 2, 3},
            OffsetDateTime.now(),
            extensions
    );

    WireEvent wireEvent = new WireEvent(null, cloudEvent);

    // Act
    GarbageCollectionEvent result = factory.createFrom(wireEvent);

    // Assert
    assertEquals("tenant-from-extension", result.getTenant());
  }

  @Test
  public void testCreateFrom_InvalidTimestamp() {
    // Arrange - null time causes factory to fail when parsing timestamp
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        null,  // missing time
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_NullCloudEventData() {
    // Arrange
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        null,  // null data
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_MissingDataContentType() {
    // Arrange
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        null,  // missing content type
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_UnsupportedDataContentType() {
    // Arrange
    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        "application/xml",  // unsupported type
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_JsonFormat() {
    // Arrange
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.DELETE)
        .setSource(TEST_SOURCE)
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("json")))
        .thenReturn(metadataChange);

    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        JSON_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act
    GarbageCollectionEvent result = factory.createFrom(wireEvent);

    // Assert
    assertNotNull(result);
    assertEquals(metadataChange, result.getMetadataChange());
  }

  @Test
  public void testCreateFrom_MetadataChangeMissingSource() {
    // Arrange
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.DELETE)
        // Missing source
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
        .thenReturn(metadataChange);

    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_MetadataChangeMissingOp() {
    // Arrange
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setSource(TEST_SOURCE)
        // Missing op
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
        .thenReturn(metadataChange);

    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_SnapshotMissingSnapshotId() {
    // Arrange - snapshot event but extensions missing snapshotid (only page present)
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.SNAPSHOT)
        .setSource(TEST_SOURCE)
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
        .thenReturn(metadataChange);

    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    Map<String, Object> extensions = new HashMap<>();
    extensions.put("lsrc", TEST_TENANT);
    extensions.put("page", "0");
    extensions.put("total", "5");
    extensions.put("lastpage", "false");
    // Missing snapshotid

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        extensions
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_SnapshotMissingPageHeader() {
    // Arrange - snapshot event but extensions missing page (only snapshotid present)
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.SNAPSHOT)
        .setSource(TEST_SOURCE)
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
        .thenReturn(metadataChange);

    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    Map<String, Object> extensions = new HashMap<>();
    extensions.put("lsrc", TEST_TENANT);
    extensions.put("snapshotid", "snapshot-123");
    extensions.put("total", "5");
    extensions.put("lastpage", "false");
    // Missing page

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        extensions
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_SnapshotInvalidPageNumber() {
    // Arrange - snapshot event with invalid page (not a number)
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.SNAPSHOT)
        .setSource(TEST_SOURCE)
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
        .thenReturn(metadataChange);

    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    Map<String, Object> extensions = new HashMap<>();
    extensions.put("lsrc", TEST_TENANT);
    extensions.put("snapshotid", "snapshot-123");
    extensions.put("page", "not-a-number");  // invalid
    extensions.put("total", "5");
    extensions.put("lastpage", "false");

    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        OffsetDateTime.now(),
        extensions
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act & Assert
    try {
      factory.createFrom(wireEvent);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateFrom_TimestampConversion() {
    // Arrange
    MetadataChange metadataChange = MetadataChange.newBuilder()
        .setOp(OpType.DELETE)
        .setSource(TEST_SOURCE)
        .build();

    when(mockDeserializer.deserialize(any(byte[].class), eq("protobuf")))
        .thenReturn(metadataChange);

    Map<String, String> headers = new HashMap<>();
    headers.put("_lsrc", TEST_TENANT);

    OffsetDateTime testTime = OffsetDateTime.parse("2026-02-15T10:30:00Z");
    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        PROTOBUF_CONTENT_TYPE,
        new byte[]{1, 2, 3},
        testTime,
        null
    );

    WireEvent wireEvent = new WireEvent(headers, cloudEvent);

    // Act
    GarbageCollectionEvent result = factory.createFrom(wireEvent);

    // Assert
    assertEquals(testTime.toInstant().toEpochMilli(), result.getTimestampMs());
  }

  // Helper method to build CloudEvent (supports nullable fields for error testing)
  private CloudEvent buildCloudEvent(String id, String type, String subject,
                                     String contentType, byte[] data, OffsetDateTime time,
                                     Map<String, Object> extensions) {
    TestingCloudEvent event = new TestingCloudEvent();
    event.setProperty("id", id);
    event.setProperty("type", type);
    event.setProperty("subject", subject);
    event.setProperty("datacontenttype", contentType);
    event.setProperty("data", data);
    event.setProperty("time", time);
    event.setProperty("source", URI.create("http://test"));
    event.setExtensions(extensions);
    return event;
  }
}
