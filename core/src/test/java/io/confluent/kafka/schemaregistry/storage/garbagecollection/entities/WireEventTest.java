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

package io.confluent.kafka.schemaregistry.storage.garbagecollection.entities;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import org.junit.Test;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WireEventTest {

  private static final String TEST_ID = "test-id";
  private static final String TEST_TYPE = "test.type";
  private static final String TEST_SUBJECT = "topic";
  private static final String TEST_CONTENT_TYPE = "application/json";
  private static final byte[] TEST_DATA = new byte[]{1, 2, 3};

  private static CloudEvent buildCloudEvent(String id, String type, String subject,
                                            String contentType, byte[] data, OffsetDateTime time) {
    TestingCloudEvent event = new TestingCloudEvent();
    event.setProperty("id", id);
    event.setProperty("type", type);
    event.setProperty("subject", subject);
    event.setProperty("datacontenttype", contentType);
    event.setProperty("data", data);
    event.setProperty("time", time);
    event.setProperty("source", URI.create("http://test"));
    return event;
  }

  @Test
  public void testValidate_AllFieldsPresent() {
    CloudEvent cloudEvent = buildCloudEvent(
        TEST_ID,
        TEST_TYPE,
        TEST_SUBJECT,
        TEST_CONTENT_TYPE,
        TEST_DATA,
        OffsetDateTime.now()
    );
    WireEvent wireEvent = new WireEvent(Collections.emptyMap(), cloudEvent);

    wireEvent.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidate_MissingCloudEvent() {
    WireEvent wireEvent = new WireEvent(Collections.emptyMap(), null);
    wireEvent.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidate_MissingRequiredCloudEventFields() {
    CloudEvent cloudEvent = buildCloudEvent(
        null,  // missing id
        TEST_TYPE,
        TEST_SUBJECT,
        TEST_CONTENT_TYPE,
        TEST_DATA,
        OffsetDateTime.now()
    );
    WireEvent wireEvent = new WireEvent(Collections.emptyMap(), cloudEvent);
    wireEvent.validate();
  }

  private static class TestingCloudEvent implements CloudEvent {
    private final Map<String, Object> properties = new HashMap<>();

    void setProperty(String key, Object value) {
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
      final byte[] bytes = (byte[]) data;
      return bytes != null ? new CloudEventData() {
        @Override
        public byte[] toBytes() {
          return bytes;
        }
      } : null;
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
      return null;
    }

    @Override
    public Set<String> getExtensionNames() {
      return Set.of();
    }
  }
}
