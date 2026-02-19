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

package io.confluent.eventfeed.client.rest;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.confluent.eventfeed.client.rest.entities.SendResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public class EventFeedRestServiceTest {
  private EventFeedRestService eventFeedRestService;

  private static class TestingCloudEvent implements CloudEvent {
    Map<String, Object> properties = new HashMap<>();
    Map<String, Object> attributes = new HashMap<>();
    Map<String, Object> extensions = new HashMap<>();

    public void setProperty(String key, Object value) {
      properties.put(key, value);
    }
    public void setExtension(String key, Object value) {
      extensions.put(key, value);
    }
    public void resetExtensions() {
      extensions.clear();
    }

    @Override
    public CloudEventData getData() {
      return (CloudEventData) properties.get("data");
    }

    @Override
    public SpecVersion getSpecVersion() {
      return (SpecVersion) properties.get("specversion");
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
      return (URI) properties.get("dataschema");
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
    public Object getAttribute(String attributeName) throws IllegalArgumentException {
      return attributes.get(attributeName);
    }

    @Override
    public Object getExtension(String extensionName) {
      return extensions.get(extensionName);
    }

    @Override
    public Set<String> getExtensionNames() {
      return extensions.keySet();
    }
  }

  @BeforeEach
  public void setUp() {
    eventFeedRestService = new EventFeedRestService("http://localhost:8081");
  }

  private TestingCloudEvent getMinimumTestingCloudEventByType(String type) {
    TestingCloudEvent event = new TestingCloudEvent();
    event.setProperty("datacontenttype", "application/protobuf");
    event.setExtension("partitionkey", "lkc-1");
    event.setExtension("lsrc", "default");
    event.setProperty("specversion", SpecVersion.V1);
    event.setProperty("time", OffsetDateTime.now());
    if (type.equals("DELTA")) {
      event.setProperty("id", "1");
      event.setProperty("subject", "topic");
      event.setProperty("type", "DELTA");
      event.setProperty("source", URI.create("crn://confluent.cloud/kafka=lkc-1/topic=topic-1"));
    } else if (type.equals("SNAPSHOT")) {
      event.setProperty("id", "2");
      event.setProperty("subject", "topicAndClusterLink");
      event.setProperty("type", "SNAPSHOT");
      event.setProperty("source", URI.create("crn://confluent.cloud/kafka=lkc-1/topics-and-cluster-links"));
    } else {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
    return event;
  }

  @Test
  public void testSendMethod_validInput() throws Exception {
    // Valid case
    // Build a bare minimum cloud event
    List<String> types =  Arrays.asList("DELTA", "SNAPSHOT");
    for (String type : types) {
      CloudEvent event = getMinimumTestingCloudEventByType(type);
      EventFeedRestService restServiceSpy = Mockito.spy(eventFeedRestService);
      doAnswer(invocation -> {
        byte[] requestBodyData = (byte[]) invocation.getArguments()[2];
        Map<String, String> props = (Map<String, String>) invocation.getArguments()[3];
        if (requestBodyData != null) {
          fail("Request body should be empty.");
        }
        SendResult sendResult = new SendResult();
        if (props.get("Content-Type").equals(event.getDataContentType())
                && props.get("ce-id").equals(event.getId())
                && props.get("ce-subject").equals(event.getSubject())
                && props.get("ce-source").equals(event.getSource().toString())
                && props.get("ce-type").equals(event.getType())
                && props.get("ce-partitionkey").equals(
                String.valueOf(event.getExtension("partitionkey")))) {
          sendResult.setId(event.getId());
        } else {
          fail(String.format("Request headers don't match cloud event. Headers={}, cloudEvent={}",
                  props, event));
        }
        return sendResult;
      }).when(restServiceSpy)
              .httpRequest(any(), any(), any(), any(), any());
      restServiceSpy.send(event);
    }
  }

  @Test
  public void testSendMethod_invalidInput() {
    List<String> types =  Arrays.asList("DELTA", "SNAPSHOT");
    TestingCloudEvent event;
    for (String type : types) {
      event = getMinimumTestingCloudEventByType(type);
      event.setProperty("datacontenttype", null);
      try {
        eventFeedRestService.send(event);
        fail("datacontentype must not be null.");
      } catch (Exception e) {
        assertTrue(e instanceof IllegalArgumentException);
      }

      event = getMinimumTestingCloudEventByType(type);
      event.setProperty("id", null);
      try {
        eventFeedRestService.send(event);
        fail("id must not be null.");
      } catch (Exception e) {
        assertTrue(e instanceof IllegalArgumentException);
      }

      event = getMinimumTestingCloudEventByType(type);
      event.setProperty("subject", null);
      try {
        eventFeedRestService.send(event);
        fail("subject must not be null.");
      } catch (Exception e) {
        assertTrue(e instanceof IllegalArgumentException);
      }

      event = getMinimumTestingCloudEventByType(type);
      event.setProperty("source", null);
      try {
        eventFeedRestService.send(event);
        fail("source must not be null.");
      } catch (Exception e) {
        assertTrue(e instanceof IllegalArgumentException);
      }

      event = getMinimumTestingCloudEventByType(type);
      event.setProperty("type", null);
      try {
        eventFeedRestService.send(event);
        fail("source must not be null.");
      } catch (Exception e) {
        assertTrue(e instanceof IllegalArgumentException);
      }

      event = getMinimumTestingCloudEventByType(type);
      event.resetExtensions();
      try {
        eventFeedRestService.send(event);
        fail("source must not be null.");
      } catch (Exception e) {
        assertTrue(e instanceof IllegalArgumentException);
      }

      event = getMinimumTestingCloudEventByType(type);
      event.setProperty("specversion", SpecVersion.V03);
      try {
        eventFeedRestService.send(event);
        fail("specVersion must be V1.");
      } catch (Exception e) {
        assertTrue(e instanceof IllegalArgumentException);
      }
    }
  }
}
