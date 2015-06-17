package io.confluent.kafka.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KafkaJsonSerializerTest {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void serialize() throws Exception {
    KafkaJsonSerializer serializer = new KafkaJsonSerializer();
    serializer.configure(Collections.<String, Object>emptyMap(), false);

    Map<String, Object> message = new HashMap<String, Object>();
    message.put("foo", "bar");
    message.put("baz", 354.99);

    byte[] bytes = serializer.serialize("foo", message);

    Object deserialized = this.objectMapper.readValue(bytes, Object.class);
    assertEquals(message, deserialized);
  }

}
