/*
 * Copyright 2018 Confluent Inc.
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
package io.confluent.kafka.serializers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.jackson.Jackson;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KafkaJsonSerializerTest {

  private ObjectMapper objectMapper = Jackson.newObjectMapper();
  private KafkaJsonSerializer<Object> serializer;

  @Before
  public void setup() {
    serializer = new KafkaJsonSerializer<>();
    Map<String, Object> config = new HashMap<>();
    config.put(KafkaJsonSerializerConfig.WRITE_DATES_AS_ISO8601, true);
    serializer.configure(config, false);
  }

  @Test
  public void serializeNull() {
    assertNull(serializer.serialize("foo", null));
  }

  @Test
  public void serialize() throws Exception {
    Map<String, Object> message = new HashMap<>();
    message.put("foo", "bar");
    message.put("baz", 354.99);

    byte[] bytes = serializer.serialize("foo", message);

    Object deserialized = this.objectMapper.readValue(bytes, Object.class);
    assertEquals(message, deserialized);
  }

  @Test
  public void serializeUser() throws Exception {
    User user = new User("john", "doe", (short) 50, "jack", LocalDate.parse("2018-12-27"));

    byte[] bytes = serializer.serialize("foo", user);

    Object deserialized = this.objectMapper.readValue(bytes, User.class);
    assertEquals(user, deserialized);
  }

  public static class User {
    @JsonProperty
    public String firstName;
    @JsonProperty
    public String lastName;
    @JsonProperty
    public short age;
    @JsonProperty
    public Optional<String> nickName;
    @JsonProperty
    public LocalDate birthdate;

    public User() {}

    public User(String firstName, String lastName, short age, String nickName, LocalDate birthdate) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.age = age;
      this.nickName = Optional.ofNullable(nickName);
      this.birthdate = birthdate;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      User user = (User) o;
      return age == user.age
          && Objects.equals(firstName, user.firstName)
          && Objects.equals(lastName, user.lastName)
          && Objects.equals(nickName, user.nickName)
          && Objects.equals(birthdate, user.birthdate);
    }

    @Override
    public int hashCode() {
      return Objects.hash(firstName, lastName, age, nickName, birthdate);
    }
  }
}
