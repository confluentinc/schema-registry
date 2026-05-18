/*
 * Copyright 2021 Confluent Inc.
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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

/**
 * A utility class wrapping a generic ObjectMapper singleton.
 */
public class JacksonMapper {
  public static final ObjectMapper INSTANCE = newObjectMapper();

  // Default max string length (1MB) - matches default MAX_REQUEST_BODY_SIZE_CONFIG
  private static final int DEFAULT_MAX_STRING_LENGTH = 1048576;

  public static ObjectMapper newObjectMapper() {
    return newObjectMapper(DEFAULT_MAX_STRING_LENGTH);
  }

  public static ObjectMapper newObjectMapper(int maxStringLength) {
    // Configure StreamReadConstraints to prevent Jackson from loading
    // excessively large strings/arrays/objects into memory
    StreamReadConstraints streamReadConstraints = StreamReadConstraints.builder()
        .maxStringLength(maxStringLength)
        .maxNumberLength(1000)
        .maxNestingDepth(1000)
        .build();

    JsonFactory jsonFactory = JsonFactory.builder()
        .streamReadConstraints(streamReadConstraints)
        .build();

    final ObjectMapper mapper = JsonMapper.builder(jsonFactory)
        .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
        .build();

    return configure(mapper);
  }

  private static ObjectMapper configure(ObjectMapper mapper) {
    // For Optional support
    mapper.registerModule(new Jdk8Module());
    return mapper;
  }
}
