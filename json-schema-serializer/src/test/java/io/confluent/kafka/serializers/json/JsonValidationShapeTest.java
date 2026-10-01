/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.serializers.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class JsonValidationShapeTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final List<String> U = Collections.singletonList("u");

  @Test
  public void annotationsLeaveTheShapeAlone() throws Exception {
    String plain = "{\"properties\": {\"u\": {\"$ref\": \"#/definitions/U\"}}, \"definitions\": "
        + "{\"U\": {\"oneOf\": [{\"type\": \"number\"}, {\"type\": \"string\"}]}}}";
    String annotated = "{\"title\": \"Row\", \"description\": \"v2\", \"properties\": {\"u\": "
        + "{\"$ref\": \"#/definitions/U\", \"description\": \"a union\"}}, \"definitions\": "
        + "{\"U\": {\"oneOf\": [{\"type\": \"number\", \"title\": \"n\"}, "
        + "{\"type\": \"string\"}]}}}";
    assertNotNull(shape(plain));
    assertEquals(shape(plain), shape(annotated));
  }

  @Test
  public void aBranchValidatingOtherwiseChangesTheShape() throws Exception {
    assertNotEquals(
        shape("{\"properties\": {\"u\": {\"oneOf\": [{\"type\": \"integer\"}, {}]}}}"),
        shape("{\"properties\": {\"u\": {\"oneOf\": [{\"type\": \"number\"}, {}]}}}"));
  }

  @Test
  public void anotherDraftChangesTheShape() throws Exception {
    String properties = "\"properties\": {\"u\": {\"oneOf\": [{\"type\": \"number\"}]}}";
    assertNotEquals(shape("{" + properties + "}"),
        shape("{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", " + properties
            + "}"));
  }

  @Test
  public void aReferenceItCannotFollowHasNoShape() throws Exception {
    assertNull(shape("{\"properties\": {\"u\": {\"$ref\": \"other.json#/U\"}}}"));
    assertNull(shape("{\"properties\": {\"u\": {\"$ref\": \"#/definitions/L\"}}, "
        + "\"definitions\": {\"L\": {\"type\": \"array\", "
        + "\"items\": {\"$ref\": \"#/definitions/L\"}}}}"));
    assertNull(shape("{\"properties\": {\"u\": {\"$ref\": \"#anchor\"}}}"));
    assertNull(shape("{\"properties\": {\"v\": {}}}"));
  }

  @Test
  public void aDefinitionSharedByManyBranchesCountsOnce() throws Exception {
    // Twenty branches use one Meta of 600 properties: inlined at each use it would pass the cap,
    // shaped once it does not.
    StringBuilder meta = new StringBuilder();
    for (int i = 0; i < 600; i++) {
      meta.append(i > 0 ? ", " : "").append("\"m").append(i).append("\": {\"type\": \"string\"}");
    }
    StringBuilder branches = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      branches.append(i > 0 ? ", " : "").append("{\"properties\": {\"kind\": {\"enum\": [\"E")
          .append(i).append("\"]}, \"meta\": {\"$ref\": \"#/definitions/Meta\"}}}");
    }
    String schema = "{\"properties\": {\"u\": {\"oneOf\": [" + branches + "]}}, "
        + "\"definitions\": {\"Meta\": {\"properties\": {" + meta + "}}}}";

    assertNotNull(shape(schema));
    assertEquals(shape(schema), shape(schema));
  }

  private static JsonNode shape(String schema) throws Exception {
    return JsonValidationShape.of(MAPPER.readTree(schema), U);
  }
}
