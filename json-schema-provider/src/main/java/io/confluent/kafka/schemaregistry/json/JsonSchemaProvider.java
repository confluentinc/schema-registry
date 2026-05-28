/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;

public class JsonSchemaProvider extends AbstractSchemaProvider {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaProvider.class);

  private static final Set<String> META_SCHEMA_URIS = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          "http://json-schema.org/draft-04/schema",
          "http://json-schema.org/draft-06/schema",
          "http://json-schema.org/draft-07/schema",
          "http://json-schema.org/draft/2019-09/schema",
          "https://json-schema.org/draft/2019-09/schema",
          "https://json-schema.org/draft/2019-09/meta/core",
          "https://json-schema.org/draft/2019-09/meta/validation",
          "https://json-schema.org/draft/2019-09/meta/applicator",
          "https://json-schema.org/draft/2019-09/meta/meta-data",
          "https://json-schema.org/draft/2019-09/meta/format",
          "https://json-schema.org/draft/2019-09/meta/content",
          "http://json-schema.org/draft/2020-12/schema",
          "https://json-schema.org/draft/2020-12/schema"
      )));

  @Override
  public String schemaType() {
    return JsonSchema.TYPE;
  }

  @Override
  public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew, boolean normalize) {
    try {
      Map<String, String> resolvedReferences = resolveReferences(schema, isNew);
      JsonSchema parsed = new JsonSchema(
              schema.getSchema(),
              schema.getReferences(),
              resolvedReferences,
              schema.getMetadata(),
              schema.getRuleSet(),
              null
      );
      rejectExternalRefs(parsed.toJsonNode(), resolvedReferences.keySet());
      return parsed;
    } catch (Exception e) {
      log.error("Could not parse JSON schema", e);
      throw new IllegalArgumentException("Invalid schema of type " + schema.getSchemaType()
          + ", details: " + e.getMessage(), e);
    }
  }

  private static void rejectExternalRefs(JsonNode root, Set<String> registeredRefNames) {
    if (root == null) {
      return;
    }
    Set<String> allowed = new HashSet<>(registeredRefNames);
    // $id is the keyword for drafts 6+; legacy draft 4 uses "id".
    JsonNode id = root.isObject() ? root.get("$id") : null;
    if (id == null || !id.isTextual()) {
      id = root.isObject() ? root.get("id") : null;
    }
    if (id != null && id.isTextual()) {
      allowed.add(id.asText());
    }
    scanRefs(root, allowed);
  }

  private static void scanRefs(JsonNode node, Set<String> allowed) {
    if (node.isObject()) {
      JsonNode ref = node.get("$ref");
      if (ref != null && ref.isTextual()) {
        String value = ref.asText();
        if (!value.isEmpty() && !value.startsWith("#")) {
          int hash = value.indexOf('#');
          String base = hash < 0 ? value : value.substring(0, hash);
          if (base.startsWith("./")) {
            base = base.substring(2);
          }
          if (!allowed.contains(base) && !META_SCHEMA_URIS.contains(base)) {
            throw new IllegalArgumentException(
                "External JSON Schema references are not allowed: " + value);
          }
        }
      }
      for (JsonNode child : node) {
        scanRefs(child, allowed);
      }
    } else if (node.isArray()) {
      for (JsonNode child : node) {
        scanRefs(child, allowed);
      }
    }
  }
}
