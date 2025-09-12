/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.io.InputStream;
import java.io.IOException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.*;

public class SchemaMD5RegressionTest {

  // Test data - created once and reused
  private static final List<SchemaReference> TEST_REFERENCES;
  private static final Metadata TEST_METADATA;
  private static final RuleSet TEST_RULE_SET;

  private final String AVRO_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"Person\"," +
        "\"namespace\":\"io.confluent.kafka.example\"," +
        "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}," +
        "{\"name\":\"age\",\"type\":\"int\"}," +
        "{\"name\":\"ssn\",\"type\":\"string\"}," +
        "{\"name\":\"user\",\"type\":\"io.confluent.kafka.example.User\"}," +
        "{\"name\":\"address\",\"type\":\"io.confluent.kafka.example.Address\"}]}";
  
  private final String PROTOBUF_SCHEMA_STRING = "syntax = \"proto3\";\n" +
      "package io.confluent.kafka.example;\n" +
      "import \"google/protobuf/timestamp.proto\";\n" +
      "message Person {\n" +
      "  string name = 1;\n" +
      "  int32 age = 2;\n" +
      "  string ssn = 3;\n" +
      "  User user = 4;\n" +
      "  Address address = 5;\n" +
      "}\n" +
      "message User {\n" +
      "  string id = 1;\n" +
      "  string email = 2;\n" +
      "}\n" +
      "message Address {\n" +
      "  string street = 1;\n" +
      "  string city = 2;\n" +
      "  string zip = 3;\n" +
      "}";
  
  private final String JSON_SCHEMA_STRING = "{\n" +
      "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
      "  \"type\": \"object\",\n" +
      "  \"title\": \"Person\",\n" +
      "  \"properties\": {\n" +
      "    \"name\": {\n" +
      "      \"type\": \"string\"\n" +
      "    },\n" +
      "    \"age\": {\n" +
      "      \"type\": \"integer\",\n" +
      "      \"minimum\": 0\n" +
      "    },\n" +
      "    \"ssn\": {\n" +
      "      \"type\": \"string\",\n" +
      "      \"pattern\": \"^[0-9]{3}-[0-9]{2}-[0-9]{4}$\"\n" +
      "    },\n" +
      "    \"user\": {\n" +
      "      \"$ref\": \"#/definitions/User\"\n" +
      "    },\n" +
      "    \"address\": {\n" +
      "      \"$ref\": \"#/definitions/Address\"\n" +
      "    }\n" +
      "  },\n" +
      "  \"required\": [\"name\", \"age\"],\n" +
      "  \"definitions\": {\n" +
      "    \"User\": {\n" +
      "      \"type\": \"object\",\n" +
      "      \"properties\": {\n" +
      "        \"id\": {\"type\": \"string\"},\n" +
      "        \"email\": {\"type\": \"string\", \"format\": \"email\"}\n" +
      "      }\n" +
      "    },\n" +
      "    \"Address\": {\n" +
      "      \"type\": \"object\",\n" +
      "      \"properties\": {\n" +
      "        \"street\": {\"type\": \"string\"},\n" +
      "        \"city\": {\"type\": \"string\"},\n" +
      "        \"zip\": {\"type\": \"string\"}\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" +
      "}";
    
  static {
    TEST_REFERENCES = Arrays.asList(
        new SchemaReference("io.confluent.kafka.example.User", "User", 1),
        new SchemaReference("io.confluent.kafka.example.Address", "Address", 2)
    );

    Map<String, Set<String>> tags = new TreeMap<>();
    tags.put("environment", new TreeSet<>(Arrays.asList("production", "staging")));
    tags.put("team", new TreeSet<>(Arrays.asList("data-engineering")));
    
    Map<String, String> properties = new TreeMap<>();
    properties.put("owner", "data-team");
    properties.put("description", "Person schema for user management");
    properties.put("version", "1.0.0");
    
    Set<String> sensitive = new TreeSet<>(Arrays.asList("ssn", "credit-card"));
    TEST_METADATA = new Metadata(tags, properties, sensitive);

    List<Rule> migrationRules = Arrays.asList(
        new Rule("upgrade-rule-1", "Upgrade rule for field changes", 
                RuleKind.TRANSFORM, RuleMode.UPGRADE, "FIELD_UPDATE",
                Collections.singleton("PERSON"), 
                Collections.singletonMap("field", "name"),
                "field.name = field.name.toUpperCase()", "none", "error", false),
        new Rule("downgrade-rule-1", "Downgrade rule for compatibility", 
                RuleKind.TRANSFORM, RuleMode.DOWNGRADE, "FIELD_DELETE",
                Collections.singleton("PERSON"), 
                Collections.singletonMap("field", "ssn"),
                "delete field.ssn", "none", "error", false)
    );

    List<Rule> domainRules = Arrays.asList(
        new Rule("validation-rule-1", "Validate age field", 
                RuleKind.CONDITION, RuleMode.WRITE, "FIELD_VALIDATION",
                Collections.singleton("PERSON"), 
                Collections.singletonMap("field", "age"),
                "field.age >= 0 && field.age <= 150", "none", "error", false),
        new Rule("masking-rule-1", "Mask sensitive data on read", 
                RuleKind.TRANSFORM, RuleMode.READ, "DATA_MASKING",
                Collections.singleton("PERSON"), 
                Collections.singletonMap("field", "ssn"),
                "field.ssn = '***-**-****'", "none", "error", false)
    );

    TEST_RULE_SET = new RuleSet(migrationRules, domainRules);
  }

  private Schema createSchema(String subject, Integer version, Integer id, 
                             String schemaString, String schemaType,
                             List<SchemaReference> references,
                             Metadata metadata,
                             RuleSet ruleSet) {
    Schema schema = new Schema(subject, version, id);
    schema.setSchemaType(schemaType);
    schema.setSchema(schemaString);
    
    if (references != null) {
      schema.setReferences(references);
    }
    if (metadata != null) {
      schema.setMetadata(metadata);
    }
    if (ruleSet != null) {
      schema.setRuleSet(ruleSet);
    }
    
    return schema;
  }

  private String calculateMD5Hash(Schema schema) {
    MD5 md5 = MD5.ofSchema(schema);
    byte[] bytes = md5.bytes();
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long msb = buffer.getLong();
    long lsb = buffer.getLong();
    return new UUID(msb, lsb).toString();
  }

  // Use in version < 8.0 to recreate guid values
  private void printAllHashValues() {
    String[] schemaTypes = {"AVRO", "PROTOBUF", "JSON"};
    String[] schemaStrings = {AVRO_SCHEMA_STRING, PROTOBUF_SCHEMA_STRING, JSON_SCHEMA_STRING};

    int testIndex = 0;
    for (int schemaTypeIndex = 0; schemaTypeIndex < 3; schemaTypeIndex++) {
      String schemaType = schemaTypes[schemaTypeIndex];
      String schemaString = schemaStrings[schemaTypeIndex];

      System.out.println(String.format("Testing %s schema:", schemaType));

      for (int i = 0; i < 8; i++) {
        boolean hasReferences = (i & 4) != 0;
        boolean hasMetadata = (i & 2) != 0;
        boolean hasRules = (i & 1) != 0;

        Schema schema = createSchema("Person", 1, 100, schemaString, schemaType,
            hasReferences ? TEST_REFERENCES : null,
            hasMetadata ? TEST_METADATA : null,
            hasRules ? TEST_RULE_SET : null);

        String hash = calculateMD5Hash(schema);

        System.out.println(hash);

        testIndex++;
      }
    }
  }

  @Test
  public void testGoldenFileConsistency() throws NoSuchAlgorithmException, IOException {
    // Load golden file data
    ObjectMapper mapper = new ObjectMapper();
    InputStream goldenFileStream = getClass().getClassLoader()
        .getResourceAsStream("schema-md5-golden-data.json");
    assertNotNull("Golden file should exist", goldenFileStream);

    JsonNode goldenData = mapper.readTree(goldenFileStream);

    // Verify schema strings match
    assertEquals("AVRO schema should match golden file",
        goldenData.get("schemas").get("avro").asText(), AVRO_SCHEMA_STRING);
    assertEquals("PROTOBUF schema should match golden file",
        goldenData.get("schemas").get("protobuf").asText(), PROTOBUF_SCHEMA_STRING);
    assertEquals("JSON schema should match golden file",
        goldenData.get("schemas").get("json").asText(), JSON_SCHEMA_STRING);

    // Verify references match
    JsonNode goldenRefs = goldenData.get("references");
    assertEquals("Should have 2 references", 2, goldenRefs.size());
    assertEquals("First reference name should match",
        "io.confluent.kafka.example.User", goldenRefs.get(0).get("name").asText());
    assertEquals("First reference subject should match",
        "User", goldenRefs.get(0).get("subject").asText());
    assertEquals("First reference version should match",
        1, goldenRefs.get(0).get("version").asInt());

    // Verify metadata matches
    String goldenMetadataString = goldenData.get("metadata").asText();
    assertEquals("Metadata toString should match",
        goldenMetadataString, TEST_METADATA.toString());

    // Verify rules match - convert RuleSet to JSON for comparison
    JsonNode goldenRules = goldenData.get("rules");
    assertNotNull("Golden rules should exist", goldenRules);
    assertTrue("Golden rules should have migrationRules", goldenRules.has("migrationRules"));
    assertTrue("Golden rules should have domainRules", goldenRules.has("domainRules"));
    String[] schemaTypes = {"AVRO", "PROTOBUF", "JSON"};
    String[] schemaStrings = {AVRO_SCHEMA_STRING, PROTOBUF_SCHEMA_STRING, JSON_SCHEMA_STRING};

    // Verify each permutation of schema computes to the desired hash
    for (int schemaTypeIndex = 0; schemaTypeIndex < 3; schemaTypeIndex++) {
      String schemaType = schemaTypes[schemaTypeIndex];
      String schemaString = schemaStrings[schemaTypeIndex];

      JsonNode expectedHashes = goldenData.get("expectedHashes").get(schemaType);

      for (int i = 0; i < 8; i++) {
        boolean hasReferences = (i & 4) != 0;
        boolean hasMetadata = (i & 2) != 0;
        boolean hasRules = (i & 1) != 0;

        Schema schema = createSchema("Person", 1, 100, schemaString, schemaType,
            hasReferences ? TEST_REFERENCES : null,
            hasMetadata ? TEST_METADATA : null,
            hasRules ? TEST_RULE_SET : null);

        String actualHash = calculateMD5Hash(schema);
        String combinationKey = String.format("%3s", Integer.toBinaryString(i)).replace(' ', '0');
        String expectedHash = expectedHashes.get(combinationKey).asText();

        assertEquals(String.format("Hash mismatch for %s combination %d (References=%s, Metadata=%s, Rules=%s)",
                schemaType.toUpperCase(), i, hasReferences, hasMetadata, hasRules),
            expectedHash, actualHash);
      }
    }
  }
}
