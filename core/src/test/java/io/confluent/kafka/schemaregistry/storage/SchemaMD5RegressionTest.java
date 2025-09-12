/*
 * Copyright 2025 Confluent Inc.
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

import java.util.ArrayList;
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

  // Test data - loaded from golden file
  private List<SchemaReference> testReferences;
  private Metadata testMetadata;
  private RuleSet testRuleSet;
  private String avroSchemaString;
  private String protobufSchemaString;
  private String jsonSchemaString;

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

  private void loadTestDataFromGoldenFile(JsonNode goldenData) {
    JsonNode schemasNode = goldenData.get("schemas");
    avroSchemaString = schemasNode.get("avro").asText();
    protobufSchemaString = schemasNode.get("protobuf").asText();
    jsonSchemaString = schemasNode.get("json").asText();

    JsonNode referencesNode = goldenData.get("references");
    testReferences = new ArrayList<>();
    for (JsonNode refNode : referencesNode) {
      testReferences.add(new SchemaReference(
          refNode.get("name").asText(),
          refNode.get("subject").asText(),
          refNode.get("version").asInt()
      ));
    }

    JsonNode metadataNode = goldenData.get("metadata");
    Map<String, Set<String>> tags = new TreeMap<>();
    JsonNode tagsNode = metadataNode.get("tags");
    for (String tagKey : (Iterable<String>) () -> tagsNode.fieldNames()) {
      Set<String> tagValues = new TreeSet<>();
      for (JsonNode tagValue : tagsNode.get(tagKey)) {
        tagValues.add(tagValue.asText());
      }
      tags.put(tagKey, tagValues);
    }

    Map<String, String> properties = new TreeMap<>();
    JsonNode propertiesNode = metadataNode.get("properties");
    for (String propKey : (Iterable<String>) () -> propertiesNode.fieldNames()) {
      properties.put(propKey, propertiesNode.get(propKey).asText());
    }

    Set<String> sensitive = new TreeSet<>();
    for (JsonNode sensitiveNode : metadataNode.get("sensitive")) {
      sensitive.add(sensitiveNode.asText());
    }

    testMetadata = new Metadata(tags, properties, sensitive);

    JsonNode rulesNode = goldenData.get("rules");

    List<Rule> migrationRules = new ArrayList<>();
    JsonNode migrationRulesNode = rulesNode.get("migrationRules");
    for (JsonNode ruleNode : migrationRulesNode) {
      Set<String> tagsSet = new TreeSet<>();
      for (JsonNode tagNode : ruleNode.get("tags")) {
        tagsSet.add(tagNode.asText());
      }
      
      Map<String, String> params = new TreeMap<>();
      JsonNode paramsNode = ruleNode.get("params");
      for (String paramKey : (Iterable<String>) () -> paramsNode.fieldNames()) {
        params.put(paramKey, paramsNode.get(paramKey).asText());
      }

      migrationRules.add(new Rule(
          ruleNode.get("name").asText(),
          ruleNode.get("doc").asText(),
          RuleKind.valueOf(ruleNode.get("kind").asText()),
          RuleMode.valueOf(ruleNode.get("mode").asText()),
          ruleNode.get("type").asText(),
          tagsSet,
          params,
          ruleNode.get("expr").asText(),
          ruleNode.get("onSuccess").asText(),
          ruleNode.get("onFailure").asText(),
          ruleNode.get("disabled").asBoolean()
      ));
    }

    // Parse domain rules
    List<Rule> domainRules = new ArrayList<>();
    JsonNode domainRulesNode = rulesNode.get("domainRules");
    for (JsonNode ruleNode : domainRulesNode) {
      Set<String> tagsSet = new TreeSet<>();
      for (JsonNode tagNode : ruleNode.get("tags")) {
        tagsSet.add(tagNode.asText());
      }
      
      Map<String, String> params = new TreeMap<>();
      JsonNode paramsNode = ruleNode.get("params");
      for (String paramKey : (Iterable<String>) () -> paramsNode.fieldNames()) {
        params.put(paramKey, paramsNode.get(paramKey).asText());
      }

      domainRules.add(new Rule(
          ruleNode.get("name").asText(),
          ruleNode.get("doc").asText(),
          RuleKind.valueOf(ruleNode.get("kind").asText()),
          RuleMode.valueOf(ruleNode.get("mode").asText()),
          ruleNode.get("type").asText(),
          tagsSet,
          params,
          ruleNode.get("expr").asText(),
          ruleNode.get("onSuccess").asText(),
          ruleNode.get("onFailure").asText(),
          ruleNode.get("disabled").asBoolean()
      ));
    }

    testRuleSet = new RuleSet(migrationRules, domainRules);
  }

  // Use in version < 8.0 to recreate guid values
  @SuppressWarnings("unused")
  private void printAllHashValues() {
    String[] schemaTypes = {"AVRO", "PROTOBUF", "JSON"};
    String[] schemaStrings = {avroSchemaString, protobufSchemaString, jsonSchemaString};

    for (int schemaTypeIndex = 0; schemaTypeIndex < 3; schemaTypeIndex++) {
      String schemaType = schemaTypes[schemaTypeIndex];
      String schemaString = schemaStrings[schemaTypeIndex];

      System.out.println(String.format("Testing %s schema:", schemaType));

      for (int i = 0; i < 8; i++) {
        boolean hasReferences = (i & 4) != 0;
        boolean hasMetadata = (i & 2) != 0;
        boolean hasRules = (i & 1) != 0;

        Schema schema = createSchema("Person", 1, 100, schemaString, schemaType,
            hasReferences ? testReferences : null,
            hasMetadata ? testMetadata : null,
            hasRules ? testRuleSet : null);

        String hash = calculateMD5Hash(schema);

        System.out.println(hash);
      }
    }
  }

  @Test
  public void testGoldenMD5Consistency() throws NoSuchAlgorithmException, IOException {
    // Load golden file data
    ObjectMapper mapper = new ObjectMapper();
    InputStream goldenFileStream = getClass().getClassLoader()
        .getResourceAsStream("schema-md5-golden-data.json");
    assertNotNull("Golden file should exist", goldenFileStream);

    JsonNode goldenData = mapper.readTree(goldenFileStream);


    loadTestDataFromGoldenFile(goldenData);

    String[] schemaTypes = {"AVRO", "PROTOBUF", "JSON"};
    String[] schemaStrings = {avroSchemaString, protobufSchemaString, jsonSchemaString};

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
            hasReferences ? testReferences : null,
            hasMetadata ? testMetadata : null,
            hasRules ? testRuleSet : null);

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
